from langgraph.types import interrupt
from pydantic import BaseModel, Field

from src.config import get_llm
from src.guardrails import validate_command_safety
from src.state import IncidentState, InvestigationTask
from src.tools import (
    execute_remediation_cli,
    fetch_k8s_logs,
    fetch_metrics,
    fetch_recent_commits,
)


class DiagnosisOutput(BaseModel):
    hypothesis: str = Field(
        description="Root cause analysis based on current findings."
    )
    confidence_score: float = Field(
        ge=0.0, le=1.0, description="Confidence from 0.0 to 1.0."
    )
    proposed_command: str = Field(
        description="Safe kubectl remediation command (e.g., 'kubectl rollout undo deployment/checkout-api')."
    )


async def planner_node(state: IncidentState) -> dict:
    """Initializes or carries forward the current investigation iteration.

    Note: guardrail_error is deliberately left untouched here. synthesizer_node reads it
    to inject policy feedback into the self-correction prompt, and clears it on success.
    """
    return {"iteration": state.get("iteration", 0)}


async def investigator_worker(task: InvestigationTask, state: IncidentState) -> dict:
    """Async worker node executed concurrently via Send()."""
    source = task["source"]
    query = task["query"]
    iteration = state.get("iteration", 0)

    if source == "k8s_logs":
        result = await fetch_k8s_logs(query, iteration)
    elif source == "git_commits":
        result = await fetch_recent_commits(query)
    else:
        result = await fetch_metrics(query)

    return {"findings": [f"(Iter {iteration}) {result}"]}


async def synthesizer_node(state: IncidentState) -> dict:
    """Evaluates bounded findings asynchronously and produces a confidence-scored diagnosis."""
    structured_llm = get_llm().with_structured_output(DiagnosisOutput)
    findings_text = "\n".join(state["findings"])

    guardrail_feedback = ""
    if state.get("guardrail_error"):
        guardrail_feedback = (
            f"\nPREVIOUS COMMAND REJECTED BY SECURITY POLICY: {state['guardrail_error']}. "
            "You MUST propose a compliant kubectl command."
        )

    prompt = (
        f"Alert: {state['alert_summary']}\n"
        f"Iteration: {state['iteration']}\n"
        f"Collected Evidence:\n{findings_text}\n"
        f"{guardrail_feedback}\n\n"
        "Synthesize the root cause. If evidence is merely superficial (e.g., generic 502s "
        "without exact error codes), keep confidence_score below 0.8."
    )
    diagnosis: DiagnosisOutput = await structured_llm.ainvoke(prompt)

    is_safe, reason = validate_command_safety(diagnosis.proposed_command)

    return {
        "hypothesis": diagnosis.hypothesis,
        "confidence_score": diagnosis.confidence_score,
        "proposed_command": diagnosis.proposed_command,
        "guardrail_error": "" if is_safe else reason,
        "iteration": state["iteration"] + 1,
    }


async def human_approval_and_remediate_node(state: IncidentState) -> dict:
    """Pauses execution using native interrupt() until a human approves or overrides."""
    approval_payload = {
        "alert": state["alert_summary"],
        "root_cause": state["hypothesis"],
        "confidence": state["confidence_score"],
        "proposed_command": state["proposed_command"],
        "instructions": "Approve ('yes'), reject ('no'), or provide an override CLI command.",
    }

    human_response = interrupt(approval_payload)

    if isinstance(human_response, str) and human_response.lower() == "no":
        return {"remediation_result": "ABORTED: Operator rejected remediation."}

    command_to_run = (
        state["proposed_command"]
        if str(human_response).lower() == "yes"
        else str(human_response).strip()
    )

    # Re-validate even human overrides against security guardrails
    is_safe, reason = validate_command_safety(command_to_run)
    if not is_safe:
        return {"remediation_result": f"BLOCKED BY GUARDRAIL: {reason}"}

    output = await execute_remediation_cli(command_to_run)
    return {"remediation_result": output}
