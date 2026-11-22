from langgraph.graph import END, START, StateGraph
from langgraph.types import Send

from src.nodes import (
    human_approval_and_remediate_node,
    investigator_worker,
    planner_node,
    synthesizer_node,
)
from src.state import IncidentState


def fan_out_investigations(state: IncidentState) -> list[Send]:
    """Dynamically spawns parallel async workers based on current iteration."""
    if state["iteration"] == 0:
        tasks = [
            {"source": "k8s_logs", "query": "checkout-api errors"},
            {"source": "git_commits", "query": "checkout-api recent"},
            {"source": "metrics", "query": "checkout-api memory cpu"},
        ]
    else:
        tasks = [{"source": "k8s_logs", "query": "checkout-api OOM exit codes"}]

    return [
        Send("investigate", {"source": t["source"], "query": t["query"], **state})
        for t in tasks
    ]


def route_after_synthesis(state: IncidentState) -> str:
    """
    Deterministic routing policy:
    1. Hard stop if max retries (3) reached -> escalate to human.
    2. If proposed command failed security guardrail -> retry synthesis directly with
       policy feedback (no need to re-collect evidence).
    3. If confidence < 0.8 -> loop back to planner for deeper investigation.
    4. Otherwise -> proceed to HITL approval.
    """
    if state["iteration"] >= 3:
        return "remediate"
    if state.get("guardrail_error"):
        return "synthesizer"
    if state["confidence_score"] < 0.8:
        return "planner"
    return "remediate"


async def _investigate_dispatch(state: dict) -> dict:
    return await investigator_worker(
        {"source": state["source"], "query": state["query"]}, state
    )


def build_graph(checkpointer):
    builder = StateGraph(IncidentState)

    builder.add_node("planner", planner_node)
    builder.add_node("investigate", _investigate_dispatch)
    builder.add_node("synthesizer", synthesizer_node)
    builder.add_node("remediate", human_approval_and_remediate_node)

    builder.add_edge(START, "planner")
    builder.add_conditional_edges("planner", fan_out_investigations, ["investigate"])
    builder.add_edge("investigate", "synthesizer")
    builder.add_conditional_edges(
        "synthesizer",
        route_after_synthesis,
        {
            "planner": "planner",
            "synthesizer": "synthesizer",
            "remediate": "remediate",
        },
    )
    builder.add_edge("remediate", END)

    return builder.compile(checkpointer=checkpointer)
