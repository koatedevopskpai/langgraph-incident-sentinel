from typing import Annotated, TypedDict

MAX_FINDINGS_HISTORY = 10
MAX_CHARS_PER_FINDING = 600
HEAD_CHARS = 200
TRUNCATION_MARKER = "\n... [TRUNCATED] ...\n"
TAIL_CHARS = MAX_CHARS_PER_FINDING - HEAD_CHARS - len(TRUNCATION_MARKER)


def _bound_finding(text: str) -> str:
    """
    Clamp an oversized finding while preserving both ends of the evidence.

    The head carries service/timestamp metadata; the tail carries the terminating
    exception, exit code or OOMKilled reason. Cutting only the middle keeps both.
    """
    if len(text) <= MAX_CHARS_PER_FINDING:
        return text
    return f"{text[:HEAD_CHARS]}{TRUNCATION_MARKER}{text[-TAIL_CHARS:]}"


def bounded_findings_reducer(existing: list[str], new_items: list[str]) -> list[str]:
    """
    Merges parallel worker outputs deterministically while enforcing:
    1. Head+tail truncation (prevents massive stack traces from blowing token budgets
       while retaining the root cause at the end of the log)
    2. Exact deduplication across retry loops
    3. Sliding window cap (keeps the most recent MAX_FINDINGS_HISTORY items)
    """
    combined = list(existing)
    for item in new_items:
        truncated = _bound_finding(item)
        if truncated not in combined:
            combined.append(truncated)

    return combined[-MAX_FINDINGS_HISTORY:]


class InvestigationTask(TypedDict):
    source: str
    query: str


class IncidentState(TypedDict):
    incident_id: str
    alert_summary: str
    findings: Annotated[list[str], bounded_findings_reducer]
    hypothesis: str
    confidence_score: float
    iteration: int
    proposed_command: str
    guardrail_error: str
    remediation_result: str
