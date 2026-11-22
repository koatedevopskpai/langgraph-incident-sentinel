import re

DENYLIST_PATTERNS = [
    r"\brm\s+-rf\b",
    r"\bkubectl\s+delete\s+(namespace|ns|node|pv|pvc)\b",
    r"\bdrop\s+(table|database)\b",
    r"\bmkfs\b",
    r":\(\)\{\s*:\|:&\s*\};:",  # Fork bomb
]

ALLOWED_PREFIXES = (
    "kubectl rollout",
    "kubectl scale",
    "kubectl delete pod",
    "kubectl set env",
)


def validate_command_safety(command: str) -> tuple[bool, str]:
    """
    Validates proposed remediation commands against deterministic security rules.
    Returns (is_safe, reason).
    """
    normalized = command.strip()
    if not normalized:
        return False, "Empty command proposed."

    for pattern in DENYLIST_PATTERNS:
        if re.search(pattern, normalized, re.IGNORECASE):
            return False, f"Blocked by security denylist pattern: `{pattern}`"

    if not normalized.startswith(ALLOWED_PREFIXES):
        return (
            False,
            f"Command must start with an approved safe prefix: {ALLOWED_PREFIXES}",
        )

    return True, "OK"
