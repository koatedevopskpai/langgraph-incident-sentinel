import asyncio


async def fetch_k8s_logs(query: str, iteration: int) -> str:
    await asyncio.sleep(0.05)  # Simulates non-blocking network I/O
    if iteration == 0:
        return "[k8s_logs] WARN: High latency detected on pod checkout-api-7f8d9. HTTP 502 spikes."
    return (
        "[k8s_logs] ERROR: OOMKilled (Exit Code 137) on checkout-api-7f8d9. "
        "Memory limit 512Mi exceeded during payload serialization."
    )


async def fetch_recent_commits(query: str) -> str:
    await asyncio.sleep(0.05)
    return (
        "[git_commits] Commit 9a4b2c1 (14 mins ago) by dev@acme.com: "
        "'feat(checkout): load full order history into memory for PDF receipt generation'"
    )


async def fetch_metrics(query: str) -> str:
    await asyncio.sleep(0.05)
    return "[metrics] checkout-api RSS memory jumped from 180MB to 512MB at 14:02 UTC."


async def execute_remediation_cli(command: str) -> str:
    await asyncio.sleep(0.05)
    return f"EXECUTED: `{command}` -> Deployment checkout-api rolled back to revision 42. Pods healthy."
