"""Stateless FastAPI webhooks for Slack/PagerDuty human-in-the-loop approvals.

The API holds no per-incident memory: every request resolves the graph state from the
checkpointer by ``thread_id``, so any pod can accept an approval after the worker that
started the incident has exited.
"""

from contextlib import asynccontextmanager
from typing import AsyncIterator, Optional

from fastapi import FastAPI, HTTPException
from langgraph.types import Command
from pydantic import BaseModel, Field

from src.config import get_database_url
from src.graph import build_graph


class AlertRequest(BaseModel):
    incident_id: str = Field(min_length=1, description="Checkpoint thread id for this incident.")
    alert_summary: str = Field(min_length=1, description="Alert text from PagerDuty/Slack.")


class ApprovalRequest(BaseModel):
    thread_id: str = Field(min_length=1, description="Incident thread id awaiting approval.")
    decision: str = Field(
        min_length=1,
        description="'yes', 'no', or a custom CLI command (validated by guardrails).",
    )


def _thread_config(thread_id: str) -> dict:
    return {"configurable": {"thread_id": thread_id}}


def _pending_interrupt(snapshot) -> Optional[dict]:
    if not snapshot.next:
        return None
    for task in snapshot.tasks:
        for pending in task.interrupts:
            return pending.value
    return None


def _summarize(snapshot) -> dict:
    values = snapshot.values
    pending = _pending_interrupt(snapshot)
    return {
        "status": "paused" if pending else "completed",
        "next": list(snapshot.next),
        "interrupt": pending,
        "state": {
            "iteration": values.get("iteration"),
            "hypothesis": values.get("hypothesis"),
            "confidence_score": values.get("confidence_score"),
            "proposed_command": values.get("proposed_command"),
            "remediation_result": values.get("remediation_result"),
        },
    }


async def _drain(graph, payload, config: dict) -> None:
    """Drive the graph until it completes or parks on an interrupt()."""
    async for _ in graph.astream(payload, config, stream_mode="updates"):
        pass


def create_app(checkpointer=None) -> FastAPI:
    """
    Build the app.

    Passing a checkpointer (e.g. ``MemorySaver()``) wires the graph immediately, which
    keeps tests offline. Omitting it defers to a shared AsyncPostgresSaver pool opened
    during application lifespan.
    """

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        if checkpointer is not None:
            app.state.graph = build_graph(checkpointer)
            yield
            return

        # Imported lazily so offline/test environments need no Postgres driver.
        from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver

        async with AsyncPostgresSaver.from_conn_string(get_database_url()) as saver:
            await saver.setup()
            app.state.graph = build_graph(saver)
            yield

    app = FastAPI(title="On-Call Sentinel", version="0.1.0", lifespan=lifespan)

    if checkpointer is not None:
        app.state.graph = build_graph(checkpointer)

    @app.get("/healthz")
    async def healthz() -> dict:
        return {"status": "ok"}

    @app.post("/webhooks/alert", status_code=202)
    async def receive_alert(request: AlertRequest) -> dict:
        graph = app.state.graph
        config = _thread_config(request.incident_id)

        existing = await graph.aget_state(config)
        if existing.values:
            # Idempotent: replaying an alert must not restart a paused investigation.
            return {"thread_id": request.incident_id, **_summarize(existing)}

        await _drain(
            graph,
            {
                "incident_id": request.incident_id,
                "alert_summary": request.alert_summary,
                "findings": [],
                "iteration": 0,
                "guardrail_error": "",
            },
            config,
        )
        snapshot = await graph.aget_state(config)
        return {"thread_id": request.incident_id, **_summarize(snapshot)}

    @app.post("/webhooks/approve", status_code=202)
    async def receive_approval(request: ApprovalRequest) -> dict:
        graph = app.state.graph
        config = _thread_config(request.thread_id)

        snapshot = await graph.aget_state(config)
        if not snapshot.values:
            raise HTTPException(
                status_code=404,
                detail=f"Unknown incident thread: {request.thread_id}",
            )
        if not snapshot.next:
            raise HTTPException(
                status_code=409,
                detail=f"Incident {request.thread_id} is not awaiting approval.",
            )

        await _drain(graph, Command(resume=request.decision), config)
        resumed = await graph.aget_state(config)
        return {"thread_id": request.thread_id, **_summarize(resumed)}

    return app


app = create_app()
