from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from httpx import ASGITransport, AsyncClient
from langgraph.checkpoint.memory import MemorySaver

from src.api import create_app
from src.nodes import DiagnosisOutput

SAFE_COMMAND = "kubectl rollout undo deployment/checkout-api"


def _diagnosis(confidence: float = 0.95, command: str = SAFE_COMMAND) -> DiagnosisOutput:
    return DiagnosisOutput(
        hypothesis="OOMKilled after commit 9a4b2c1 loaded full order history.",
        confidence_score=confidence,
        proposed_command=command,
    )


def _wire_llm(mock_get_llm, proposals: list[DiagnosisOutput]) -> MagicMock:
    structured = MagicMock()
    structured.ainvoke = AsyncMock(side_effect=list(proposals))
    mock_get_llm.return_value.with_structured_output.return_value = structured
    return structured


def _client(app) -> AsyncClient:
    # ASGITransport keeps the test in-process; no server, no network, no Postgres.
    return AsyncClient(transport=ASGITransport(app=app), base_url="http://sentinel.test")


@pytest.mark.asyncio
@patch("src.nodes.get_llm")
async def test_alert_pauses_then_approval_completes(mock_get_llm):
    _wire_llm(mock_get_llm, [_diagnosis()])
    app = create_app(MemorySaver())

    async with _client(app) as client:
        alert = await client.post(
            "/webhooks/alert",
            json={
                "incident_id": "inc-api-1",
                "alert_summary": "502 errors on checkout-api",
            },
        )
        assert alert.status_code == 202
        body = alert.json()
        assert body["status"] == "paused"
        assert body["next"] == ["remediate"]
        assert body["interrupt"]["proposed_command"] == SAFE_COMMAND

        approval = await client.post(
            "/webhooks/approve",
            json={"thread_id": "inc-api-1", "decision": "yes"},
        )
        assert approval.status_code == 202
        resumed = approval.json()
        assert resumed["status"] == "completed"
        assert resumed["next"] == []
        assert f"EXECUTED: `{SAFE_COMMAND}`" in resumed["state"]["remediation_result"]


@pytest.mark.asyncio
@patch("src.nodes.get_llm")
async def test_operator_override_is_still_guardrail_validated(mock_get_llm):
    _wire_llm(mock_get_llm, [_diagnosis()])
    app = create_app(MemorySaver())

    async with _client(app) as client:
        await client.post(
            "/webhooks/alert",
            json={"incident_id": "inc-api-2", "alert_summary": "502 errors"},
        )
        approval = await client.post(
            "/webhooks/approve",
            json={"thread_id": "inc-api-2", "decision": "kubectl delete namespace production"},
        )

        assert approval.status_code == 202
        result = approval.json()["state"]["remediation_result"]
        assert result.startswith("BLOCKED BY GUARDRAIL:")
        assert "denylist" in result


@pytest.mark.asyncio
@patch("src.nodes.get_llm")
async def test_repeated_alert_does_not_restart_investigation(mock_get_llm):
    structured = _wire_llm(mock_get_llm, [_diagnosis()])
    app = create_app(MemorySaver())

    async with _client(app) as client:
        payload = {"incident_id": "inc-api-3", "alert_summary": "502 errors"}
        first = await client.post("/webhooks/alert", json=payload)
        second = await client.post("/webhooks/alert", json=payload)

        assert first.json()["status"] == "paused"
        assert second.json()["status"] == "paused"
        assert structured.ainvoke.await_count == 1


@pytest.mark.asyncio
async def test_approval_for_unknown_thread_returns_404():
    app = create_app(MemorySaver())

    async with _client(app) as client:
        response = await client.post(
            "/webhooks/approve",
            json={"thread_id": "does-not-exist", "decision": "yes"},
        )

    assert response.status_code == 404


@pytest.mark.asyncio
@patch("src.nodes.get_llm")
async def test_double_approval_returns_409(mock_get_llm):
    _wire_llm(mock_get_llm, [_diagnosis()])
    app = create_app(MemorySaver())

    async with _client(app) as client:
        await client.post(
            "/webhooks/alert",
            json={"incident_id": "inc-api-4", "alert_summary": "502 errors"},
        )
        first = await client.post(
            "/webhooks/approve", json={"thread_id": "inc-api-4", "decision": "yes"}
        )
        second = await client.post(
            "/webhooks/approve", json={"thread_id": "inc-api-4", "decision": "yes"}
        )

    assert first.status_code == 202
    assert second.status_code == 409
