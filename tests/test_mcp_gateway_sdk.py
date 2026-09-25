from __future__ import annotations

import asyncio
import json
from typing import Any

import pytest
from mcp.types import CallToolResult, TextContent

from student_agent.mcp_gateway import EvidenceGateway

EVIDENCE = {
    "schema_version": "day09-mcp-evidence-v1",
    "evidence_ref": "ev_12345678901234567890",
    "result_hash": "sha256:" + "a" * 64,
    "domain": "policy",
    "data": {"policy_version": "EC_POLICY_V2"},
}


class FakeContracts:
    def validate_evidence(self, value: Any, label: str) -> None:
        assert value["schema_version"] == "day09-mcp-evidence-v1", label


class FakeSession:
    def __init__(self, results: list[CallToolResult]) -> None:
        self.results = results
        self.calls: list[tuple[str, dict[str, str]]] = []

    async def call_tool(self, name: str, *, arguments: dict[str, str]) -> CallToolResult:
        self.calls.append((name, arguments))
        return self.results.pop(0)


def result_with_structured_evidence() -> CallToolResult:
    # Construct the installed MCP SDK type; its Python attributes are snake_case.
    return CallToolResult(content=[], structuredContent=EVIDENCE)


def test_gateway_reads_real_sdk_structured_content_and_caches_per_case() -> None:
    session = FakeSession([result_with_structured_evidence(), result_with_structured_evidence()])
    gateway = EvidenceGateway(session, FakeContracts())  # type: ignore[arg-type]

    async def scenario() -> tuple[dict[str, Any], dict[str, Any]]:
        await gateway.call("get_policy", case_id="CASE_001", policy_version="EC_POLICY_V2")
        cached = await gateway.call("get_policy", case_id="CASE_001", policy_version="EC_POLICY_V2")
        other_case = await gateway.call(
            "get_policy", case_id="CASE_002", policy_version="EC_POLICY_V2"
        )
        return cached, other_case

    first = asyncio.run(
        gateway.call("get_policy", case_id="CASE_001", policy_version="EC_POLICY_V2")
    )
    first["data"]["mutated"] = True
    cached, other_case = asyncio.run(scenario())

    assert "mutated" not in cached["data"]
    assert len(session.calls) == 2
    assert other_case["evidence_ref"] == EVIDENCE["evidence_ref"]
    assert gateway.calls_by_case == {"CASE_001": 1, "CASE_002": 1}


def test_gateway_supports_text_json_fallback() -> None:
    session = FakeSession([CallToolResult(content=[TextContent(text=json.dumps(EVIDENCE))])])
    gateway = EvidenceGateway(session, FakeContracts())  # type: ignore[arg-type]
    actual = asyncio.run(
        gateway.call("get_policy", case_id="CASE_001", policy_version="EC_POLICY_V2")
    )
    assert actual == EVIDENCE


def test_gateway_rejects_real_sdk_tool_error_without_echoing_payload() -> None:
    session = FakeSession(
        [CallToolResult(content=[TextContent(text="sensitive server detail")], isError=True)]
    )
    gateway = EvidenceGateway(session, FakeContracts())  # type: ignore[arg-type]

    async def scenario() -> None:
        await gateway.call("get_policy", case_id="CASE_001", policy_version="EC_POLICY_V2")

    with pytest.raises(RuntimeError, match="returned an error") as error:
        asyncio.run(scenario())
    assert "sensitive server detail" not in str(error.value)
