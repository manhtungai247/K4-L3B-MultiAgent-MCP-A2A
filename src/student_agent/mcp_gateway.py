from __future__ import annotations

import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from copy import deepcopy
from typing import Any

import httpx2
from mcp import ClientSession
from mcp.client.streamable_http import streamable_http_client

from .contracts import Contracts


class MCPToolError(RuntimeError):
    """A completed MCP call reported a tool-level rejection."""


class EvidenceGateway:
    def __init__(self, session: ClientSession, contracts: Contracts) -> None:
        self._session = session
        self._contracts = contracts
        self._cache: dict[tuple[str, str, tuple[tuple[str, str], ...]], dict[str, Any]] = {}
        self.calls_by_case: dict[str, int] = {}
        self.transport_errors: set[str] = set()

    async def list_tools(self) -> list[str]:
        response = await self._session.list_tools()
        return sorted(tool.name for tool in response.tools)

    async def call(self, tool_name: str, *, case_id: str, **arguments: str) -> dict[str, Any]:
        payload = {"case_id": case_id, **arguments}
        cache_key = (
            case_id,
            tool_name,
            tuple(
                sorted((key, json.dumps(value, sort_keys=True)) for key, value in arguments.items())
            ),
        )
        cached = self._cache.get(cache_key)
        if cached is not None:
            return deepcopy(cached)

        self.calls_by_case[case_id] = self.calls_by_case.get(case_id, 0) + 1
        result = await self._session.call_tool(tool_name, arguments=payload)
        is_error = getattr(result, "is_error", getattr(result, "isError", False))
        if is_error:
            # Tool errors may echo request data; keep credentials and case payloads out of logs.
            raise MCPToolError(f"MCP tool {tool_name} returned an error")
        evidence = getattr(result, "structuredContent", None)
        if evidence is None:
            evidence = getattr(result, "structured_content", None)
        if evidence is None:
            text_blocks = [block.text for block in result.content if getattr(block, "text", None)]
            if len(text_blocks) != 1:
                raise ValueError(f"MCP tool {tool_name} did not return one evidence object")
            evidence = json.loads(text_blocks[0])
        if not isinstance(evidence, dict):
            raise ValueError(f"MCP tool {tool_name} did not return an evidence object")
        self._contracts.validate_evidence(evidence, f"MCP tool {tool_name}")
        self._cache[cache_key] = deepcopy(evidence)
        return deepcopy(evidence)


@asynccontextmanager
async def connect_gateway(
    endpoint: str, team_api_key: str, contracts: Contracts
) -> AsyncIterator[EvidenceGateway]:
    headers = {"Authorization": f"Bearer {team_api_key}"}
    timeout = httpx2.Timeout(300.0, connect=30.0, write=30.0, pool=30.0)
    async with (
        httpx2.AsyncClient(headers=headers, timeout=timeout) as http_client,
        streamable_http_client(endpoint, http_client=http_client) as (read_stream, write_stream),
        ClientSession(read_stream, write_stream) as session,
    ):
        await session.initialize()
        yield EvidenceGateway(session, contracts)
