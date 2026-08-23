from __future__ import annotations

import asyncio

from mcp import Client

from iol_mcp.server import create_server
from iol_mcp.service import MCPReadOnlyService


class Gateway:
    def account_status(self): return {}
    def portfolio(self, country): return {"activos": []}
    def orders(self): return []
    def optional_movements(self, as_of): return (None, "unavailable")
    def movements(self, date_from, date_to, country): return []


def _service(tmp_path):
    profile = {
        "schema_version": 1, "objectives": "growth", "horizon": "long", "liquidity_needs": "none",
        "loss_tolerance": "high", "allowed_markets": ["argentina"], "allowed_instruments": ["cedears"],
    }
    return MCPReadOnlyService(lambda: Gateway(), profile_loader=lambda: profile, review_dir=tmp_path / "reports/monthly", decision_dir=tmp_path / "data/review")


def test_server_publishes_only_six_read_only_tools(tmp_path):
    server = create_server(_service(tmp_path))
    tools = asyncio.run(server.list_tools())

    assert {tool.name for tool in tools} == {
        "get_portfolio_snapshot", "get_investor_profile", "get_movements", "get_investment_history", "get_asset_context", "get_capabilities",
    }
    for tool in tools:
        assert tool.annotations.read_only_hint is True
        assert tool.annotations.idempotent_hint is True
        assert tool.annotations.open_world_hint is False


def test_mcp_client_can_initialize_list_and_call_read_only_capabilities(tmp_path):
    async def exercise():
        async with Client(create_server(_service(tmp_path))) as client:
            tools = await client.list_tools()
            result = await client.call_tool("get_capabilities")
            return tools, result

    tools, result = asyncio.run(exercise())
    assert len(tools.tools) == 6
    assert result.structured_content["read_only"] is True
    assert result.structured_content["can_execute_orders"] is False
