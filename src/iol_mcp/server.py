from __future__ import annotations

import logging
import os
import time
from typing import Callable, TypeVar

from mcp.server import MCPServer
from mcp.types import ToolAnnotations

from iol_review.gateway import ReadOnlyIOLGateway, configured_read_only_gateway

from .models import AssetContext, Capabilities, InvestmentHistory, InvestorProfileResponse, MovementList, PortfolioSnapshot
from .service import MCPReadOnlyService


READ_ONLY = ToolAnnotations(read_only_hint=True, destructive_hint=False, idempotent_hint=True, open_world_hint=False)
INSTRUCTIONS = (
    "CodexIOL provides private, read-only InvertirOnline investment context. "
    "Use IOL data as the source of truth for current holdings. This server cannot prepare, execute, or cancel transactions."
)
logger = logging.getLogger(__name__)
Result = TypeVar("Result")


def _observe_tool(name: str, call: Callable[[], Result]) -> Result:
    """Log operational metadata only; never tool arguments or financial output."""
    started = time.monotonic()
    outcome = "available"
    try:
        return call()
    except Exception as exc:
        outcome = f"error:{type(exc).__name__}"
        raise
    finally:
        elapsed_ms = round((time.monotonic() - started) * 1000)
        logger.info("mcp_tool=%s outcome=%s duration_ms=%s", name, outcome, elapsed_ms)


def _live_gateway() -> ReadOnlyIOLGateway:
    return configured_read_only_gateway()


def create_server(service: MCPReadOnlyService | None = None) -> MCPServer:
    service = service or MCPReadOnlyService(_live_gateway)
    server = MCPServer("CodexIOL", version="1.0.0", instructions=INSTRUCTIONS)

    @server.tool(
        description="Return the current IOL portfolio, cash, read-only account context and recent orders. Use it as the source of truth for actual holdings. Never prepares or executes transactions.",
        annotations=READ_ONLY,
    )
    def get_portfolio_snapshot(refresh: bool = False) -> PortfolioSnapshot:
        return _observe_tool("get_portfolio_snapshot", lambda: service.portfolio_snapshot(refresh=refresh))

    @server.tool(
        description="Return the structured private investor profile used to interpret the portfolio. It excludes credentials and unnecessary personal data.",
        annotations=READ_ONLY,
    )
    def get_investor_profile() -> InvestorProfileResponse:
        return _observe_tool("get_investor_profile", service.investor_profile)

    @server.tool(
        description="Return normalized read-only IOL movements for a bounded date range. Use it to understand recent cashflows and trades; it never changes account data.",
        annotations=READ_ONLY,
    )
    def get_movements(from_date: str | None = None, to_date: str | None = None, country: str = "argentina") -> MovementList:
        from datetime import date
        return _observe_tool(
            "get_movements",
            lambda: service.movements(date.fromisoformat(from_date) if from_date else None, date.fromisoformat(to_date) if to_date else None, country),
        )

    @server.tool(
        description="Return prior monthly reviews, human decisions and the current transition-plan summary. Historical records are distinct from current IOL holdings.",
        annotations=READ_ONLY,
    )
    def get_investment_history(limit: int = 6) -> InvestmentHistory:
        return _observe_tool("get_investment_history", lambda: service.investment_history(limit=limit))

    @server.tool(
        description="Return internal context for one symbol in one explicit market: current holding, related orders and movements, profile constraints and prior review memory. It does not perform external research.",
        annotations=READ_ONLY,
    )
    def get_asset_context(symbol: str, market: str, refresh: bool = False) -> AssetContext:
        return _observe_tool("get_asset_context", lambda: service.asset_context(symbol=symbol, market=market, refresh=refresh))

    @server.tool(
        description="Return the read-only capabilities and explicit financial actions that CodexIOL MCP cannot perform.",
        annotations=READ_ONLY,
    )
    def get_capabilities() -> Capabilities:
        return _observe_tool("get_capabilities", service.capabilities)

    return server


def main() -> None:
    transport = os.getenv("IOL_MCP_TRANSPORT", "streamable-http")
    if transport not in {"stdio", "streamable-http"}:
        raise RuntimeError("IOL_MCP_TRANSPORT must be stdio or streamable-http")
    server = create_server()
    if transport == "stdio":
        server.run(transport="stdio")
        return
    host = os.getenv("IOL_MCP_HOST", "127.0.0.1")
    port = int(os.getenv("IOL_MCP_PORT", "8000"))
    server.run(transport="streamable-http", host=host, port=port, streamable_http_path="/mcp")
