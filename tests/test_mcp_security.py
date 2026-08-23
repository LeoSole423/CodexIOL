from pathlib import Path

from iol_mcp.security import FORBIDDEN_TOOL_TERMS
from iol_mcp.server import create_server


def test_no_published_mcp_tool_can_mutate_financial_state(tmp_path):
    from iol_mcp.service import MCPReadOnlyService

    service = MCPReadOnlyService(lambda: None, profile_loader=lambda: {}, review_dir=tmp_path / "reports/monthly", decision_dir=tmp_path / "data/review")
    tool_names = {tool.name.lower() for tool in __import__("asyncio").run(create_server(service).list_tools())}

    for name in tool_names:
        assert not any(term in name for term in FORBIDDEN_TOOL_TERMS)


def test_mcp_source_has_no_operational_imports_or_raw_iol_api():
    forbidden = ("commands_orders", "commands_funds", "commands_api", "tickets", "iol_client", "raw_request")
    for path in Path("src/iol_mcp").rglob("*.py"):
        if path.name == "security.py":
            continue
        source = path.read_text(encoding="utf-8").lower()
        for item in forbidden:
            assert item not in source, f"{path} exposes {item}"
