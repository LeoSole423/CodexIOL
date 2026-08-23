from pathlib import Path


def test_active_package_has_no_legacy_dependencies():
    forbidden = ("sqlite3", "fastapi", "iol_advisor", "iol_engines", "iol_web", "iol_shared", "iol_reconciliation", "legacy")
    for path in Path("src/iol_cli").glob("*.py"):
        text = path.read_text(encoding="utf-8").lower()
        for name in forbidden:
            assert name not in text, f"{path} contains forbidden dependency {name}"


def test_review_is_separated_from_execution_code():
    forbidden_imports = ("commands_orders", "commands_funds", "commands_api", "import tickets", "from tickets", ".buy(", ".sell(", ".cancel_order(", ".fci_", ".raw_request(")
    for path in Path("src/iol_review").glob("*.py"):
        text = path.read_text(encoding="utf-8").lower()
        for name in forbidden_imports:
            assert name not in text, f"{path} contains execution dependency {name}"


def test_mcp_is_recursively_separated_from_execution_code():
    forbidden_imports = (
        "commands_orders", "commands_funds", "commands_api", "tickets", "iol_client",
        "legacy", "raw_request", ".buy(", ".sell(", ".cancel_order(", ".fci_",
    )
    for path in Path("src/iol_mcp").rglob("*.py"):
        if path.name == "security.py":
            continue
        text = path.read_text(encoding="utf-8").lower()
        for name in forbidden_imports:
            assert name not in text, f"{path} contains execution dependency {name}"
