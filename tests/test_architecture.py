from pathlib import Path


def test_active_package_has_no_legacy_dependencies():
    forbidden = ("sqlite3", "fastapi", "iol_advisor", "iol_engines", "iol_web", "iol_shared", "iol_reconciliation", "legacy")
    for path in Path("src/iol_cli").glob("*.py"):
        text = path.read_text(encoding="utf-8").lower()
        for name in forbidden:
            assert name not in text, f"{path} contains forbidden dependency {name}"
