from typer.testing import CliRunner

from iol_cli.cli import app

runner = CliRunner()


def test_command_tree(cli_env):
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    for command in ("auth", "account", "market", "orders", "funds", "api"):
        assert command in result.stdout
    for retired in ("advisor", "batch", "engines", "simulate", "snapshot", "web"):
        assert retired not in result.stdout


def test_prepare_does_not_call_iol(cli_env):
    result = runner.invoke(app, ["orders", "prepare", "--side", "buy", "--market", "bcba", "--symbol", "GGAL", "--quantity", "2", "--price", "100"])
    assert result.exit_code == 0
    assert '"status": "prepared"' in result.stdout


def test_mutating_api_requires_confirmation(cli_env):
    result = runner.invoke(app, ["api", "request", "DELETE", "/api/v2/example"], input="NO\n")
    assert result.exit_code == 1
    assert "Operation cancelled" in result.stdout


def test_invalid_json_rejected_before_request(cli_env):
    result = runner.invoke(app, ["api", "request", "POST", "/api/v2/example", "--json", "not-json"])
    assert result.exit_code == 2
