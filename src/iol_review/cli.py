from __future__ import annotations

from datetime import date
from pathlib import Path

import typer
from rich.console import Console

from iol_cli.config import ConfigError, load_config
from iol_cli.iol_client import IOLClient

from .context import build_context, write_context
from .decisions import record_decision
from .gateway import ReadOnlyIOLGateway
from .profile import ProfileError, load_profile


console = Console()
app = typer.Typer(add_completion=False, no_args_is_help=True, help="Read-only monthly portfolio review support")
profile_app = typer.Typer(help="Validate the private investor profile")
context_app = typer.Typer(help="Export read-only IOL context")
decision_app = typer.Typer(help="Record a human decision about a review")


@profile_app.command("validate")
def profile_validate():
    try:
        load_profile()
        console.print("Investor profile is valid.")
    except ProfileError as exc:
        console.print(f"Profile error: {exc}")
        raise typer.Exit(1)


@context_app.command("export")
def context_export(as_of: str | None = typer.Option(None, "--as-of"), out: Path | None = typer.Option(None, "--out")):
    try:
        cutoff = date.fromisoformat(as_of) if as_of else date.today()
    except ValueError:
        raise typer.BadParameter("--as-of must use YYYY-MM-DD")
    try:
        config = load_config()
    except ConfigError as exc:
        console.print(f"Configuration error: {exc}")
        raise typer.Exit(1)
    target = out or Path("data/review") / f"{cutoff:%Y-%m}-context.json"
    context = build_context(ReadOnlyIOLGateway(IOLClient(config.username, config.password, config.base_url, config.timeout)), cutoff)
    write_context(target, context)
    console.print_json(data={"path": str(target), "content_hash": context["content_hash"], "warnings": context["warnings"]})


@decision_app.command("record")
def decision_record(review_id: str, status: str = typer.Option(..., "--status"), notes: str = typer.Option(..., "--notes")):
    try:
        destination = record_decision(Path("data/review") / f"{review_id}-decision.json", review_id, status, notes)
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    console.print(f"Decision recorded: {destination}")


app.add_typer(profile_app, name="profile")
app.add_typer(context_app, name="context")
app.add_typer(decision_app, name="decision")
