from __future__ import annotations

from datetime import date, timedelta
from typing import Callable, Optional

import typer

from .common import fail_api, print_json
from .iol_client import IOLAPIError
from .util import normalize_country


def build_account_app(get_client: Callable) -> typer.Typer:
    app = typer.Typer(help="Account, portfolio and movement queries")

    @app.command("status")
    def status(ctx: typer.Context):
        try: print_json(get_client(ctx).get_account_status())
        except IOLAPIError as exc: fail_api(exc)

    @app.command("portfolio")
    def portfolio(ctx: typer.Context, country: str = typer.Option("argentina", "--country")):
        try: print_json(get_client(ctx).get_portfolio(normalize_country(country)))
        except IOLAPIError as exc: fail_api(exc)

    @app.command("movements")
    def movements(ctx: typer.Context, date_from: Optional[str] = typer.Option(None, "--from"), date_to: Optional[str] = typer.Option(None, "--to"), country: str = typer.Option("argentina", "--country"), currency: Optional[str] = typer.Option(None, "--currency")):
        end = date.fromisoformat(date_to) if date_to else date.today()
        start = date.fromisoformat(date_from) if date_from else end - timedelta(days=90)
        if start > end: raise typer.BadParameter("--from cannot be after --to")
        try: print_json(get_client(ctx).get_movements(start.isoformat(), end.isoformat(), normalize_country(country), currency))
        except IOLAPIError as exc: fail_api(exc)
        except ValueError as exc: raise typer.BadParameter("dates must use YYYY-MM-DD") from exc

    return app
