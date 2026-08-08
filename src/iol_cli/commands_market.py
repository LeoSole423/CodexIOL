from __future__ import annotations

from typing import Callable

import typer

from .common import fail_api, print_json
from .iol_client import IOLAPIError
from .util import normalize_country, normalize_market


def build_market_app(get_client: Callable) -> typer.Typer:
    app = typer.Typer(help="Market data from IOL")

    @app.command("quote")
    def quote(ctx: typer.Context, market: str = typer.Option(..., "--market"), symbol: str = typer.Option(..., "--symbol")):
        try: print_json(get_client(ctx).get_quote(normalize_market(market), symbol.strip().upper()))
        except IOLAPIError as exc: fail_api(exc)

    @app.command("instruments")
    def instruments(ctx: typer.Context, country: str = typer.Option("argentina", "--country")):
        try: print_json(get_client(ctx).get_instruments(normalize_country(country)))
        except IOLAPIError as exc: fail_api(exc)

    @app.command("panels")
    def panels(ctx: typer.Context, instrument: str = typer.Option(..., "--instrument"), country: str = typer.Option("argentina", "--country")):
        try: print_json(get_client(ctx).get_panels(normalize_country(country), instrument))
        except IOLAPIError as exc: fail_api(exc)

    @app.command("panel-quotes")
    def panel_quotes(ctx: typer.Context, instrument: str = typer.Option(..., "--instrument"), panel: str = typer.Option(..., "--panel"), country: str = typer.Option("argentina", "--country")):
        try: print_json(get_client(ctx).get_panel_quotes(instrument, panel, normalize_country(country)))
        except IOLAPIError as exc: fail_api(exc)

    return app
