from __future__ import annotations

from typing import Callable

import typer

from .common import confirm, console, fail_api, print_json
from .iol_client import IOLAPIError


def build_funds_app(get_client: Callable) -> typer.Typer:
    app = typer.Typer(help="FCI subscriptions and redemptions")

    @app.command("subscribe")
    def subscribe(ctx: typer.Context, symbol: str = typer.Option(..., "--symbol"), amount: float = typer.Option(..., "--amount"), validate: bool = typer.Option(False, "--validate")):
        if amount <= 0: raise typer.BadParameter("--amount must be greater than zero")
        payload = {"simbolo": symbol.strip().upper(), "monto": float(amount), "soloValidar": bool(validate)}
        print_json(payload)
        if not validate:
            console.print("This will submit a real FCI subscription.")
            confirm()
        try: print_json(get_client(ctx).fci_subscribe(payload))
        except IOLAPIError as exc: fail_api(exc)

    @app.command("redeem")
    def redeem(ctx: typer.Context, symbol: str = typer.Option(..., "--symbol"), quantity: float = typer.Option(..., "--quantity"), validate: bool = typer.Option(False, "--validate")):
        if quantity <= 0: raise typer.BadParameter("--quantity must be greater than zero")
        payload = {"simbolo": symbol.strip().upper(), "cantidad": float(quantity), "soloValidar": bool(validate)}
        print_json(payload)
        if not validate:
            console.print("This will submit a real FCI redemption.")
            confirm()
        try: print_json(get_client(ctx).fci_redeem(payload))
        except IOLAPIError as exc: fail_api(exc)

    return app
