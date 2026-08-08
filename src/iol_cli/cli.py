from __future__ import annotations

from dataclasses import dataclass

import typer

from .commands_account import build_account_app
from .commands_api import build_api_app
from .commands_funds import build_funds_app
from .commands_market import build_market_app
from .commands_orders import build_orders_app
from .common import console, fail_api
from .config import Config, ConfigError, load_config
from .iol_client import IOLAPIError, IOLClient


@dataclass
class CLIContext:
    config: Config


app = typer.Typer(add_completion=False, no_args_is_help=True, help="Operational CLI for InvertirOnline")


def get_config(ctx: typer.Context) -> Config:
    return ctx.obj.config


def get_client(ctx: typer.Context) -> IOLClient:
    config = get_config(ctx)
    return IOLClient(config.username, config.password, config.base_url, config.timeout)


@app.callback()
def main(ctx: typer.Context):
    try: ctx.obj = CLIContext(load_config())
    except ConfigError as exc: console.print(f"Configuration error: {exc}"); raise typer.Exit(code=1)


auth_app = typer.Typer(help="Authentication")


@auth_app.command("test")
def auth_test(ctx: typer.Context):
    try:
        get_client(ctx).authenticate()
        console.print("Authentication OK.")
    except IOLAPIError as exc: fail_api(exc)


app.add_typer(auth_app, name="auth")
app.add_typer(build_account_app(get_client), name="account")
app.add_typer(build_market_app(get_client), name="market")
app.add_typer(build_orders_app(get_client, get_config), name="orders")
app.add_typer(build_funds_app(get_client), name="funds")
app.add_typer(build_api_app(get_client), name="api")
