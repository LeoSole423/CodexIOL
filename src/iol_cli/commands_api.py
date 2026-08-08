from __future__ import annotations

import json
from typing import Callable, Optional

import typer

from .common import confirm, console, fail_api, print_json
from .iol_client import IOLAPIError

SAFE_METHODS = {"GET", "HEAD"}
ALLOWED_METHODS = SAFE_METHODS | {"POST", "PUT", "PATCH", "DELETE"}


def parse_payload(raw: Optional[str]):
    if raw is None: return None
    try: return json.loads(raw)
    except json.JSONDecodeError as exc: raise typer.BadParameter("--json must contain valid JSON") from exc


def build_api_app(get_client: Callable) -> typer.Typer:
    app = typer.Typer(help="Generic access to IOL API endpoints")

    @app.command("request")
    def request(ctx: typer.Context, method: str = typer.Argument(...), path: str = typer.Argument(...), json_payload: Optional[str] = typer.Option(None, "--json")):
        normalized = method.strip().upper()
        if normalized not in ALLOWED_METHODS: raise typer.BadParameter("method must be GET, HEAD, POST, PUT, PATCH or DELETE")
        if not path.startswith("/"): raise typer.BadParameter("path must start with /")
        payload = parse_payload(json_payload)
        if normalized not in SAFE_METHODS:
            print_json({"method": normalized, "path": path, "payload": payload})
            console.print("This generic request may mutate the real IOL account.")
            confirm()
        try: print_json(get_client(ctx).raw_request(normalized, path, payload))
        except IOLAPIError as exc: fail_api(exc)

    return app
