from __future__ import annotations

import json
from typing import Any

import typer
from rich.console import Console

from .iol_client import IOLAPIError

console = Console()


def print_json(value: Any) -> None:
    console.print_json(json.dumps(value, ensure_ascii=True, indent=2, default=str))


def confirm() -> None:
    console.print("Type CONFIRMAR to continue:")
    if input("CONFIRMAR> ").strip() != "CONFIRMAR":
        console.print("Operation cancelled.")
        raise typer.Exit(code=1)


def fail_api(exc: IOLAPIError) -> None:
    console.print(f"IOL API error: {exc}")
    raise typer.Exit(code=1)
