from __future__ import annotations

from datetime import datetime
from typing import Callable, Dict, Optional

import typer

from .common import confirm, console, fail_api, print_json
from .iol_client import IOLAPIError
from .tickets import TicketError, TicketStore
from .util import default_valid_until, normalize_country, normalize_market, normalize_order_type, normalize_plazo, simulate_notional


def build_order_payload(side: str, market: str, symbol: str, quantity: Optional[float], price: Optional[float], amount: Optional[float], plazo: str, valid_until: Optional[str], order_type: str) -> Dict:
    side = side.strip().lower()
    if side not in {"buy", "sell"}: raise typer.BadParameter("--side must be buy or sell")
    symbol = symbol.strip().upper()
    if not symbol: raise typer.BadParameter("--symbol is required")
    if quantity is not None and quantity <= 0: raise typer.BadParameter("--quantity must be greater than zero")
    if amount is not None and amount <= 0: raise typer.BadParameter("--amount must be greater than zero")
    if price is not None and price <= 0: raise typer.BadParameter("--price must be greater than zero")
    normalized_type = normalize_order_type(order_type)
    if side == "sell" and quantity is None: raise typer.BadParameter("sell requires --quantity")
    if side == "sell" and amount is not None: raise typer.BadParameter("sell does not accept --amount")
    if side == "buy" and (quantity is None) == (amount is None): raise typer.BadParameter("buy requires exactly one of --quantity or --amount")
    if normalized_type == "precioLimite" and price is None: raise typer.BadParameter("limit orders require --price")
    if valid_until:
        try: datetime.fromisoformat(valid_until.replace("Z", "+00:00"))
        except ValueError as exc: raise typer.BadParameter("--valid-until must be ISO 8601") from exc
    payload: Dict = {"mercado": normalize_market(market), "simbolo": symbol, "plazo": normalize_plazo(plazo), "validez": valid_until or default_valid_until(), "tipoOrden": normalized_type}
    if quantity is not None: payload["cantidad"] = float(quantity)
    if amount is not None: payload["monto"] = float(amount)
    if price is not None: payload["precio"] = float(price)
    return payload


def build_orders_app(get_client: Callable, get_config: Callable) -> typer.Typer:
    app = typer.Typer(help="Query and safely execute orders")

    @app.command("list")
    def list_orders(ctx: typer.Context, status: Optional[str] = typer.Option(None, "--status"), date_from: Optional[str] = typer.Option(None, "--from"), date_to: Optional[str] = typer.Option(None, "--to"), country: Optional[str] = typer.Option(None, "--country"), number: Optional[int] = typer.Option(None, "--number")):
        params = {}
        if status: params["filtro.estado"] = status
        if date_from: params["filtro.fechaDesde"] = date_from
        if date_to: params["filtro.fechaHasta"] = date_to
        if country: params["filtro.pais"] = normalize_country(country)
        if number is not None: params["filtro.numero"] = number
        try: print_json(get_client(ctx).list_orders(params))
        except IOLAPIError as exc: fail_api(exc)

    @app.command("get")
    def get_order(ctx: typer.Context, number: int = typer.Argument(...)):
        try: print_json(get_client(ctx).get_order(number))
        except IOLAPIError as exc: fail_api(exc)

    @app.command("cancel")
    def cancel_order(ctx: typer.Context, number: int = typer.Argument(...)):
        console.print(f"Cancel IOL order #{number}")
        confirm()
        try: print_json(get_client(ctx).cancel_order(number))
        except IOLAPIError as exc: fail_api(exc)

    @app.command("prepare")
    def prepare(ctx: typer.Context, side: str = typer.Option(..., "--side"), market: str = typer.Option(..., "--market"), symbol: str = typer.Option(..., "--symbol"), quantity: Optional[float] = typer.Option(None, "--quantity"), price: Optional[float] = typer.Option(None, "--price"), amount: Optional[float] = typer.Option(None, "--amount"), plazo: str = typer.Option("t0", "--plazo"), valid_until: Optional[str] = typer.Option(None, "--valid-until"), order_type: str = typer.Option("limit", "--order-type"), especie_d: bool = typer.Option(False, "--especie-d")):
        try: payload = build_order_payload(side, market, symbol, quantity, price, amount, plazo, valid_until, order_type)
        except ValueError as exc: raise typer.BadParameter(str(exc)) from exc
        config = get_config(ctx)
        summary = simulate_notional(quantity, price, amount, config.commission_rate, config.commission_min, side.strip().lower())
        ticket = TicketStore(config.state_dir, config.ticket_ttl_minutes).create(side.strip().lower(), payload, summary, especie_d)
        print_json(ticket)

    @app.command("execute")
    def execute(ctx: typer.Context, ticket_id: str = typer.Argument(...)):
        config = get_config(ctx)
        store = TicketStore(config.state_dir, config.ticket_ttl_minutes)
        try: ticket = store.get_executable(ticket_id)
        except TicketError as exc: console.print(str(exc)); raise typer.Exit(code=1)
        print_json(ticket)
        confirm()
        client = get_client(ctx)
        try:
            # Claim before the network call. If the response is ambiguous, the ticket
            # remains non-executable and must be reconciled against IOL manually.
            store.mark_executing(ticket_id)
            result = client.buy(ticket["payload"], ticket["especie_d"]) if ticket["operation"] == "buy" else client.sell(ticket["payload"], ticket["especie_d"])
            store.mark_executed(ticket_id)
            print_json(result)
        except IOLAPIError as exc: fail_api(exc)

    return app
