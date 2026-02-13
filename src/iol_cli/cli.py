import csv
import json
import sys
from datetime import date, datetime
from dataclasses import dataclass
from typing import Any, Dict, Optional

import typer
from rich.console import Console

from .config import ConfigError, load_config
from .db import connect, init_db, resolve_db_path
from .iol_client import IOLClient, IOLAPIError
from .snapshot import backfill_orders_and_snapshot, catchup_snapshot, run_snapshot
from .storage import add_pending, get_pending, remove_pending
from .batch import BatchError, plan_from_md, plan_template, run_batch
from .advisor_context import build_advisor_context_from_db_path, render_advisor_context_md
from .util import (
    default_valid_until,
    normalize_country,
    normalize_market,
    normalize_order_type,
    normalize_plazo,
    simulate_notional,
)

app = typer.Typer(add_completion=False, help="IOL CLI")
console = Console()

@dataclass
class CLIContext:
    config: Any
    base_url: str
    env: str
    verbose: bool


def _print_json(data: Any) -> None:
    try:
        text = json.dumps(data, ensure_ascii=True, indent=2)
        console.print_json(text)
    except Exception:
        console.print(data)


def _parse_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def _confirm_or_exit(confirm: Optional[str] = None) -> None:
    # Default: interactive confirmation. For automation, user must explicitly pass --confirm CONFIRMAR.
    if confirm is not None:
        if confirm.strip() != "CONFIRMAR":
            raise typer.BadParameter("--confirm must be exactly CONFIRMAR")
        return
    console.print("Type CONFIRMAR to continue:")
    value = input("CONFIRMAR> ").strip()
    if value != "CONFIRMAR":
        raise typer.Exit(code=1)


def _get_client(ctx: CLIContext) -> IOLClient:
    client = IOLClient(
        username=ctx.config.username,
        password=ctx.config.password,
        base_url=ctx.base_url,
        timeout=ctx.config.timeout,
    )
    return client


def _build_order_payload(
    side: str,
    market: str,
    symbol: str,
    quantity: Optional[float],
    price: Optional[float],
    amount: Optional[float],
    plazo: Optional[str],
    valid_until: Optional[str],
    order_type: Optional[str],
    source_id: Optional[int],
) -> Dict[str, Any]:
    if not market or not symbol:
        raise typer.BadParameter("market and symbol are required")
    if price is None:
        raise typer.BadParameter("price is required by API")
    if side == "buy":
        if quantity is None and amount is None:
            raise typer.BadParameter("quantity or amount is required for buy")
        if quantity is not None and amount is not None:
            raise typer.BadParameter("use only one of quantity or amount")
    if side == "sell" and quantity is None:
        raise typer.BadParameter("quantity is required for sell")
    if side == "sell" and amount is not None:
        raise typer.BadParameter("amount is not valid for sell")

    payload: Dict[str, Any] = {
        "mercado": normalize_market(market),
        "simbolo": symbol,
        "precio": float(price),
        "validez": valid_until or default_valid_until(),
    }
    if side == "buy":
        payload["plazo"] = normalize_plazo(plazo or "t0")
    if side == "sell" and plazo:
        payload["plazo"] = normalize_plazo(plazo)
    if quantity is not None:
        payload["cantidad"] = float(quantity)
    if amount is not None:
        payload["monto"] = float(amount)
    if order_type:
        payload["tipoOrden"] = normalize_order_type(order_type)
    if source_id is not None:
        payload["idFuente"] = int(source_id)
    return payload


def _simulate_and_store(ctx: CLIContext, side: str, payload: Dict[str, Any], especie_d: bool) -> str:
    summary = simulate_notional(
        quantity=payload.get("cantidad"),
        price=payload.get("precio"),
        amount=payload.get("monto"),
        commission_rate=ctx.config.commission_rate,
        commission_min=ctx.config.commission_min,
        side=side,
    )
    record = {
        "side": side,
        "payload": payload,
        "especie_d": especie_d,
        "summary": summary,
        "env": ctx.env,
        "base_url": ctx.base_url,
    }
    confirmation_id = add_pending(record)
    console.print(f"Simulation saved. confirmation_id: {confirmation_id}")
    _print_json({"payload": payload, "summary": summary})
    return confirmation_id


@app.callback()
def main(
    ctx: typer.Context,
    base_url: Optional[str] = typer.Option(None, "--base-url", help="Override base URL"),
    verbose: bool = typer.Option(False, "--verbose", help="Verbose output"),
):
    try:
        config = load_config()
        resolved = config.resolve_base_url(base_url_override=base_url)
    except ConfigError as exc:
        console.print(f"Config error: {exc}")
        raise typer.Exit(code=1)
    ctx.obj = CLIContext(config=config, base_url=resolved, env="real", verbose=verbose)


auth_app = typer.Typer(help="Authentication")
app.add_typer(auth_app, name="auth")


@auth_app.command("test")
def auth_test(ctx: typer.Context):
    """Authenticate and validate credentials."""
    client = _get_client(ctx.obj)
    try:
        client.authenticate()
        console.print(f"Auth OK. Base URL: {ctx.obj.base_url} Env: {ctx.obj.env}")
    except IOLAPIError as exc:
        console.print(f"Auth failed: {exc}")
        raise typer.Exit(code=1)


@app.command()
def portfolio(
    ctx: typer.Context,
    country: str = typer.Option("argentina", "--country", help="argentina or estados_Unidos"),
):
    client = _get_client(ctx.obj)
    try:
        data = client.get_portfolio(normalize_country(country))
        _print_json(data)
    except IOLAPIError as exc:
        console.print(f"Error: {exc}")
        raise typer.Exit(code=1)


market_app = typer.Typer(help="Market data")
app.add_typer(market_app, name="market")


@market_app.command("quote")
def market_quote(
    ctx: typer.Context,
    market: str = typer.Option(..., "--market", help="bcba, nyse, nasdaq"),
    symbol: str = typer.Option(..., "--symbol", help="Symbol"),
):
    client = _get_client(ctx.obj)
    try:
        data = client.get_quote(normalize_market(market), symbol)
        _print_json(data)
    except IOLAPIError as exc:
        console.print(f"Error: {exc}")
        raise typer.Exit(code=1)


@market_app.command("instruments")
def market_instruments(
    ctx: typer.Context,
    country: str = typer.Option("argentina", "--country", help="argentina or estados_Unidos"),
):
    client = _get_client(ctx.obj)
    try:
        data = client.get_instruments(normalize_country(country))
        _print_json(data)
    except IOLAPIError as exc:
        console.print(f"Error: {exc}")
        raise typer.Exit(code=1)


@market_app.command("panels")
def market_panels(
    ctx: typer.Context,
    instrument: str = typer.Option(..., "--instrument", help="Instrument name"),
    country: str = typer.Option("argentina", "--country", help="argentina or estados_Unidos"),
):
    client = _get_client(ctx.obj)
    try:
        data = client.get_panels(normalize_country(country), instrument)
        _print_json(data)
    except IOLAPIError as exc:
        console.print(f"Error: {exc}")
        raise typer.Exit(code=1)


@market_app.command("panel-quotes")
def market_panel_quotes(
    ctx: typer.Context,
    instrument: str = typer.Option(..., "--instrument", help="Instrument"),
    panel: str = typer.Option(..., "--panel", help="Panel"),
    country: str = typer.Option("argentina", "--country", help="argentina or estados_Unidos"),
):
    client = _get_client(ctx.obj)
    try:
        data = client.get_panel_quotes(instrument, panel, normalize_country(country))
        _print_json(data)
    except IOLAPIError as exc:
        console.print(f"Error: {exc}")
        raise typer.Exit(code=1)


orders_app = typer.Typer(help="Orders list/detail/cancel")
app.add_typer(orders_app, name="orders")


@orders_app.command("list")
def orders_list(
    ctx: typer.Context,
    status: Optional[str] = typer.Option(None, "--status", help="todas, pendientes, terminadas, canceladas"),
    date_from: Optional[str] = typer.Option(None, "--from", help="ISO date-time"),
    date_to: Optional[str] = typer.Option(None, "--to", help="ISO date-time"),
    country: Optional[str] = typer.Option(None, "--country", help="argentina or estados_Unidos"),
    number: Optional[int] = typer.Option(None, "--number", help="Order number"),
):
    client = _get_client(ctx.obj)
    params: Dict[str, Any] = {}
    if number is not None:
        params["filtro.numero"] = number
    if status:
        params["filtro.estado"] = status
    if date_from:
        params["filtro.fechaDesde"] = date_from
    if date_to:
        params["filtro.fechaHasta"] = date_to
    if country:
        params["filtro.pais"] = normalize_country(country)
    try:
        data = client.list_orders(params=params)
        _print_json(data)
    except IOLAPIError as exc:
        console.print(f"Error: {exc}")
        raise typer.Exit(code=1)


@orders_app.command("get")
def orders_get(ctx: typer.Context, number: int = typer.Argument(..., help="Order number")):
    client = _get_client(ctx.obj)
    try:
        data = client.get_order(number)
        _print_json(data)
    except IOLAPIError as exc:
        console.print(f"Error: {exc}")
        raise typer.Exit(code=1)


@orders_app.command("cancel")
def orders_cancel(
    ctx: typer.Context,
    number: int = typer.Argument(..., help="Order number"),
    confirm: Optional[str] = typer.Option(
        None,
        "--confirm",
        help="Execute without interactive prompt by passing CONFIRMAR",
    ),
):
    client = _get_client(ctx.obj)
    _confirm_or_exit(confirm)
    try:
        data = client.cancel_order(number)
        _print_json(data)
    except IOLAPIError as exc:
        console.print(f"Error: {exc}")
        raise typer.Exit(code=1)


order_app = typer.Typer(help="Order actions")
app.add_typer(order_app, name="order")


@order_app.command("simulate")
def order_simulate(
    ctx: typer.Context,
    side: str = typer.Option(..., "--side", help="buy or sell"),
    market: str = typer.Option(..., "--market"),
    symbol: str = typer.Option(..., "--symbol"),
    quantity: Optional[float] = typer.Option(None, "--quantity"),
    price: Optional[float] = typer.Option(None, "--price"),
    amount: Optional[float] = typer.Option(None, "--amount"),
    plazo: Optional[str] = typer.Option("t0", "--plazo"),
    valid_until: Optional[str] = typer.Option(None, "--valid-until"),
    order_type: Optional[str] = typer.Option("limit", "--order-type"),
    source_id: Optional[int] = typer.Option(None, "--source-id"),
    especie_d: bool = typer.Option(False, "--especie-d"),
):
    side_norm = side.strip().lower()
    if side_norm not in ("buy", "sell"):
        raise typer.BadParameter("side must be buy or sell")
    payload = _build_order_payload(
        side=side_norm,
        market=market,
        symbol=symbol,
        quantity=quantity,
        price=price,
        amount=amount,
        plazo=plazo,
        valid_until=valid_until,
        order_type=order_type,
        source_id=source_id,
    )
    _simulate_and_store(ctx.obj, side_norm, payload, especie_d)


@order_app.command("confirm")
def order_confirm(
    ctx: typer.Context,
    confirmation_id: str = typer.Argument(..., help="Confirmation ID"),
    confirm: Optional[str] = typer.Option(
        None,
        "--confirm",
        help="Execute without interactive prompt by passing CONFIRMAR",
    ),
):
    pending = get_pending(confirmation_id)
    if not pending:
        console.print("Confirmation ID not found.")
        raise typer.Exit(code=1)
    console.print("Pending order:")
    _print_json(pending)
    if pending.get("env") and pending.get("env") != ctx.obj.env:
        console.print(f"Warning: pending env is {pending.get('env')} but current env is {ctx.obj.env}")
    _confirm_or_exit(confirm)
    client = _get_client(ctx.obj)
    try:
        if pending.get("side") == "buy":
            result = client.buy(pending.get("payload", {}), especie_d=bool(pending.get("especie_d")))
        else:
            result = client.sell(pending.get("payload", {}), especie_d=bool(pending.get("especie_d")))
        remove_pending(confirmation_id)
        _print_json(result)
    except IOLAPIError as exc:
        console.print(f"Error: {exc}")
        raise typer.Exit(code=1)


@order_app.command("buy")
def order_buy(
    ctx: typer.Context,
    market: str = typer.Option(..., "--market"),
    symbol: str = typer.Option(..., "--symbol"),
    quantity: Optional[float] = typer.Option(None, "--quantity"),
    price: Optional[float] = typer.Option(None, "--price"),
    amount: Optional[float] = typer.Option(None, "--amount"),
    plazo: Optional[str] = typer.Option("t0", "--plazo"),
    valid_until: Optional[str] = typer.Option(None, "--valid-until"),
    order_type: Optional[str] = typer.Option("limit", "--order-type"),
    source_id: Optional[int] = typer.Option(None, "--source-id"),
    especie_d: bool = typer.Option(False, "--especie-d"),
    confirm: Optional[str] = typer.Option(
        None,
        "--confirm",
        help="Execute without interactive prompt by passing CONFIRMAR",
    ),
):
    payload = _build_order_payload(
        side="buy",
        market=market,
        symbol=symbol,
        quantity=quantity,
        price=price,
        amount=amount,
        plazo=plazo,
        valid_until=valid_until,
        order_type=order_type,
        source_id=source_id,
    )
    confirmation_id = _simulate_and_store(ctx.obj, "buy", payload, especie_d)
    _confirm_or_exit(confirm)
    client = _get_client(ctx.obj)
    try:
        result = client.buy(payload, especie_d=especie_d)
        remove_pending(confirmation_id)
        _print_json(result)
    except IOLAPIError as exc:
        console.print(f"Error: {exc}")
        raise typer.Exit(code=1)


@order_app.command("sell")
def order_sell(
    ctx: typer.Context,
    market: str = typer.Option(..., "--market"),
    symbol: str = typer.Option(..., "--symbol"),
    quantity: Optional[float] = typer.Option(None, "--quantity"),
    price: Optional[float] = typer.Option(None, "--price"),
    plazo: Optional[str] = typer.Option(None, "--plazo"),
    valid_until: Optional[str] = typer.Option(None, "--valid-until"),
    order_type: Optional[str] = typer.Option("limit", "--order-type"),
    source_id: Optional[int] = typer.Option(None, "--source-id"),
    especie_d: bool = typer.Option(False, "--especie-d"),
    confirm: Optional[str] = typer.Option(
        None,
        "--confirm",
        help="Execute without interactive prompt by passing CONFIRMAR",
    ),
):
    payload = _build_order_payload(
        side="sell",
        market=market,
        symbol=symbol,
        quantity=quantity,
        price=price,
        amount=None,
        plazo=plazo,
        valid_until=valid_until,
        order_type=order_type,
        source_id=source_id,
    )
    confirmation_id = _simulate_and_store(ctx.obj, "sell", payload, especie_d)
    _confirm_or_exit(confirm)
    client = _get_client(ctx.obj)
    try:
        result = client.sell(payload, especie_d=especie_d)
        remove_pending(confirmation_id)
        _print_json(result)
    except IOLAPIError as exc:
        console.print(f"Error: {exc}")
        raise typer.Exit(code=1)


fci_app = typer.Typer(help="FCI operations")
app.add_typer(fci_app, name="fci")


@fci_app.command("subscribe")
def fci_subscribe(
    ctx: typer.Context,
    symbol: str = typer.Option(..., "--symbol"),
    amount: float = typer.Option(..., "--amount"),
    validate: bool = typer.Option(False, "--validate", help="Validation only"),
    confirm: Optional[str] = typer.Option(
        None,
        "--confirm",
        help="Execute without interactive prompt by passing CONFIRMAR",
    ),
):
    payload = {
        "simbolo": symbol,
        "monto": float(amount),
        "soloValidar": bool(validate),
    }
    if validate:
        client = _get_client(ctx.obj)
        try:
            result = client.fci_subscribe(payload)
            _print_json(result)
        except IOLAPIError as exc:
            console.print(f"Error: {exc}")
            raise typer.Exit(code=1)
        return
    console.print("Simulation:")
    _print_json(payload)
    _confirm_or_exit(confirm)
    client = _get_client(ctx.obj)
    try:
        result = client.fci_subscribe({"simbolo": symbol, "monto": float(amount), "soloValidar": False})
        _print_json(result)
    except IOLAPIError as exc:
        console.print(f"Error: {exc}")
        raise typer.Exit(code=1)


@fci_app.command("redeem")
def fci_redeem(
    ctx: typer.Context,
    symbol: str = typer.Option(..., "--symbol"),
    quantity: float = typer.Option(..., "--quantity"),
    validate: bool = typer.Option(False, "--validate", help="Validation only"),
    confirm: Optional[str] = typer.Option(
        None,
        "--confirm",
        help="Execute without interactive prompt by passing CONFIRMAR",
    ),
):
    payload = {
        "simbolo": symbol,
        "cantidad": float(quantity),
        "soloValidar": bool(validate),
    }
    if validate:
        client = _get_client(ctx.obj)
        try:
            result = client.fci_redeem(payload)
            _print_json(result)
        except IOLAPIError as exc:
            console.print(f"Error: {exc}")
            raise typer.Exit(code=1)
        return
    console.print("Simulation:")
    _print_json(payload)
    _confirm_or_exit(confirm)
    client = _get_client(ctx.obj)
    try:
        result = client.fci_redeem({"simbolo": symbol, "cantidad": float(quantity), "soloValidar": False})
        _print_json(result)
    except IOLAPIError as exc:
        console.print(f"Error: {exc}")
        raise typer.Exit(code=1)


@app.command()
def raw(
    ctx: typer.Context,
    method: str = typer.Argument(..., help="HTTP method"),
    path: str = typer.Argument(..., help="API path, e.g. /api/v2/portafolio/argentina"),
    json_payload: Optional[str] = typer.Option(None, "--json", help="Raw JSON string"),
):
    client = _get_client(ctx.obj)
    payload = None
    raw_json = None
    if json_payload:
        try:
            payload = json.loads(json_payload)
        except json.JSONDecodeError:
            raw_json = json_payload
    try:
        result = client.raw_request(method.upper(), path, payload=payload, raw_json=raw_json)
        _print_json(result)
    except IOLAPIError as exc:
        console.print(f"Error: {exc}")
        raise typer.Exit(code=1)


snapshot_app = typer.Typer(help="Snapshots")
app.add_typer(snapshot_app, name="snapshot")


@snapshot_app.command("run")
def snapshot_run(
    ctx: typer.Context,
    country: str = typer.Option("argentina", "--country"),
    source: str = typer.Option("manual", "--source"),
    force: bool = typer.Option(
        False,
        "--force",
        help="Overwrite an existing snapshot even if it is closer to the market close time.",
    ),
    mode: str = typer.Option(
        "close",
        "--mode",
        help="Snapshot mode: 'close' (default) keeps daily semantics; 'live' uses today's date intraday.",
    ),
    only_market_open: bool = typer.Option(
        False,
        "--only-market-open",
        help="If set, skips snapshot when the market is closed (uses IOL_MARKET_OPEN_TIME/IOL_MARKET_CLOSE_TIME).",
    ),
):
    client = _get_client(ctx.obj)
    try:
        result = run_snapshot(
            client,
            ctx.obj.config,
            country,
            source=source,
            force=force,
            mode=mode,
            only_market_open=only_market_open,
        )
        _print_json(result)
    except Exception as exc:
        console.print(f"Snapshot error: {exc}")
        raise typer.Exit(code=1)


@snapshot_app.command("catchup")
def snapshot_catchup(
    ctx: typer.Context,
    country: str = typer.Option("argentina", "--country"),
):
    client = _get_client(ctx.obj)
    try:
        result = catchup_snapshot(client, ctx.obj.config, country)
        _print_json(result)
    except Exception as exc:
        console.print(f"Catchup error: {exc}")
        raise typer.Exit(code=1)


@snapshot_app.command("backfill")
def snapshot_backfill(
    ctx: typer.Context,
    date_from: str = typer.Option(..., "--from"),
    date_to: str = typer.Option(..., "--to"),
    country: str = typer.Option("argentina", "--country"),
):
    client = _get_client(ctx.obj)
    try:
        result = backfill_orders_and_snapshot(
            client,
            ctx.obj.config,
            country,
            _parse_date(date_from),
            _parse_date(date_to),
        )
        _print_json(result)
    except Exception as exc:
        console.print(f"Backfill error: {exc}")
        raise typer.Exit(code=1)


data_app = typer.Typer(help="Local data access")
app.add_typer(data_app, name="data")


batch_app = typer.Typer(help="Batch execution from a JSON plan")
app.add_typer(batch_app, name="batch")


@batch_app.command("template")
def batch_template():
    """Print an example batch plan JSON."""
    _print_json(plan_template())


@batch_app.command("validate")
def batch_validate(
    ctx: typer.Context,
    plan: str = typer.Option(..., "--plan", help="Path to plan JSON"),
    price_mode: Optional[str] = typer.Option(None, "--price-mode", help="fast|mid|last override"),
    default_market: str = typer.Option("bcba", "--default-market"),
    default_plazo: str = typer.Option("t1", "--default-plazo"),
):
    client = _get_client(ctx.obj)
    try:
        result = run_batch(
            client=client,
            config=ctx.obj.config,
            plan_path=plan,
            dry_run=True,
            price_mode_override=price_mode,
            default_market=default_market,
            default_plazo=default_plazo,
            confirm_enabled=False,
        )
        _print_json(result)
    except (BatchError, IOLAPIError) as exc:
        console.print(f"Error: {exc}")
        raise typer.Exit(code=1)


@batch_app.command("run")
def batch_run(
    ctx: typer.Context,
    plan: str = typer.Option(..., "--plan", help="Path to plan JSON"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Validate only; do not execute"),
    price_mode: Optional[str] = typer.Option(None, "--price-mode", help="fast|mid|last override"),
    default_market: str = typer.Option("bcba", "--default-market"),
    default_plazo: str = typer.Option("t1", "--default-plazo"),
    confirm: Optional[str] = typer.Option(
        None,
        "--confirm",
        help="Execute orders by passing CONFIRMAR. Without this flag the command behaves as dry-run.",
    ),
):
    confirm_enabled = False
    if confirm is not None:
        if confirm.strip() != "CONFIRMAR":
            raise typer.BadParameter("--confirm must be exactly CONFIRMAR")
        confirm_enabled = True

    def _preview_printer(ops: Any) -> None:
        console.print("Preview (prepared ops):")
        _print_json({"ops": ops})

    client = _get_client(ctx.obj)
    try:
        result = run_batch(
            client=client,
            config=ctx.obj.config,
            plan_path=plan,
            dry_run=bool(dry_run),
            price_mode_override=price_mode,
            default_market=default_market,
            default_plazo=default_plazo,
            confirm_enabled=confirm_enabled,
            on_preview=_preview_printer if (confirm_enabled and not dry_run) else None,
        )
        _print_json(result)
    except (BatchError, IOLAPIError) as exc:
        console.print(f"Error: {exc}")
        raise typer.Exit(code=1)


@batch_app.command("from-md")
def batch_from_md(
    md: str = typer.Option(..., "--md", help="Markdown file path"),
    out: str = typer.Option(..., "--out", help="Output JSON plan path"),
):
    """Parse a rebalance markdown and generate a plan JSON."""
    try:
        plan = plan_from_md(md, out)
        _print_json({"out": out, "plan": plan})
    except BatchError as exc:
        console.print(f"Error: {exc}")
        raise typer.Exit(code=1)


advisor_app = typer.Typer(help="Advisor utilities")
app.add_typer(advisor_app, name="advisor")


def _maybe_read_stdin(value: str) -> str:
    # Allow `--prompt -` / `--response -` to read multi-line text from stdin.
    if value.strip() == "-":
        return sys.stdin.read()
    return value


@advisor_app.command("context")
def advisor_context(
    ctx: typer.Context,
    as_of: Optional[str] = typer.Option(None, "--as-of", help="Optional YYYY-MM-DD; uses snapshot on-or-before date"),
    limit: int = typer.Option(10, "--limit", min=1, max=100, help="Top N for movers/tables"),
    history_days: int = typer.Option(365, "--history-days", min=1, max=3650, help="Days of history to include"),
    include_cash: bool = typer.Option(True, "--include-cash/--no-include-cash", help="Include ARS cash in allocation"),
    include_orders: bool = typer.Option(False, "--include-orders", help="Include recent orders (from local DB)"),
    orders_limit: int = typer.Option(20, "--orders-limit", min=1, max=500, help="Max orders to include when enabled"),
    out: Optional[str] = typer.Option(None, "--out", help="Write output to file instead of stdout"),
    fmt: str = typer.Option("json", "--format", help="json|md"),
):
    """Build a JSON context pack from the local SQLite DB for portfolio analysis (no API calls)."""
    db_path = resolve_db_path(ctx.obj.config.db_path)
    payload = build_advisor_context_from_db_path(
        db_path=db_path,
        as_of=as_of,
        limit=int(limit),
        history_days=int(history_days),
        include_cash=bool(include_cash),
        include_orders=bool(include_orders),
        orders_limit=int(orders_limit),
    )

    fmt_norm = (fmt or "").strip().lower()
    if fmt_norm not in ("json", "md"):
        raise typer.BadParameter("--format must be json or md")

    if fmt_norm == "json":
        if out:
            text = json.dumps(payload, ensure_ascii=True, indent=2) + "\n"
            with open(out, "w", encoding="utf-8") as f:
                f.write(text)
            _print_json({"out": out})
            return
        _print_json(payload)
        return

    text = render_advisor_context_md(payload)
    if out:
        with open(out, "w", encoding="utf-8") as f:
            f.write(text)
        _print_json({"out": out})
        return
    console.print(text)


@advisor_app.command("log")
def advisor_log(
    ctx: typer.Context,
    prompt: str = typer.Option(..., "--prompt", help="Original user prompt (or '-' to read stdin)"),
    response: str = typer.Option(..., "--response", help="Assistant response (or '-' to read stdin)"),
    snapshot_date: Optional[str] = typer.Option(None, "--snapshot-date", help="Optional YYYY-MM-DD context"),
):
    db_path = resolve_db_path(ctx.obj.config.db_path)
    conn = connect(db_path)
    init_db(conn)
    try:
        created_at = datetime.utcnow().isoformat(timespec="seconds") + "Z"
        resolved_snapshot = snapshot_date
        if resolved_snapshot is None:
            row = conn.execute(
                "SELECT snapshot_date FROM portfolio_snapshots ORDER BY snapshot_date DESC LIMIT 1"
            ).fetchone()
            resolved_snapshot = row["snapshot_date"] if row else None

        prompt_text = _maybe_read_stdin(prompt)
        response_text = _maybe_read_stdin(response)
        cur = conn.execute(
            """
            INSERT INTO advisor_logs (created_at, snapshot_date, prompt, response, env, base_url)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                created_at,
                resolved_snapshot,
                prompt_text,
                response_text,
                ctx.obj.env,
                ctx.obj.base_url,
            ),
        )
        conn.commit()
        _print_json(
            {
                "id": cur.lastrowid,
                "created_at": created_at,
                "snapshot_date": resolved_snapshot,
            }
        )
    finally:
        conn.close()


@advisor_app.command("list")
def advisor_list(
    ctx: typer.Context,
    limit: int = typer.Option(20, "--limit", min=1, max=200),
):
    db_path = resolve_db_path(ctx.obj.config.db_path)
    conn = connect(db_path)
    init_db(conn)
    try:
        rows = conn.execute(
            """
            SELECT id, created_at, snapshot_date, env, base_url, prompt, response
            FROM advisor_logs
            ORDER BY id DESC
            LIMIT ?
            """,
            (int(limit),),
        ).fetchall()
        data = [dict(row) for row in rows]
        _print_json(data)
    finally:
        conn.close()


@data_app.command("query")
def data_query(
    ctx: typer.Context,
    sql: str = typer.Argument(..., help="SQL SELECT query"),
):
    query = sql.strip()
    if not query.lower().startswith("select"):
        console.print("Only SELECT queries are allowed.")
        raise typer.Exit(code=1)
    db_path = resolve_db_path(ctx.obj.config.db_path)
    conn = connect(db_path)
    init_db(conn)
    try:
        rows = conn.execute(query).fetchall()
        data = [dict(row) for row in rows]
        _print_json(data)
    finally:
        conn.close()


@data_app.command("export")
def data_export(
    ctx: typer.Context,
    table: str = typer.Option(..., "--table"),
    fmt: str = typer.Option("json", "--format"),
):
    table_name = table.strip()
    if table_name not in (
        "portfolio_snapshots",
        "portfolio_assets",
        "account_balances",
        "orders",
        "snapshot_runs",
        "advisor_logs",
        "batch_runs",
        "batch_ops",
    ):
        console.print("Invalid table name.")
        raise typer.Exit(code=1)
    db_path = resolve_db_path(ctx.obj.config.db_path)
    conn = connect(db_path)
    init_db(conn)
    try:
        rows = conn.execute(f"SELECT * FROM {table_name}").fetchall()
        data = [dict(row) for row in rows]
    finally:
        conn.close()
    fmt_norm = fmt.strip().lower()
    if fmt_norm == "json":
        _print_json(data)
        return
    if fmt_norm == "csv":
        if not data:
            return
        writer = csv.DictWriter(sys.stdout, fieldnames=data[0].keys())
        writer.writeheader()
        for row in data:
            writer.writerow(row)
        return
    console.print("Unsupported format. Use json or csv.")
    raise typer.Exit(code=1)


@app.command("web")
def web(
    host: str = typer.Option("0.0.0.0", "--host"),
    port: int = typer.Option(8000, "--port"),
    reload: bool = typer.Option(False, "--reload"),
):
    """Run the local portfolio dashboard web app (reads snapshots from SQLite)."""
    import uvicorn

    uvicorn.run("iol_web.app:app", host=host, port=int(port), reload=bool(reload))


if __name__ == "__main__":
    app()
