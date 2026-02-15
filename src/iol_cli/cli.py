import csv
import json
import sys
from datetime import date, datetime, timedelta, timezone
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import typer
from rich.console import Console

from .config import ConfigError, load_config
from .db import connect, init_db, resolve_db_path
from .iol_client import IOLClient, IOLAPIError
from .snapshot import backfill_orders_and_snapshot, catchup_snapshot, run_snapshot
from .storage import add_pending, get_pending, remove_pending
from .batch import BatchError, plan_from_md, plan_template, run_batch
from .advisor_context import build_advisor_context_from_db_path, render_advisor_context_md
from .evidence_fetch import collect_symbol_evidence
from .opportunities import (
    build_candidates,
    latest_metrics_by_symbol,
    panel_rows,
    parse_iso_date,
    price_series_by_symbol,
    report_markdown,
    snapshot_row_from_panel,
    snapshot_row_from_quote,
)
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


def _parse_iso_date_optional(value: Optional[str], label: str) -> Optional[str]:
    if value is None:
        return None
    v = value.strip()
    if not v:
        return None
    try:
        date.fromisoformat(v)
    except Exception as exc:
        raise typer.BadParameter(f"{label} must be YYYY-MM-DD") from exc
    return v


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


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
advisor_alert_app = typer.Typer(help="Manual advisor alerts")
advisor_event_app = typer.Typer(help="Manual advisor events")
advisor_evidence_app = typer.Typer(help="Web evidence for symbols")
advisor_opp_app = typer.Typer(help="Opportunity pipeline (ranking)")
advisor_app.add_typer(advisor_alert_app, name="alert")
advisor_app.add_typer(advisor_event_app, name="event")
advisor_app.add_typer(advisor_evidence_app, name="evidence")
advisor_app.add_typer(advisor_opp_app, name="opportunities")

_ALERT_SEVERITIES = {"low", "medium", "high"}
_ALERT_STATUSES = {"open", "closed", "all"}
_EVENT_TYPES = {"note", "macro", "portfolio", "order", "other"}
_CONFIDENCE_LEVELS = {"low", "medium", "high"}
_OPP_MODES = {"new", "rebuy", "both"}
_OPP_UNIVERSES = {"bcba_cedears"}


def _maybe_read_stdin(value: str) -> str:
    # Allow `--prompt -` / `--response -` to read multi-line text from stdin.
    if value.strip() == "-":
        return sys.stdin.read()
    return value


def _normalize_enum(value: str, label: str, allowed: set) -> str:
    v = (value or "").strip().lower()
    if v not in allowed:
        allowed_txt = "|".join(sorted(allowed))
        raise typer.BadParameter(f"{label} must be {allowed_txt}")
    return v


def _latest_snapshot_date(conn) -> Optional[str]:
    row = conn.execute("SELECT snapshot_date FROM portfolio_snapshots ORDER BY snapshot_date DESC LIMIT 1").fetchone()
    return str(row["snapshot_date"]) if row and row["snapshot_date"] else None


def _load_holdings_map_from_context(ctx_payload: Dict[str, Any]) -> Dict[str, float]:
    out: Dict[str, float] = {}
    for r in ((ctx_payload or {}).get("assets") or {}).get("rows") or []:
        sym = str(r.get("symbol") or "").strip()
        if not sym:
            continue
        out[sym] = float(r.get("total_value") or 0.0)
    return out


def _load_market_snapshot_rows(conn, as_of: str) -> List[Dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT snapshot_date, symbol, market, last_price, bid, ask, spread_pct, daily_var_pct, operations_count, volume_amount, source
        FROM market_symbol_snapshots
        WHERE snapshot_date <= ?
        ORDER BY snapshot_date ASC
        """,
        (as_of,),
    ).fetchall()
    return [dict(r) for r in rows]


def _load_evidence_rows_grouped(conn, as_of: str, lookback_days: int = 60) -> Dict[str, List[Dict[str, Any]]]:
    d = date.fromisoformat(as_of)
    cutoff = (d - timedelta(days=int(lookback_days))).isoformat() + "T00:00:00Z"
    rows = conn.execute(
        """
        SELECT symbol, query, source_name, source_url, published_date, retrieved_at_utc, claim,
               confidence, date_confidence, notes, conflict_key
        FROM advisor_evidence
        WHERE retrieved_at_utc >= ?
        ORDER BY retrieved_at_utc DESC
        """,
        (cutoff,),
    ).fetchall()
    out: Dict[str, List[Dict[str, Any]]] = {}
    for r in rows:
        sym = str(r["symbol"])
        out.setdefault(sym, []).append(dict(r))
    return out


def _pick_symbols_for_auto_evidence(
    holdings_map: Dict[str, float],
    latest_metrics: Dict[str, Dict[str, Any]],
    max_symbols: int,
) -> List[str]:
    chosen: List[str] = []
    for s in sorted(holdings_map.keys()):
        if s not in chosen:
            chosen.append(s)
    if len(chosen) >= int(max_symbols):
        return chosen[: int(max_symbols)]
    ranked = sorted(
        latest_metrics.values(),
        key=lambda r: float(r.get("volume_amount") or 0.0),
        reverse=True,
    )
    for r in ranked:
        s = str(r.get("symbol") or "").strip().upper()
        if not s or s in chosen:
            continue
        chosen.append(s)
        if len(chosen) >= int(max_symbols):
            break
    return chosen


def _store_evidence_rows(conn, rows: List[Dict[str, Any]]) -> int:
    inserted = 0
    for r in rows:
        sym = str(r.get("symbol") or "").strip().upper()
        query_v = str(r.get("query") or "").strip()
        source_name_v = str(r.get("source_name") or "").strip()
        source_url_v = str(r.get("source_url") or "").strip()
        claim_v = str(r.get("claim") or "").strip()
        conf_v = str(r.get("confidence") or "").strip().lower()
        date_conf_v = str(r.get("date_confidence") or "").strip().lower()
        if (
            not sym
            or not query_v
            or not source_name_v
            or not source_url_v
            or not claim_v
            or conf_v not in _CONFIDENCE_LEVELS
            or date_conf_v not in _CONFIDENCE_LEVELS
        ):
            continue
        conn.execute(
            """
            INSERT INTO advisor_evidence (
                created_at, symbol, query, source_name, source_url, published_date, retrieved_at_utc,
                claim, confidence, date_confidence, notes, conflict_key
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(r.get("retrieved_at_utc") or _utc_now_iso()),
                sym,
                query_v,
                source_name_v,
                source_url_v,
                r.get("published_date"),
                str(r.get("retrieved_at_utc") or _utc_now_iso()),
                claim_v,
                conf_v,
                date_conf_v,
                r.get("notes"),
                r.get("conflict_key"),
            ),
        )
        inserted += 1
    return inserted


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


@advisor_alert_app.command("create")
def advisor_alert_create(
    ctx: typer.Context,
    alert_type: str = typer.Option(..., "--type", help="Alert type"),
    title: str = typer.Option(..., "--title", help="Short alert title"),
    description: str = typer.Option(..., "--description", help="Alert detail"),
    severity: str = typer.Option("medium", "--severity", help="low|medium|high"),
    symbol: Optional[str] = typer.Option(None, "--symbol"),
    snapshot_date: Optional[str] = typer.Option(None, "--snapshot-date", help="Optional YYYY-MM-DD"),
    due_date: Optional[str] = typer.Option(None, "--due-date", help="Optional YYYY-MM-DD"),
):
    sev = _normalize_enum(severity, "--severity", _ALERT_SEVERITIES)
    snap = _parse_iso_date_optional(snapshot_date, "--snapshot-date")
    due = _parse_iso_date_optional(due_date, "--due-date")
    alert_type_v = alert_type.strip()
    title_v = title.strip()
    description_v = description.strip()
    symbol_v = symbol.strip() if symbol and symbol.strip() else None
    if not alert_type_v:
        raise typer.BadParameter("--type is required")
    if not title_v:
        raise typer.BadParameter("--title is required")
    if not description_v:
        raise typer.BadParameter("--description is required")

    now = _utc_now_iso()
    db_path = resolve_db_path(ctx.obj.config.db_path)
    conn = connect(db_path)
    init_db(conn)
    try:
        cur = conn.execute(
            """
            INSERT INTO advisor_alerts (
                created_at, updated_at, status, severity, alert_type, title, description, symbol, snapshot_date, due_date
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (now, now, "open", sev, alert_type_v, title_v, description_v, symbol_v, snap, due),
        )
        conn.commit()
        _print_json(
            {
                "id": cur.lastrowid,
                "status": "open",
                "severity": sev,
                "type": alert_type_v,
                "symbol": symbol_v,
                "snapshot_date": snap,
                "due_date": due,
                "created_at": now,
            }
        )
    finally:
        conn.close()


@advisor_alert_app.command("list")
def advisor_alert_list(
    ctx: typer.Context,
    status: str = typer.Option("open", "--status", help="open|closed|all"),
    severity: Optional[str] = typer.Option(None, "--severity", help="low|medium|high"),
    symbol: Optional[str] = typer.Option(None, "--symbol"),
    limit: int = typer.Option(50, "--limit", min=1, max=500),
):
    status_v = _normalize_enum(status, "--status", _ALERT_STATUSES)
    severity_v = None
    if severity is not None:
        severity_v = _normalize_enum(severity, "--severity", _ALERT_SEVERITIES)
    symbol_v = symbol.strip() if symbol and symbol.strip() else None

    where: List[str] = []
    params: List[Any] = []
    if status_v != "all":
        where.append("status = ?")
        params.append(status_v)
    if severity_v is not None:
        where.append("severity = ?")
        params.append(severity_v)
    if symbol_v is not None:
        where.append("symbol = ?")
        params.append(symbol_v)

    sql = """
        SELECT id, created_at, updated_at, status, severity, alert_type, title, description,
               symbol, snapshot_date, due_date, closed_at, closed_reason
        FROM advisor_alerts
    """
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY CASE severity WHEN 'high' THEN 3 WHEN 'medium' THEN 2 ELSE 1 END DESC, due_date ASC, id DESC LIMIT ?"
    params.append(int(limit))

    db_path = resolve_db_path(ctx.obj.config.db_path)
    conn = connect(db_path)
    init_db(conn)
    try:
        rows = conn.execute(sql, tuple(params)).fetchall()
        _print_json([dict(r) for r in rows])
    finally:
        conn.close()


@advisor_alert_app.command("close")
def advisor_alert_close(
    ctx: typer.Context,
    alert_id: int = typer.Option(..., "--id", min=1, help="Alert ID"),
    reason: str = typer.Option(..., "--reason", help="Why the alert is closed"),
):
    reason_v = reason.strip()
    if not reason_v:
        raise typer.BadParameter("--reason is required")

    now = _utc_now_iso()
    db_path = resolve_db_path(ctx.obj.config.db_path)
    conn = connect(db_path)
    init_db(conn)
    try:
        row = conn.execute(
            "SELECT id, status, severity, alert_type, title, symbol, snapshot_date, due_date, closed_at, closed_reason FROM advisor_alerts WHERE id = ?",
            (int(alert_id),),
        ).fetchone()
        if not row:
            console.print("Alert not found.")
            raise typer.Exit(code=1)

        if row["status"] != "closed":
            conn.execute(
                """
                UPDATE advisor_alerts
                SET status = 'closed', updated_at = ?, closed_at = ?, closed_reason = ?
                WHERE id = ?
                """,
                (now, now, reason_v, int(alert_id)),
            )
            conn.commit()
            _print_json(
                {
                    "id": int(alert_id),
                    "status": "closed",
                    "closed_at": now,
                    "closed_reason": reason_v,
                }
            )
            return

        _print_json(
            {
                "id": int(alert_id),
                "status": "closed",
                "closed_at": row["closed_at"],
                "closed_reason": row["closed_reason"],
                "already_closed": True,
            }
        )
    finally:
        conn.close()


@advisor_event_app.command("add")
def advisor_event_add(
    ctx: typer.Context,
    event_type: str = typer.Option(..., "--type", help="note|macro|portfolio|order|other"),
    title: str = typer.Option(..., "--title"),
    description: Optional[str] = typer.Option(None, "--description"),
    symbol: Optional[str] = typer.Option(None, "--symbol"),
    snapshot_date: Optional[str] = typer.Option(None, "--snapshot-date", help="Optional YYYY-MM-DD"),
    alert_id: Optional[int] = typer.Option(None, "--alert-id", min=1),
    payload_json: Optional[str] = typer.Option(None, "--payload-json", help="Optional raw JSON object"),
):
    event_type_v = _normalize_enum(event_type, "--type", _EVENT_TYPES)
    title_v = title.strip()
    if not title_v:
        raise typer.BadParameter("--title is required")
    description_v = description.strip() if description and description.strip() else None
    symbol_v = symbol.strip() if symbol and symbol.strip() else None
    snap = _parse_iso_date_optional(snapshot_date, "--snapshot-date")

    payload_v = None
    if payload_json is not None:
        try:
            obj = json.loads(payload_json)
        except json.JSONDecodeError as exc:
            raise typer.BadParameter("--payload-json must be valid JSON") from exc
        payload_v = json.dumps(obj, ensure_ascii=True)

    now = _utc_now_iso()
    db_path = resolve_db_path(ctx.obj.config.db_path)
    conn = connect(db_path)
    init_db(conn)
    try:
        if alert_id is not None:
            row = conn.execute("SELECT id FROM advisor_alerts WHERE id = ?", (int(alert_id),)).fetchone()
            if not row:
                console.print("Alert ID not found.")
                raise typer.Exit(code=1)
        cur = conn.execute(
            """
            INSERT INTO advisor_events (created_at, event_type, title, description, symbol, snapshot_date, alert_id, payload_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (now, event_type_v, title_v, description_v, symbol_v, snap, int(alert_id) if alert_id is not None else None, payload_v),
        )
        conn.commit()
        _print_json(
            {
                "id": cur.lastrowid,
                "created_at": now,
                "type": event_type_v,
                "title": title_v,
                "symbol": symbol_v,
                "snapshot_date": snap,
                "alert_id": int(alert_id) if alert_id is not None else None,
            }
        )
    finally:
        conn.close()


@advisor_event_app.command("list")
def advisor_event_list(
    ctx: typer.Context,
    event_type: Optional[str] = typer.Option(None, "--type", help="note|macro|portfolio|order|other"),
    symbol: Optional[str] = typer.Option(None, "--symbol"),
    alert_id: Optional[int] = typer.Option(None, "--alert-id", min=1),
    limit: int = typer.Option(50, "--limit", min=1, max=500),
):
    event_type_v = None
    if event_type is not None:
        event_type_v = _normalize_enum(event_type, "--type", _EVENT_TYPES)
    symbol_v = symbol.strip() if symbol and symbol.strip() else None

    where: List[str] = []
    params: List[Any] = []
    if event_type_v is not None:
        where.append("event_type = ?")
        params.append(event_type_v)
    if symbol_v is not None:
        where.append("symbol = ?")
        params.append(symbol_v)
    if alert_id is not None:
        where.append("alert_id = ?")
        params.append(int(alert_id))

    sql = """
        SELECT id, created_at, event_type, title, description, symbol, snapshot_date, alert_id, payload_json
        FROM advisor_events
    """
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY id DESC LIMIT ?"
    params.append(int(limit))

    db_path = resolve_db_path(ctx.obj.config.db_path)
    conn = connect(db_path)
    init_db(conn)
    try:
        rows = conn.execute(sql, tuple(params)).fetchall()
        _print_json([dict(r) for r in rows])
    finally:
        conn.close()


@advisor_evidence_app.command("add")
def advisor_evidence_add(
    ctx: typer.Context,
    symbol: str = typer.Option(..., "--symbol"),
    query: str = typer.Option(..., "--query"),
    source_name: str = typer.Option(..., "--source-name"),
    source_url: str = typer.Option(..., "--source-url"),
    claim: str = typer.Option(..., "--claim"),
    confidence: str = typer.Option(..., "--confidence", help="low|medium|high"),
    date_confidence: str = typer.Option(..., "--date-confidence", help="low|medium|high"),
    published_date: Optional[str] = typer.Option(None, "--published-date", help="Optional YYYY-MM-DD"),
    notes: Optional[str] = typer.Option(None, "--notes"),
    conflict_key: Optional[str] = typer.Option(None, "--conflict-key"),
):
    sym = symbol.strip().upper()
    query_v = query.strip()
    source_name_v = source_name.strip()
    source_url_v = source_url.strip()
    claim_v = claim.strip()
    if not sym:
        raise typer.BadParameter("--symbol is required")
    if not query_v:
        raise typer.BadParameter("--query is required")
    if not source_name_v:
        raise typer.BadParameter("--source-name is required")
    if not source_url_v:
        raise typer.BadParameter("--source-url is required")
    if not claim_v:
        raise typer.BadParameter("--claim is required")
    conf_v = _normalize_enum(confidence, "--confidence", _CONFIDENCE_LEVELS)
    date_conf_v = _normalize_enum(date_confidence, "--date-confidence", _CONFIDENCE_LEVELS)
    pub_v = _parse_iso_date_optional(published_date, "--published-date")
    notes_v = notes.strip() if notes and notes.strip() else None
    conflict_v = conflict_key.strip() if conflict_key and conflict_key.strip() else None
    now = _utc_now_iso()

    db_path = resolve_db_path(ctx.obj.config.db_path)
    conn = connect(db_path)
    init_db(conn)
    try:
        cur = conn.execute(
            """
            INSERT INTO advisor_evidence (
                created_at, symbol, query, source_name, source_url, published_date, retrieved_at_utc,
                claim, confidence, date_confidence, notes, conflict_key
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                now,
                sym,
                query_v,
                source_name_v,
                source_url_v,
                pub_v,
                now,
                claim_v,
                conf_v,
                date_conf_v,
                notes_v,
                conflict_v,
            ),
        )
        conn.commit()
        _print_json(
            {
                "id": cur.lastrowid,
                "symbol": sym,
                "confidence": conf_v,
                "date_confidence": date_conf_v,
                "retrieved_at_utc": now,
            }
        )
    finally:
        conn.close()


@advisor_evidence_app.command("list")
def advisor_evidence_list(
    ctx: typer.Context,
    symbol: Optional[str] = typer.Option(None, "--symbol"),
    days: int = typer.Option(60, "--days", min=1, max=3650),
    limit: int = typer.Option(200, "--limit", min=1, max=2000),
):
    sym = symbol.strip().upper() if symbol and symbol.strip() else None
    cutoff = (date.today() - timedelta(days=int(days))).isoformat() + "T00:00:00Z"
    where = ["retrieved_at_utc >= ?"]
    params: List[Any] = [cutoff]
    if sym is not None:
        where.append("symbol = ?")
        params.append(sym)
    params.append(int(limit))

    db_path = resolve_db_path(ctx.obj.config.db_path)
    conn = connect(db_path)
    init_db(conn)
    try:
        rows = conn.execute(
            f"""
            SELECT id, created_at, symbol, query, source_name, source_url, published_date, retrieved_at_utc,
                   claim, confidence, date_confidence, notes, conflict_key
            FROM advisor_evidence
            WHERE {" AND ".join(where)}
            ORDER BY retrieved_at_utc DESC, id DESC
            LIMIT ?
            """,
            tuple(params),
        ).fetchall()
        _print_json([dict(r) for r in rows])
    finally:
        conn.close()


@advisor_evidence_app.command("fetch")
def advisor_evidence_fetch(
    ctx: typer.Context,
    symbols: Optional[str] = typer.Option(None, "--symbols", help="Comma-separated symbols"),
    from_context: bool = typer.Option(True, "--from-context/--no-from-context"),
    as_of: Optional[str] = typer.Option(None, "--as-of", help="Optional YYYY-MM-DD"),
    per_source_limit: int = typer.Option(2, "--per-source-limit", min=1, max=10),
    max_symbols: int = typer.Option(15, "--max-symbols", min=1, max=200),
    include_news: bool = typer.Option(True, "--news/--no-news"),
    include_sec: bool = typer.Option(True, "--sec/--no-sec"),
    timeout_sec: int = typer.Option(10, "--timeout-sec", min=1, max=60),
):
    db_path = resolve_db_path(ctx.obj.config.db_path)
    conn = connect(db_path)
    init_db(conn)
    try:
        latest_snap = _latest_snapshot_date(conn)
    finally:
        conn.close()
    as_of_v = parse_iso_date(as_of, default=latest_snap or date.today().isoformat())

    picked: List[str] = []
    if symbols and symbols.strip():
        for raw in symbols.split(","):
            s = raw.strip().upper()
            if s and s not in picked:
                picked.append(s)

    if from_context:
        ctx_payload = build_advisor_context_from_db_path(db_path=db_path, as_of=as_of_v, limit=200, history_days=3650)
        holdings_map = _load_holdings_map_from_context(ctx_payload)
        for s in sorted(holdings_map.keys()):
            if s not in picked:
                picked.append(s)

    picked = picked[: int(max_symbols)]
    if not picked:
        _print_json({"as_of": as_of_v, "symbols": [], "inserted": 0, "errors": []})
        return

    all_rows: List[Dict[str, Any]] = []
    errors: List[Dict[str, Any]] = []
    for sym in picked:
        rows, errs = collect_symbol_evidence(
            symbol=sym,
            per_source_limit=int(per_source_limit),
            include_news=bool(include_news),
            include_sec=bool(include_sec),
            timeout_sec=int(timeout_sec),
        )
        all_rows.extend(rows)
        for e in errs:
            errors.append({"symbol": sym, "error": e})

    conn = connect(db_path)
    init_db(conn)
    try:
        inserted = _store_evidence_rows(conn, all_rows)
        conn.commit()
    finally:
        conn.close()

    _print_json(
        {
            "as_of": as_of_v,
            "symbols": picked,
            "inserted": inserted,
            "fetched_rows": len(all_rows),
            "errors": errors,
        }
    )


@advisor_opp_app.command("snapshot-universe")
def advisor_opportunities_snapshot_universe(
    ctx: typer.Context,
    as_of: Optional[str] = typer.Option(None, "--as-of", help="Optional YYYY-MM-DD"),
    universe: str = typer.Option("bcba_cedears", "--universe", help="bcba_cedears"),
):
    universe_v = _normalize_enum(universe, "--universe", _OPP_UNIVERSES)
    db_path = resolve_db_path(ctx.obj.config.db_path)
    conn = connect(db_path)
    init_db(conn)
    try:
        latest_snap = _latest_snapshot_date(conn)
    finally:
        conn.close()
    as_of_v = parse_iso_date(as_of, default=latest_snap or date.today().isoformat())

    ctx_payload = build_advisor_context_from_db_path(db_path=db_path, as_of=as_of_v, limit=200, history_days=3650)
    holdings_map = _load_holdings_map_from_context(ctx_payload)
    symbols = set(holdings_map.keys())

    client = _get_client(ctx.obj)
    panel_data: List[Dict[str, Any]] = []
    if universe_v == "bcba_cedears":
        try:
            panel_payload = client.get_panel_quotes("Acciones", "CEDEARs", normalize_country("argentina"))
            panel_data = panel_rows(panel_payload)
        except Exception:
            panel_data = []

    rows_to_upsert: List[Dict[str, Any]] = []
    for r in panel_data:
        pr = snapshot_row_from_panel(as_of_v, r, market="bcba")
        if pr is None:
            continue
        rows_to_upsert.append(pr)
        symbols.add(str(pr["symbol"]))

    quote_errors: List[Dict[str, Any]] = []
    for sym in sorted(symbols):
        try:
            quote = client.get_quote(normalize_market("bcba"), sym)
            rows_to_upsert.append(snapshot_row_from_quote(as_of_v, sym, quote, market="bcba"))
        except Exception as exc:
            quote_errors.append({"symbol": sym, "error": str(exc)})

    conn = connect(db_path)
    init_db(conn)
    try:
        for r in rows_to_upsert:
            conn.execute(
                """
                INSERT INTO market_symbol_snapshots (
                    snapshot_date, symbol, market, last_price, bid, ask, spread_pct,
                    daily_var_pct, operations_count, volume_amount, source
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(snapshot_date, symbol, source) DO UPDATE SET
                    market=excluded.market,
                    last_price=excluded.last_price,
                    bid=excluded.bid,
                    ask=excluded.ask,
                    spread_pct=excluded.spread_pct,
                    daily_var_pct=excluded.daily_var_pct,
                    operations_count=excluded.operations_count,
                    volume_amount=excluded.volume_amount
                """,
                (
                    r["snapshot_date"],
                    r["symbol"],
                    r["market"],
                    r["last_price"],
                    r["bid"],
                    r["ask"],
                    r["spread_pct"],
                    r["daily_var_pct"],
                    r["operations_count"],
                    r["volume_amount"],
                    r["source"],
                ),
            )
        conn.commit()
    finally:
        conn.close()

    _print_json(
        {
            "as_of": as_of_v,
            "universe": universe_v,
            "rows_upserted": len(rows_to_upsert),
            "symbols_considered": len(symbols),
            "panel_rows": len(panel_data),
            "quote_errors": quote_errors,
        }
    )


@advisor_opp_app.command("run")
def advisor_opportunities_run(
    ctx: typer.Context,
    budget_ars: float = typer.Option(..., "--budget-ars"),
    mode: str = typer.Option("both", "--mode", help="new|rebuy|both"),
    as_of: Optional[str] = typer.Option(None, "--as-of", help="Optional YYYY-MM-DD"),
    top: int = typer.Option(10, "--top", min=1, max=100),
    universe: str = typer.Option("bcba_cedears", "--universe", help="bcba_cedears"),
    fetch_evidence: bool = typer.Option(True, "--fetch-evidence/--no-fetch-evidence"),
    evidence_max_symbols: int = typer.Option(15, "--evidence-max-symbols", min=1, max=200),
    evidence_per_source_limit: int = typer.Option(2, "--evidence-per-source-limit", min=1, max=10),
    evidence_news: bool = typer.Option(True, "--evidence-news/--no-evidence-news"),
    evidence_sec: bool = typer.Option(True, "--evidence-sec/--no-evidence-sec"),
    evidence_timeout_sec: int = typer.Option(10, "--evidence-timeout-sec", min=1, max=60),
):
    if float(budget_ars) <= 0:
        raise typer.BadParameter("--budget-ars must be > 0")
    mode_v = _normalize_enum(mode, "--mode", _OPP_MODES)
    universe_v = _normalize_enum(universe, "--universe", _OPP_UNIVERSES)

    db_path = resolve_db_path(ctx.obj.config.db_path)
    conn = connect(db_path)
    init_db(conn)
    try:
        latest_snap = _latest_snapshot_date(conn)
    finally:
        conn.close()
    as_of_v = parse_iso_date(as_of, default=latest_snap or date.today().isoformat())

    cfg = {
        "weights": {"risk": 0.35, "value": 0.20, "momentum": 0.35, "catalyst": 0.10},
        "thresholds": {
            "spread_pct_max": 2.5,
            "concentration_pct_max": 15.0,
            "new_asset_initial_cap_pct": 8.0,
            "drawdown_exclusion_pct": -25.0,
            "rebuy_dip_threshold_pct": -8.0,
        },
    }

    now = _utc_now_iso()
    run_id = None
    conn = connect(db_path)
    init_db(conn)
    try:
        cur = conn.execute(
            """
            INSERT INTO advisor_opportunity_runs (
                created_at_utc, as_of, mode, universe, budget_ars, top_n, status, error_message, config_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (now, as_of_v, mode_v, universe_v, float(budget_ars), int(top), "running", None, json.dumps(cfg, ensure_ascii=True)),
        )
        run_id = int(cur.lastrowid)
        conn.commit()
    finally:
        conn.close()

    try:
        ctx_payload = build_advisor_context_from_db_path(db_path=db_path, as_of=as_of_v, limit=500, history_days=3650)
        portfolio_total = float(((ctx_payload or {}).get("snapshot") or {}).get("total_value_ars") or 0.0)
        holdings_map = _load_holdings_map_from_context(ctx_payload)

        conn = connect(db_path)
        init_db(conn)
        try:
            market_rows = _load_market_snapshot_rows(conn, as_of_v)
            evidence_map = _load_evidence_rows_grouped(conn, as_of_v, lookback_days=60)
        finally:
            conn.close()

        latest_metrics = latest_metrics_by_symbol(market_rows, as_of_v)
        if not latest_metrics:
            raise RuntimeError("NO_MARKET_SNAPSHOTS: run 'iol advisor opportunities snapshot-universe' first")

        evidence_fetch_summary: Dict[str, Any] = {
            "enabled": bool(fetch_evidence),
            "symbols": [],
            "fetched_rows": 0,
            "inserted": 0,
            "errors": [],
        }
        if fetch_evidence:
            auto_symbols = _pick_symbols_for_auto_evidence(holdings_map, latest_metrics, max_symbols=int(evidence_max_symbols))
            evidence_fetch_summary["symbols"] = auto_symbols
            collected: List[Dict[str, Any]] = []
            fetch_errors: List[Dict[str, Any]] = []
            for sym in auto_symbols:
                rows, errs = collect_symbol_evidence(
                    symbol=sym,
                    per_source_limit=int(evidence_per_source_limit),
                    include_news=bool(evidence_news),
                    include_sec=bool(evidence_sec),
                    timeout_sec=int(evidence_timeout_sec),
                )
                collected.extend(rows)
                for e in errs:
                    fetch_errors.append({"symbol": sym, "error": e})

            conn = connect(db_path)
            init_db(conn)
            try:
                inserted_rows = _store_evidence_rows(conn, collected)
                conn.commit()
            finally:
                conn.close()
            evidence_fetch_summary["fetched_rows"] = len(collected)
            evidence_fetch_summary["inserted"] = int(inserted_rows)
            evidence_fetch_summary["errors"] = fetch_errors

            conn = connect(db_path)
            init_db(conn)
            try:
                evidence_map = _load_evidence_rows_grouped(conn, as_of_v, lookback_days=60)
            finally:
                conn.close()

        series_by_symbol = price_series_by_symbol(market_rows, as_of_v)
        candidates = build_candidates(
            as_of=as_of_v,
            mode=mode_v,
            budget_ars=float(budget_ars),
            top_n=int(top),
            portfolio_total_ars=portfolio_total,
            holdings_value_by_symbol=holdings_map,
            latest_metrics=latest_metrics,
            series_by_symbol=series_by_symbol,
            evidence_by_symbol=evidence_map,
        )

        conn = connect(db_path)
        init_db(conn)
        try:
            conn.execute("DELETE FROM advisor_opportunity_candidates WHERE run_id = ?", (int(run_id),))
            for c in candidates:
                d = c.to_dict()
                conn.execute(
                    """
                    INSERT INTO advisor_opportunity_candidates (
                        run_id, symbol, candidate_type, score_total, score_risk, score_value, score_momentum,
                        score_catalyst, entry_low, entry_high, suggested_weight_pct, suggested_amount_ars,
                        reason_summary, risk_flags_json, filters_passed
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        int(run_id),
                        d["symbol"],
                        d["candidate_type"],
                        float(d["score_total"]),
                        float(d["score_risk"]),
                        float(d["score_value"]),
                        float(d["score_momentum"]),
                        float(d["score_catalyst"]),
                        d["entry_low"],
                        d["entry_high"],
                        d["suggested_weight_pct"],
                        d["suggested_amount_ars"],
                        d["reason_summary"],
                        d["risk_flags_json"],
                        int(d["filters_passed"]),
                    ),
                )
            conn.execute(
                "UPDATE advisor_opportunity_runs SET status='ok', error_message=NULL WHERE id = ?",
                (int(run_id),),
            )
            conn.commit()
        finally:
            conn.close()

        top_rows = [c.to_dict() for c in candidates if int(c.filters_passed) == 1][: int(top)]
        _print_json(
            {
                "run_id": int(run_id),
                "as_of": as_of_v,
                "mode": mode_v,
                "universe": universe_v,
                "budget_ars": float(budget_ars),
                "top_n": int(top),
                "evidence_fetch": evidence_fetch_summary,
                "candidates_total": len(candidates),
                "top_operable": top_rows,
            }
        )
    except Exception as exc:
        conn = connect(db_path)
        init_db(conn)
        try:
            conn.execute(
                "UPDATE advisor_opportunity_runs SET status='error', error_message=? WHERE id = ?",
                (str(exc), int(run_id)),
            )
            conn.commit()
        finally:
            conn.close()
        console.print(f"Error: {exc}")
        raise typer.Exit(code=1)


@advisor_opp_app.command("report")
def advisor_opportunities_report(
    ctx: typer.Context,
    run_id: int = typer.Option(..., "--run-id", min=1),
    out: Optional[str] = typer.Option(None, "--out", help="Optional markdown output file"),
):
    db_path = resolve_db_path(ctx.obj.config.db_path)
    conn = connect(db_path)
    init_db(conn)
    try:
        run = conn.execute(
            """
            SELECT id, created_at_utc, as_of, mode, universe, budget_ars, top_n, status, error_message
            FROM advisor_opportunity_runs
            WHERE id = ?
            """,
            (int(run_id),),
        ).fetchone()
        if not run:
            console.print("Run ID not found.")
            raise typer.Exit(code=1)
        rows = conn.execute(
            """
            SELECT symbol, candidate_type, score_total, score_risk, score_value, score_momentum, score_catalyst,
                   entry_low, entry_high, suggested_weight_pct, suggested_amount_ars, reason_summary, risk_flags_json,
                   filters_passed
            FROM advisor_opportunity_candidates
            WHERE run_id = ?
            ORDER BY score_total DESC, symbol ASC
            """,
            (int(run_id),),
        ).fetchall()
    finally:
        conn.close()

    md = report_markdown(dict(run), [dict(r) for r in rows])
    if out:
        with open(out, "w", encoding="utf-8") as f:
            f.write(md)
        _print_json({"run_id": int(run_id), "out": out})
        return
    console.print(md)


@advisor_opp_app.command("list-runs")
def advisor_opportunities_list_runs(
    ctx: typer.Context,
    limit: int = typer.Option(20, "--limit", min=1, max=500),
    status: Optional[str] = typer.Option(None, "--status", help="ok|error|running"),
):
    status_v = status.strip().lower() if status and status.strip() else None
    if status_v is not None and status_v not in ("ok", "error", "running"):
        raise typer.BadParameter("--status must be ok|error|running")

    db_path = resolve_db_path(ctx.obj.config.db_path)
    conn = connect(db_path)
    init_db(conn)
    try:
        if status_v is None:
            rows = conn.execute(
                """
                SELECT id, created_at_utc, as_of, mode, universe, budget_ars, top_n, status, error_message
                FROM advisor_opportunity_runs
                ORDER BY id DESC
                LIMIT ?
                """,
                (int(limit),),
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT id, created_at_utc, as_of, mode, universe, budget_ars, top_n, status, error_message
                FROM advisor_opportunity_runs
                WHERE status = ?
                ORDER BY id DESC
                LIMIT ?
                """,
                (status_v, int(limit)),
            ).fetchall()
        _print_json([dict(r) for r in rows])
    finally:
        conn.close()


@advisor_app.command("seguimiento")
def advisor_seguimiento(
    ctx: typer.Context,
    out: Optional[str] = typer.Option(None, "--out", help="Write markdown to file instead of stdout"),
    alerts_limit: int = typer.Option(20, "--alerts-limit", min=1, max=200),
):
    db_path = resolve_db_path(ctx.obj.config.db_path)
    conn = connect(db_path)
    init_db(conn)
    try:
        latest_log = conn.execute(
            """
            SELECT id, created_at, snapshot_date, env, base_url, prompt, response
            FROM advisor_logs
            ORDER BY id DESC
            LIMIT 1
            """
        ).fetchone()
        alerts = conn.execute(
            """
            SELECT id, severity, alert_type, title, description, symbol, snapshot_date, due_date, created_at
            FROM advisor_alerts
            WHERE status = 'open'
            ORDER BY CASE severity WHEN 'high' THEN 3 WHEN 'medium' THEN 2 ELSE 1 END DESC, due_date ASC, id DESC
            LIMIT ?
            """,
            (int(alerts_limit),),
        ).fetchall()
    finally:
        conn.close()

    now = _utc_now_iso()
    lines: List[str] = []
    lines.append("# Memoria del Asesor (Ultima Conversacion)")
    lines.append("")
    lines.append("Este archivo es un resumen operativo.")
    lines.append("Fuente de verdad: SQLite (`advisor_logs`, `advisor_alerts`, `advisor_events`).")
    lines.append("")
    lines.append("## Metadata")
    lines.append(f"- `generated_at_utc`: {now}")
    lines.append(f"- `advisor_log_id`: {latest_log['id'] if latest_log else '-'}")
    lines.append(f"- `context_snapshot_date`: {latest_log['snapshot_date'] if latest_log and latest_log['snapshot_date'] else '-'}")
    lines.append(f"- `env`: {latest_log['env'] if latest_log and latest_log['env'] else '-'}")
    lines.append(f"- `base_url`: {latest_log['base_url'] if latest_log and latest_log['base_url'] else '-'}")
    lines.append("")
    lines.append("## Resumen (5 lineas max)")
    if latest_log and latest_log["response"]:
        raw_lines = [str(x).strip() for x in str(latest_log["response"]).splitlines() if str(x).strip()]
        for r in raw_lines[:5]:
            lines.append(f"- {r}")
    else:
        lines.append("- Sin registro reciente en `advisor_logs`.")
    lines.append("")
    lines.append("## Alertas/Triggers (fuente: advisor_alerts status=open)")
    if alerts:
        for a in alerts:
            symbol = f" symbol={a['symbol']}" if a["symbol"] else ""
            due = f" due={a['due_date']}" if a["due_date"] else ""
            lines.append(
                f"- [#{a['id']}] [{a['severity']}] {a['alert_type']} | {a['title']}{symbol}{due}"
            )
    else:
        lines.append("- Sin alertas abiertas.")
    text = "\n".join(lines) + "\n"

    if out:
        with open(out, "w", encoding="utf-8") as f:
            f.write(text)
        _print_json({"out": out, "generated_at_utc": now, "open_alerts": len(alerts)})
        return
    console.print(text)


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
        "advisor_alerts",
        "advisor_events",
        "advisor_evidence",
        "market_symbol_snapshots",
        "advisor_opportunity_runs",
        "advisor_opportunity_candidates",
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
