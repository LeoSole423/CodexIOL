import json
from datetime import date, datetime, timezone
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import typer
from rich.console import Console

from .config import ConfigError, load_config
from .db import connect, init_db, resolve_db_path
from .commands_advisor_admin import register_advisor_admin_commands
from .commands_advisor_autopilot import register_advisor_autopilot_commands
from .commands_advisor_evidence import register_advisor_evidence_commands
from .commands_advisor_opportunities import register_advisor_opportunity_commands
from .commands_cashflow_reconcile import build_cashflow_app, build_reconcile_app
from .commands_movements import build_movements_app
from .commands_engines import build_engines_app
from .commands_simulate import build_simulate_app
from .commands_snapshot_batch_data import build_batch_app, build_data_app, build_snapshot_app
from .iol_client import IOLClient, IOLAPIError
from .storage import add_pending, get_pending, remove_pending
from .advisor_opportunity_pipeline import (
    run_opportunity_pipeline_impl as _run_opportunity_pipeline_impl_core,
    snapshot_universe_impl as _snapshot_universe_impl_core,
)
from .advisor_opportunity_support import (
    CONFLICT_MODES as _CONFLICT_MODES,
    CONFIDENCE_LEVELS as _CONFIDENCE_LEVELS,
    OPP_MODES as _OPP_MODES,
    OPP_UNIVERSES as _OPP_UNIVERSES,
    SOURCE_POLICIES as _SOURCE_POLICIES,
    VARIANT_SELECTORS as _VARIANT_SELECTORS,
    latest_snapshot_date as _latest_snapshot_date,
    load_evidence_rows_grouped as _load_evidence_rows_grouped,
    load_holdings_context_from_db as _load_holdings_context_from_db,
    load_holdings_map_from_context as _load_holdings_map_from_context,
    load_market_snapshot_rows as _load_market_snapshot_rows,
    normalize_enum as _normalize_enum,
    pick_symbols_for_web_link as _pick_symbols_for_web_link,
    store_evidence_rows as _store_evidence_rows_raw,
)
from .evidence_fetch import collect_symbol_evidence
from .opportunities import parse_iso_date
from .util import (
    default_valid_until,
    normalize_country,
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


snapshot_app = build_snapshot_app(get_client=_get_client, parse_date=_parse_date, print_json=_print_json)
app.add_typer(snapshot_app, name="snapshot")

cashflow_app = build_cashflow_app()
app.add_typer(cashflow_app, name="cashflow")
reconcile_app = build_reconcile_app()
app.add_typer(reconcile_app, name="reconcile")
movements_app = build_movements_app(get_client=_get_client)
app.add_typer(movements_app, name="movements")


data_app = build_data_app(print_json=_print_json)
app.add_typer(data_app, name="data")


batch_app = build_batch_app(get_client=_get_client, print_json=_print_json)
app.add_typer(batch_app, name="batch")

engines_app = build_engines_app(print_json=_print_json)
app.add_typer(engines_app, name="engines")

simulate_app = build_simulate_app(print_json=_print_json)
app.add_typer(simulate_app, name="simulate")


advisor_app = typer.Typer(help="Advisor utilities")
app.add_typer(advisor_app, name="advisor")
advisor_alert_app = typer.Typer(help="Manual advisor alerts")
advisor_event_app = typer.Typer(help="Manual advisor events")
advisor_evidence_app = typer.Typer(help="Web evidence for symbols")
advisor_opp_app = typer.Typer(help="Opportunity pipeline (ranking)")
advisor_opp_variants_app = typer.Typer(help="Opportunity model variants")
advisor_briefing_app = typer.Typer(help="Advisor briefings and autopilot runs")
advisor_autopilot_app = typer.Typer(help="Automated advisor orchestration")
advisor_app.add_typer(advisor_alert_app, name="alert")
advisor_app.add_typer(advisor_event_app, name="event")
advisor_app.add_typer(advisor_evidence_app, name="evidence")
advisor_app.add_typer(advisor_opp_app, name="opportunities")
advisor_opp_app.add_typer(advisor_opp_variants_app, name="variants")
advisor_app.add_typer(advisor_briefing_app, name="briefing")
advisor_app.add_typer(advisor_autopilot_app, name="autopilot")
advisor_target_app = typer.Typer(help="Structural target weights for conflict resolution")
advisor_app.add_typer(advisor_target_app, name="target-weights")
register_advisor_admin_commands(advisor_app, advisor_alert_app, advisor_event_app, advisor_briefing_app)


@advisor_target_app.command("set")
def advisor_target_weights_set(
    ctx: typer.Context,
    weights: List[str] = typer.Argument(
        ...,
        help="Symbol=pct pairs, e.g. SPY=32 ACWI=17 GLD=16 EEM=8 TO26=7 AL30=6 IBIT=6 ADRDOLA=8",
    ),
    as_of: Optional[str] = typer.Option(None, "--as-of", help="Effective date (default: today)"),
):
    """Set structural target weights (used by resolve_conflicts to avoid spurious sell signals)."""
    from .db import connect, init_db, resolve_db_path
    from datetime import date as _date
    parsed: Dict[str, float] = {}
    for item in weights:
        if "=" not in item:
            typer.echo(f"Invalid format '{item}', expected SYMBOL=PCT", err=True)
            raise typer.Exit(1)
        sym, pct_s = item.split("=", 1)
        try:
            parsed[sym.strip().upper()] = float(pct_s.strip())
        except ValueError:
            typer.echo(f"Invalid pct '{pct_s}' for {sym}", err=True)
            raise typer.Exit(1)
    total = sum(parsed.values())
    if abs(total - 100.0) > 1.0:
        typer.echo(f"Warning: weights sum to {total:.1f}% (expected ~100%)", err=True)
    as_of_v = as_of or _date.today().isoformat()
    db_path = resolve_db_path(ctx.obj.config.db_path)
    conn = connect(db_path)
    init_db(conn)
    try:
        for sym, pct in parsed.items():
            conn.execute(
                "INSERT INTO portfolio_target_weights (symbol, target_pct, as_of) VALUES (?, ?, ?) "
                "ON CONFLICT(symbol) DO UPDATE SET target_pct=excluded.target_pct, as_of=excluded.as_of",
                (sym, pct, as_of_v),
            )
        conn.commit()
    finally:
        conn.close()
    from rich.console import Console
    from rich.table import Table
    console = Console()
    table = Table(title=f"Target weights (as_of={as_of_v})")
    table.add_column("Symbol"); table.add_column("Target %", justify="right")
    for sym, pct in sorted(parsed.items()):
        table.add_row(sym, f"{pct:.1f}%")
    table.add_row("TOTAL", f"{total:.1f}%")
    console.print(table)


@advisor_target_app.command("show")
def advisor_target_weights_show(ctx: typer.Context):
    """Show current structural target weights."""
    from .db import connect, init_db, resolve_db_path
    db_path = resolve_db_path(ctx.obj.config.db_path)
    conn = connect(db_path)
    init_db(conn)
    try:
        rows = conn.execute(
            "SELECT symbol, target_pct, as_of FROM portfolio_target_weights ORDER BY target_pct DESC"
        ).fetchall()
    finally:
        conn.close()
    if not rows:
        typer.echo("No target weights set. Use: iol advisor target-weights set SPY=32 ...")
        return
    from rich.console import Console
    from rich.table import Table
    console = Console()
    table = Table(title="Structural target weights")
    table.add_column("Symbol"); table.add_column("Target %", justify="right"); table.add_column("as_of")
    total = 0.0
    for r in rows:
        table.add_row(str(r["symbol"]), f"{r['target_pct']:.1f}%", str(r["as_of"]))
        total += float(r["target_pct"])
    table.add_row("TOTAL", f"{total:.1f}%", "")
    console.print(table)

def _store_evidence_rows(conn, rows: List[Dict[str, Any]]) -> int:
    return _store_evidence_rows_raw(
        conn,
        rows,
        confidence_levels=_CONFIDENCE_LEVELS,
        utc_now_iso=_utc_now_iso,
    )


register_advisor_evidence_commands(
    advisor_evidence_app,
    print_json=_print_json,
    normalize_enum=_normalize_enum,
    confidence_levels=_CONFIDENCE_LEVELS,
    source_policies=_SOURCE_POLICIES,
    parse_iso_date_optional=_parse_iso_date_optional,
    utc_now_iso=_utc_now_iso,
    latest_snapshot_date=_latest_snapshot_date,
    load_holdings_map_from_context=_load_holdings_map_from_context,
    store_evidence_rows=_store_evidence_rows,
    collect_symbol_evidence_fn=lambda **kwargs: collect_symbol_evidence(**kwargs),
)


def _snapshot_universe_impl(
    cli_ctx: CLIContext,
    *,
    as_of: Optional[str],
    universe: str,
) -> Dict[str, Any]:
    return _snapshot_universe_impl_core(
        cli_ctx,
        as_of=as_of,
        universe=universe,
        get_client_fn=lambda ctx: _get_client(ctx),
    )


def _run_opportunity_pipeline_impl(
    cli_ctx: CLIContext,
    *,
    budget_ars: float,
    mode: str,
    as_of: Optional[str],
    top: int,
    universe: str,
    fetch_evidence: bool,
    evidence_max_symbols: int,
    evidence_per_source_limit: int,
    evidence_news: bool,
    evidence_sec: bool,
    evidence_timeout_sec: int,
    web_link: bool,
    web_top_k: int,
    web_source_policy: str,
    web_lookback_days: int,
    web_min_trusted_refs: int,
    web_conflict_mode: str,
    web_reuters: bool,
    web_official: bool,
    exclude_crypto_new: bool,
    min_volume_amount: float,
    min_operations: int,
    liquidity_priority: bool,
    diversify_sectors: bool,
    max_per_sector: int,
    variant: str = "active",
    cadence: Optional[str] = None,
    reuse_existing: bool = False,
) -> Dict[str, Any]:
    return _run_opportunity_pipeline_impl_core(
        cli_ctx,
        budget_ars=budget_ars,
        mode=mode,
        as_of=as_of,
        top=top,
        universe=universe,
        fetch_evidence=fetch_evidence,
        evidence_max_symbols=evidence_max_symbols,
        evidence_per_source_limit=evidence_per_source_limit,
        evidence_news=evidence_news,
        evidence_sec=evidence_sec,
        evidence_timeout_sec=evidence_timeout_sec,
        web_link=web_link,
        web_top_k=web_top_k,
        web_source_policy=web_source_policy,
        web_lookback_days=web_lookback_days,
        web_min_trusted_refs=web_min_trusted_refs,
        web_conflict_mode=web_conflict_mode,
        web_reuters=web_reuters,
        web_official=web_official,
        exclude_crypto_new=exclude_crypto_new,
        min_volume_amount=min_volume_amount,
        min_operations=min_operations,
        liquidity_priority=liquidity_priority,
        diversify_sectors=diversify_sectors,
        max_per_sector=max_per_sector,
        variant=variant,
        cadence=cadence,
        reuse_existing=reuse_existing,
        utc_now_iso_fn=_utc_now_iso,
        collect_symbol_evidence_fn=lambda **kwargs: collect_symbol_evidence(**kwargs),
        store_evidence_rows_fn=_store_evidence_rows,
    )


register_advisor_opportunity_commands(
    advisor_opp_app,
    advisor_opp_variants_app,
    print_json=_print_json,
    console=console,
    run_opportunity_pipeline_impl=_run_opportunity_pipeline_impl,
    snapshot_universe_impl=_snapshot_universe_impl,
)


register_advisor_autopilot_commands(
    advisor_autopilot_app,
    print_json=_print_json,
    normalize_enum=_normalize_enum,
    source_policies=_SOURCE_POLICIES,
    snapshot_universe_impl=_snapshot_universe_impl,
    run_opportunity_pipeline_impl=_run_opportunity_pipeline_impl,
)

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
