import json
from datetime import date, datetime, timedelta, timezone
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import typer
from rich.console import Console

from .config import ConfigError, load_config
from .db import connect, init_db, resolve_db_path
from .commands_advisor_admin import register_advisor_admin_commands
from .commands_cashflow_reconcile import build_cashflow_app, build_reconcile_app
from .commands_snapshot_batch_data import build_batch_app, build_data_app, build_snapshot_app
from .iol_client import IOLClient, IOLAPIError
from .storage import add_pending, get_pending, remove_pending
from .advisor_context import build_advisor_context_from_db_path
from .evidence_fetch import collect_symbol_evidence
from iol_advisor.continuous import (
    DEFAULT_WINDOW_DAYS,
    active_variant,
    build_variant_scorecard,
    challenger_variant,
    compare_scorecards,
    ensure_default_model_variants,
    evaluate_signal_outcomes,
    insert_run_regression,
    list_model_variants,
    maybe_promote_challenger,
    resolve_variant_selection,
)
from iol_advisor.service import (
    DEFAULT_SOURCE_POLICY,
    build_unified_context,
    find_reusable_opportunity_run,
    load_latest_opportunity_payload,
    persist_briefing_bundle,
)
from .opportunities import (
    build_candidates,
    latest_metrics_by_symbol,
    panel_rows,
    parse_iso_date,
    price_series_by_symbol,
    report_markdown,
    snapshot_row_from_panel,
    snapshot_row_from_quote,
    summarize_run_metrics,
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


snapshot_app = build_snapshot_app(get_client=_get_client, parse_date=_parse_date, print_json=_print_json)
app.add_typer(snapshot_app, name="snapshot")

cashflow_app = build_cashflow_app()
app.add_typer(cashflow_app, name="cashflow")
reconcile_app = build_reconcile_app()
app.add_typer(reconcile_app, name="reconcile")


data_app = build_data_app(print_json=_print_json)
app.add_typer(data_app, name="data")


batch_app = build_batch_app(get_client=_get_client, print_json=_print_json)
app.add_typer(batch_app, name="batch")


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
register_advisor_admin_commands(advisor_app, advisor_alert_app, advisor_event_app, advisor_briefing_app)

_CONFIDENCE_LEVELS = {"low", "medium", "high"}
_OPP_MODES = {"new", "rebuy", "both"}
_OPP_UNIVERSES = {"bcba_cedears"}
_SOURCE_POLICIES = {"strict_official_reuters"}
_CONFLICT_MODES = {"manual_review"}
_VARIANT_SELECTORS = {"active", "challenger", "both"}

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


def _load_holdings_context_from_db(conn, as_of: str) -> Dict[str, Dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT snapshot_date, symbol, quantity, last_price, ppc, total_value, gain_pct, gain_amount
        FROM portfolio_assets
        WHERE snapshot_date = (
            SELECT MAX(snapshot_date) FROM portfolio_assets WHERE snapshot_date <= ?
        )
        """,
        (str(as_of),),
    ).fetchall()
    first_seen_rows = conn.execute(
        """
        SELECT symbol, MIN(snapshot_date) AS first_seen
        FROM portfolio_assets
        WHERE snapshot_date <= ?
          AND COALESCE(total_value, 0) > 0
        GROUP BY symbol
        """,
        (str(as_of),),
    ).fetchall()
    first_seen = {str(r["symbol"] or ""): str(r["first_seen"] or "") for r in first_seen_rows}
    out: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        symbol = str(row["symbol"] or "").strip().upper()
        if not symbol:
            continue
        age_days = 0
        try:
            first = first_seen.get(symbol)
            if first:
                age_days = max(0, (date.fromisoformat(str(as_of)) - date.fromisoformat(first)).days)
        except Exception:
            age_days = 0
        out[symbol] = {
            "quantity": float(row["quantity"] or 0.0),
            "last_price": float(row["last_price"] or 0.0),
            "ppc": float(row["ppc"] or 0.0) if row["ppc"] is not None else None,
            "total_value": float(row["total_value"] or 0.0),
            "gain_pct": float(row["gain_pct"] or 0.0) if row["gain_pct"] is not None else 0.0,
            "gain_amount": float(row["gain_amount"] or 0.0) if row["gain_amount"] is not None else 0.0,
            "age_days": int(age_days),
        }
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


def _pick_symbols_for_web_link(
    holdings_map: Dict[str, float],
    prelim_candidates: List[Dict[str, Any]],
    top_k: int,
) -> List[str]:
    chosen: List[str] = []
    for s in sorted(holdings_map.keys()):
        if s and s not in chosen:
            chosen.append(s)
    operable = [r for r in prelim_candidates if int(r.get("filters_passed") or 0) == 1]
    operable.sort(
        key=lambda r: (
            -float(r.get("score_total") or 0.0),
            -float(r.get("liquidity_score") or 0.0),
            -float(r.get("trusted_refs_count") or 0.0),
            str(r.get("symbol") or ""),
        )
    )
    for r in operable[: int(top_k)]:
        sym = str(r.get("symbol") or "").strip().upper()
        if sym and sym not in chosen:
            chosen.append(sym)
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
        notes_v = r.get("notes")
        if isinstance(notes_v, (dict, list)):
            notes_v = json.dumps(notes_v, ensure_ascii=True, sort_keys=True)
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
                notes_v,
                r.get("conflict_key"),
            ),
        )
        inserted += 1
    return inserted


def _snapshot_universe_impl(
    cli_ctx: CLIContext,
    *,
    as_of: Optional[str],
    universe: str,
) -> Dict[str, Any]:
    universe_v = _normalize_enum(universe, "--universe", _OPP_UNIVERSES)
    db_path = resolve_db_path(cli_ctx.config.db_path)
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

    client = _get_client(cli_ctx)
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

    return {
        "as_of": as_of_v,
        "universe": universe_v,
        "rows_upserted": len(rows_to_upsert),
        "symbols_considered": len(symbols),
        "panel_rows": len(panel_data),
        "quote_errors": quote_errors,
    }


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
    if float(budget_ars) <= 0:
        raise typer.BadParameter("--budget-ars must be > 0")
    mode_v = _normalize_enum(mode, "--mode", _OPP_MODES)
    universe_v = _normalize_enum(universe, "--universe", _OPP_UNIVERSES)
    web_source_policy_v = _normalize_enum(web_source_policy, "--web-source-policy", _SOURCE_POLICIES)
    web_conflict_mode_v = _normalize_enum(web_conflict_mode, "--web-conflict-mode", _CONFLICT_MODES)

    db_path = resolve_db_path(cli_ctx.config.db_path)
    conn = connect(db_path)
    init_db(conn)
    try:
        ensure_default_model_variants(conn)
        latest_snap = _latest_snapshot_date(conn)
        as_of_v = parse_iso_date(as_of, default=latest_snap or date.today().isoformat())
        if variant in _VARIANT_SELECTORS and variant == "both":
            selected = resolve_variant_selection(conn, "both")
            if not selected:
                raise typer.BadParameter("--variant both requires active/challenger variants")
            results: List[Dict[str, Any]] = []
            for row in selected:
                results.append(
                    _run_opportunity_pipeline_impl(
                        cli_ctx,
                        budget_ars=float(budget_ars),
                        mode=mode_v,
                        as_of=as_of_v,
                        top=int(top),
                        universe=universe_v,
                        fetch_evidence=bool(fetch_evidence),
                        evidence_max_symbols=int(evidence_max_symbols),
                        evidence_per_source_limit=int(evidence_per_source_limit),
                        evidence_news=bool(evidence_news),
                        evidence_sec=bool(evidence_sec),
                        evidence_timeout_sec=int(evidence_timeout_sec),
                        web_link=bool(web_link),
                        web_top_k=int(web_top_k),
                        web_source_policy=web_source_policy_v,
                        web_lookback_days=int(web_lookback_days),
                        web_min_trusted_refs=int(web_min_trusted_refs),
                        web_conflict_mode=web_conflict_mode_v,
                        web_reuters=bool(web_reuters),
                        web_official=bool(web_official),
                        exclude_crypto_new=bool(exclude_crypto_new),
                        min_volume_amount=float(min_volume_amount),
                        min_operations=int(min_operations),
                        liquidity_priority=bool(liquidity_priority),
                        diversify_sectors=bool(diversify_sectors),
                        max_per_sector=int(max_per_sector),
                        variant=str(row.id),
                        cadence=cadence,
                        reuse_existing=bool(reuse_existing),
                    )
                )
            current_active = active_variant(conn)
            active_row = None
            if current_active is not None:
                for res in results:
                    if int(res.get("variant_id") or 0) == int(current_active.id):
                        active_row = res
                        break
            if active_row is None:
                active_row = results[0]
            return {
                "variant": "both",
                "variant_runs": results,
                "active_variant_id": active_row.get("variant_id"),
                "run_id": active_row.get("run_id"),
                "as_of": as_of_v,
                "mode": mode_v,
                "universe": universe_v,
                "budget_ars": float(budget_ars),
                "top_n": int(top),
                "pipeline_warnings": list(active_row.get("pipeline_warnings") or []),
                "run_metrics": dict(active_row.get("run_metrics") or {}),
                "candidates_total": int(active_row.get("candidates_total") or 0),
                "top_operable": list(active_row.get("top_operable") or []),
                "watchlist": list(active_row.get("watchlist") or []),
                "manual_review": list(active_row.get("manual_review") or []),
                "reused": bool(all(bool(r.get("reused")) for r in results)),
            }
        selected = resolve_variant_selection(conn, variant)
        if len(selected) != 1:
            raise typer.BadParameter("--variant must be active|challenger|both or a valid variant id")
        variant_row = selected[0]
        variant_cfg = dict(variant_row.config or {})
        score_version = str(variant_cfg.get("score_version") or variant_row.name)
        if reuse_existing:
            existing = find_reusable_opportunity_run(
                conn,
                as_of=as_of_v,
                mode=mode_v,
                universe=universe_v,
                budget_ars=float(budget_ars),
                top_n=int(top),
                variant_id=int(variant_row.id),
            )
            if existing:
                return {
                    "run_id": int(existing["id"]),
                    "variant_id": int(variant_row.id),
                    "variant_name": variant_row.name,
                    "score_version": score_version,
                    "as_of": as_of_v,
                    "mode": mode_v,
                    "universe": universe_v,
                    "budget_ars": float(budget_ars),
                    "top_n": int(top),
                    "evidence_fetch": {
                        "enabled": False,
                        "symbols": [],
                        "fetched_rows": 0,
                        "inserted": 0,
                        "errors": [],
                        "source_policy": web_source_policy_v,
                    },
                    "pipeline_warnings": list(existing.get("pipeline_warnings") or []),
                    "run_metrics": dict(existing.get("run_metrics") or {}),
                    "candidates_total": len(existing.get("candidates") or []),
                    "top_operable": list(existing.get("top_operable") or []),
                    "watchlist": list(existing.get("watchlist") or []),
                    "manual_review": [
                        c for c in (existing.get("candidates") or [])
                        if str(c.get("candidate_status") or "").strip().lower() == "manual_review"
                    ][: int(top)],
                    "reused": True,
                }
    finally:
        conn.close()

    cfg = {
        "weights": dict(variant_cfg.get("weights") or {"risk": 0.35, "value": 0.20, "momentum": 0.35, "catalyst": 0.10}),
        "thresholds": {
            "spread_pct_max": 2.5,
            "concentration_pct_max": 15.0,
            "new_asset_initial_cap_pct": 8.0,
            "drawdown_exclusion_pct": -25.0,
            "rebuy_dip_threshold_pct": -8.0,
            "exclude_crypto_new": bool(exclude_crypto_new),
            "min_volume_amount": float(min_volume_amount),
            "min_operations": int(min_operations),
            "liquidity_priority": bool(liquidity_priority),
            "diversify_sectors": bool(diversify_sectors),
            "max_per_sector": int(max_per_sector) if bool(diversify_sectors) else 0,
            "trim_weight_pct": float(((variant_cfg.get("thresholds") or {}).get("trim_weight_pct") or 12.0)),
            "exit_weight_pct": float(((variant_cfg.get("thresholds") or {}).get("exit_weight_pct") or 15.0)),
            "sell_momentum_max": float(((variant_cfg.get("thresholds") or {}).get("sell_momentum_max") or 35.0)),
            "exit_momentum_max": float(((variant_cfg.get("thresholds") or {}).get("exit_momentum_max") or 20.0)),
            "sell_conflict_exit": bool((variant_cfg.get("thresholds") or {}).get("sell_conflict_exit", True)),
            "liquidity_floor": float(((variant_cfg.get("thresholds") or {}).get("liquidity_floor") or 40.0)),
        },
        "variant": variant_row.to_dict(),
        "web_link": {
            "enabled": bool(web_link),
            "top_k": int(web_top_k),
            "source_policy": web_source_policy_v,
            "lookback_days": int(web_lookback_days),
            "min_trusted_refs": int(web_min_trusted_refs),
            "conflict_mode": web_conflict_mode_v,
            "reuters": bool(web_reuters),
            "official": bool(web_official),
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
                created_at_utc, as_of, mode, universe, budget_ars, top_n, variant_id, score_version, status, error_message, config_json, pipeline_warnings_json, run_metrics_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                now,
                as_of_v,
                mode_v,
                universe_v,
                float(budget_ars),
                int(top),
                int(variant_row.id),
                score_version,
                "running",
                None,
                json.dumps(cfg, ensure_ascii=True),
                None,
                None,
            ),
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
            evidence_map = _load_evidence_rows_grouped(conn, as_of_v, lookback_days=int(web_lookback_days))
            holdings_context = _load_holdings_context_from_db(conn, as_of_v)
        finally:
            conn.close()

        latest_metrics = latest_metrics_by_symbol(market_rows, as_of_v)
        if not latest_metrics:
            raise RuntimeError("NO_MARKET_SNAPSHOTS: run 'iol advisor opportunities snapshot-universe' first")

        series_by_symbol = price_series_by_symbol(market_rows, as_of_v)
        prelim_candidates = build_candidates(
            as_of=as_of_v,
            mode=mode_v,
            budget_ars=float(budget_ars),
            top_n=int(top),
            portfolio_total_ars=portfolio_total,
            holdings_value_by_symbol=holdings_map,
            latest_metrics=latest_metrics,
            series_by_symbol=series_by_symbol,
            evidence_by_symbol=evidence_map,
            holdings_context_by_symbol=holdings_context,
            min_trusted_refs=0,
            apply_expert_overlay=False,
            conflict_mode=web_conflict_mode_v,
            exclude_crypto_new=bool(exclude_crypto_new),
            min_volume_amount=float(min_volume_amount),
            min_operations=int(min_operations),
            liquidity_priority=bool(liquidity_priority),
            max_per_sector=0,
            weights=dict(cfg.get("weights") or {}),
            thresholds=dict(cfg.get("thresholds") or {}),
            score_version=score_version,
        )

        pipeline_warnings: List[str] = []
        web_link_enabled = bool(web_link and fetch_evidence)
        evidence_fetch_summary: Dict[str, Any] = {
            "enabled": bool(web_link_enabled),
            "symbols": [],
            "fetched_rows": 0,
            "inserted": 0,
            "errors": [],
            "source_policy": web_source_policy_v,
        }
        if web_link_enabled:
            auto_symbols = _pick_symbols_for_web_link(
                holdings_map=holdings_map,
                prelim_candidates=[c.to_dict() for c in prelim_candidates],
                top_k=int(web_top_k),
            )
            auto_symbols = auto_symbols[: int(evidence_max_symbols)]
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
                    source_policy=web_source_policy_v,
                    include_reuters=bool(web_reuters),
                    include_official=bool(web_official),
                    run_stage="rerank",
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
            if fetch_errors:
                pipeline_warnings.append("WEB_FETCH_PARTIAL_ERRORS")

            conn = connect(db_path)
            init_db(conn)
            try:
                evidence_map = _load_evidence_rows_grouped(conn, as_of_v, lookback_days=int(web_lookback_days))
            finally:
                conn.close()

        has_recent_evidence = any(bool(v) for v in evidence_map.values())
        apply_web_overlay = bool(
            web_link_enabled
            and (int(evidence_fetch_summary.get("inserted") or 0) > 0 or has_recent_evidence)
        )
        min_refs_final = int(web_min_trusted_refs) if apply_web_overlay else 0
        if web_link_enabled and not apply_web_overlay:
            pipeline_warnings.append("WEB_FETCH_EMPTY_FALLBACK_TO_QUANT")

        rerank_symbols = set(_pick_symbols_for_web_link(
            holdings_map=holdings_map,
            prelim_candidates=[c.to_dict() for c in prelim_candidates],
            top_k=int(web_top_k),
        ))
        latest_metrics_final = {sym: row for sym, row in latest_metrics.items() if sym in rerank_symbols}
        series_by_symbol_final = {sym: row for sym, row in series_by_symbol.items() if sym in rerank_symbols}
        evidence_map_final = {sym: evidence_map.get(sym, []) for sym in rerank_symbols}

        final_candidates = build_candidates(
            as_of=as_of_v,
            mode=mode_v,
            budget_ars=float(budget_ars),
            top_n=int(top),
            portfolio_total_ars=portfolio_total,
            holdings_value_by_symbol=holdings_map,
            latest_metrics=latest_metrics_final,
            series_by_symbol=series_by_symbol_final,
            evidence_by_symbol=evidence_map_final,
            holdings_context_by_symbol=holdings_context,
            min_trusted_refs=min_refs_final,
            apply_expert_overlay=apply_web_overlay,
            conflict_mode=web_conflict_mode_v,
            exclude_crypto_new=bool(exclude_crypto_new),
            min_volume_amount=float(min_volume_amount),
            min_operations=int(min_operations),
            liquidity_priority=bool(liquidity_priority),
            max_per_sector=int(max_per_sector) if bool(diversify_sectors) else 0,
            weights=dict(cfg.get("weights") or {}),
            thresholds=dict(cfg.get("thresholds") or {}),
            score_version=score_version,
        )
        final_symbols = {str(c.symbol).strip().upper() for c in final_candidates}
        prelim_non_operable = [
            c
            for c in prelim_candidates
            if str(c.symbol).strip().upper() not in final_symbols and str(c.candidate_status) != "operable"
        ]
        candidates = list(final_candidates) + prelim_non_operable
        run_metrics = summarize_run_metrics(candidates)

        conn = connect(db_path)
        init_db(conn)
        try:
            conn.execute("DELETE FROM advisor_opportunity_candidates WHERE run_id = ?", (int(run_id),))
            for c in candidates:
                d = c.to_dict()
                conn.execute(
                    """
                    INSERT INTO advisor_opportunity_candidates (
                        run_id, symbol, candidate_type, signal_side, signal_family, score_version, score_total, score_risk, score_value, score_momentum,
                        score_catalyst, entry_low, entry_high, suggested_weight_pct, suggested_amount_ars,
                        reason_summary, risk_flags_json, filters_passed, expert_signal_score,
                        trusted_refs_count, consensus_state, decision_gate, candidate_status, evidence_summary_json, liquidity_score, sector_bucket, is_crypto_proxy,
                        holding_context_json, score_features_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        int(run_id),
                        d["symbol"],
                        d["candidate_type"],
                        d.get("signal_side"),
                        d.get("signal_family"),
                        d.get("score_version"),
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
                        float(d.get("expert_signal_score") or 0.0),
                        int(d.get("trusted_refs_count") or 0),
                        str(d.get("consensus_state") or "insufficient"),
                        str(d.get("decision_gate") or "auto"),
                        str(d.get("candidate_status") or "watchlist"),
                        str(d.get("evidence_summary_json") or "{}"),
                        float(d.get("liquidity_score") or 0.0),
                        str(d.get("sector_bucket") or "unknown"),
                        int(d.get("is_crypto_proxy") or 0),
                        str(d.get("holding_context_json") or "{}"),
                        str(d.get("score_features_json") or "{}"),
                    ),
                )
            warnings_json = json.dumps(pipeline_warnings, ensure_ascii=True) if pipeline_warnings else None
            conn.execute(
                "UPDATE advisor_opportunity_runs SET status='ok', error_message=NULL, pipeline_warnings_json=?, run_metrics_json=? WHERE id = ?",
                (warnings_json, json.dumps(run_metrics, ensure_ascii=True, sort_keys=True), int(run_id)),
            )
            conn.commit()
        finally:
            conn.close()

        operable_rows = [c.to_dict() for c in candidates if str(c.candidate_status) == "operable"][: int(top)]
        manual_rows = [
            c.to_dict()
            for c in candidates
            if str(c.candidate_status).strip().lower() == "manual_review"
        ][: int(top)]
        watchlist_rows = [
            c.to_dict()
            for c in candidates
            if str(c.candidate_status).strip().lower() == "watchlist"
        ][: int(top)]
        return {
            "run_id": int(run_id),
            "variant_id": int(variant_row.id),
            "variant_name": variant_row.name,
            "score_version": score_version,
            "as_of": as_of_v,
            "mode": mode_v,
            "universe": universe_v,
            "budget_ars": float(budget_ars),
            "top_n": int(top),
            "evidence_fetch": evidence_fetch_summary,
            "pipeline_warnings": pipeline_warnings,
            "run_metrics": run_metrics,
            "candidates_total": len(candidates),
            "top_operable": operable_rows,
            "watchlist": watchlist_rows,
            "manual_review": manual_rows,
            "reused": False,
        }
    except Exception as exc:
        conn = connect(db_path)
        init_db(conn)
        try:
            conn.execute(
                "UPDATE advisor_opportunity_runs SET status='error', error_message=?, pipeline_warnings_json=?, run_metrics_json=? WHERE id = ?",
                (str(exc), json.dumps(["RUN_ERROR"], ensure_ascii=True), None, int(run_id)),
            )
            conn.commit()
        finally:
            conn.close()
        raise


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
    from_top_run_id: Optional[int] = typer.Option(None, "--from-top-run-id", min=1, help="Use top candidates from a previous run"),
    as_of: Optional[str] = typer.Option(None, "--as-of", help="Optional YYYY-MM-DD"),
    per_source_limit: int = typer.Option(2, "--per-source-limit", min=1, max=10),
    max_symbols: int = typer.Option(15, "--max-symbols", min=1, max=200),
    include_news: bool = typer.Option(True, "--news/--no-news"),
    include_sec: bool = typer.Option(True, "--sec/--no-sec"),
    source_policy: str = typer.Option("strict_official_reuters", "--source-policy", help="strict_official_reuters"),
    include_reuters: bool = typer.Option(True, "--reuters/--no-reuters"),
    include_official: bool = typer.Option(True, "--official/--no-official"),
    run_stage: str = typer.Option("prelim", "--run-stage", help="prelim|rerank"),
    timeout_sec: int = typer.Option(10, "--timeout-sec", min=1, max=60),
):
    source_policy_v = _normalize_enum(source_policy, "--source-policy", _SOURCE_POLICIES)
    run_stage_v = (run_stage or "").strip().lower()
    if run_stage_v not in {"prelim", "rerank"}:
        raise typer.BadParameter("--run-stage must be prelim|rerank")
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
    if from_top_run_id is not None:
        conn = connect(db_path)
        init_db(conn)
        try:
            rows = conn.execute(
                """
                SELECT symbol
                FROM advisor_opportunity_candidates
                WHERE run_id = ? AND filters_passed = 1
                ORDER BY score_total DESC, symbol ASC
                LIMIT ?
                """,
                (int(from_top_run_id), int(max_symbols)),
            ).fetchall()
            for r in rows:
                sym = str(r["symbol"] or "").strip().upper()
                if sym and sym not in picked:
                    picked.append(sym)
        finally:
            conn.close()

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
            source_policy=source_policy_v,
            include_reuters=bool(include_reuters),
            include_official=bool(include_official),
            run_stage=run_stage_v,
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
            "source_policy": source_policy_v,
            "errors": errors,
        }
    )


@advisor_opp_app.command("snapshot-universe")
def advisor_opportunities_snapshot_universe(
    ctx: typer.Context,
    as_of: Optional[str] = typer.Option(None, "--as-of", help="Optional YYYY-MM-DD"),
    universe: str = typer.Option("bcba_cedears", "--universe", help="bcba_cedears"),
):
    _print_json(_snapshot_universe_impl(ctx.obj, as_of=as_of, universe=universe))


@advisor_opp_app.command("run")
def advisor_opportunities_run(
    ctx: typer.Context,
    budget_ars: float = typer.Option(..., "--budget-ars"),
    mode: str = typer.Option("both", "--mode", help="new|rebuy|both"),
    variant: str = typer.Option("active", "--variant", help="active|challenger|both"),
    as_of: Optional[str] = typer.Option(None, "--as-of", help="Optional YYYY-MM-DD"),
    top: int = typer.Option(10, "--top", min=1, max=100),
    universe: str = typer.Option("bcba_cedears", "--universe", help="bcba_cedears"),
    fetch_evidence: bool = typer.Option(True, "--fetch-evidence/--no-fetch-evidence"),
    evidence_max_symbols: int = typer.Option(15, "--evidence-max-symbols", min=1, max=200),
    evidence_per_source_limit: int = typer.Option(2, "--evidence-per-source-limit", min=1, max=10),
    evidence_news: bool = typer.Option(True, "--evidence-news/--no-evidence-news"),
    evidence_sec: bool = typer.Option(True, "--evidence-sec/--no-evidence-sec"),
    evidence_timeout_sec: int = typer.Option(10, "--evidence-timeout-sec", min=1, max=60),
    web_link: bool = typer.Option(True, "--web-link/--no-web-link"),
    web_top_k: int = typer.Option(15, "--web-top-k", min=1, max=200),
    web_source_policy: str = typer.Option("strict_official_reuters", "--web-source-policy", help="strict_official_reuters"),
    web_lookback_days: int = typer.Option(120, "--web-lookback-days", min=1, max=3650),
    web_min_trusted_refs: int = typer.Option(2, "--web-min-trusted-refs", min=0, max=20),
    web_conflict_mode: str = typer.Option("manual_review", "--web-conflict-mode", help="manual_review"),
    web_reuters: bool = typer.Option(True, "--web-reuters/--no-web-reuters"),
    web_official: bool = typer.Option(True, "--web-official/--no-web-official"),
    exclude_crypto_new: bool = typer.Option(True, "--exclude-crypto-new/--include-crypto-new"),
    min_volume_amount: float = typer.Option(50000.0, "--min-volume-amount", min=0.0),
    min_operations: int = typer.Option(5, "--min-operations", min=0, max=1000000),
    liquidity_priority: bool = typer.Option(True, "--liquidity-priority/--no-liquidity-priority"),
    diversify_sectors: bool = typer.Option(True, "--diversify-sectors/--no-diversify-sectors"),
    max_per_sector: int = typer.Option(2, "--max-per-sector", min=1, max=20),
):
    try:
        payload = _run_opportunity_pipeline_impl(
            ctx.obj,
            budget_ars=float(budget_ars),
            mode=mode,
            as_of=as_of,
            top=int(top),
            universe=universe,
            fetch_evidence=bool(fetch_evidence),
            evidence_max_symbols=int(evidence_max_symbols),
            evidence_per_source_limit=int(evidence_per_source_limit),
            evidence_news=bool(evidence_news),
            evidence_sec=bool(evidence_sec),
            evidence_timeout_sec=int(evidence_timeout_sec),
            web_link=bool(web_link),
            web_top_k=int(web_top_k),
            web_source_policy=web_source_policy,
            web_lookback_days=int(web_lookback_days),
            web_min_trusted_refs=int(web_min_trusted_refs),
            web_conflict_mode=web_conflict_mode,
            web_reuters=bool(web_reuters),
            web_official=bool(web_official),
            exclude_crypto_new=bool(exclude_crypto_new),
            min_volume_amount=float(min_volume_amount),
            min_operations=int(min_operations),
            liquidity_priority=bool(liquidity_priority),
            diversify_sectors=bool(diversify_sectors),
            max_per_sector=int(max_per_sector),
            variant=variant,
        )
        _print_json(payload)
    except Exception as exc:
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
            SELECT id, created_at_utc, as_of, mode, universe, budget_ars, top_n, status, error_message, pipeline_warnings_json, run_metrics_json
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
                   filters_passed, expert_signal_score, trusted_refs_count, consensus_state, decision_gate, candidate_status,
                   evidence_summary_json
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
                SELECT id, created_at_utc, as_of, mode, universe, budget_ars, top_n, status, error_message, pipeline_warnings_json
                FROM advisor_opportunity_runs
                ORDER BY id DESC
                LIMIT ?
                """,
                (int(limit),),
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT id, created_at_utc, as_of, mode, universe, budget_ars, top_n, status, error_message, pipeline_warnings_json
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


@advisor_opp_variants_app.command("list")
def advisor_opportunities_variants_list(
    ctx: typer.Context,
):
    db_path = resolve_db_path(ctx.obj.config.db_path)
    conn = connect(db_path)
    init_db(conn)
    try:
        rows = [row.to_dict() for row in list_model_variants(conn)]
        _print_json(rows)
    finally:
        conn.close()


@advisor_opp_app.command("evaluate")
def advisor_opportunities_evaluate(
    ctx: typer.Context,
    as_of: Optional[str] = typer.Option(None, "--as-of", help="Optional YYYY-MM-DD"),
):
    db_path = resolve_db_path(ctx.obj.config.db_path)
    conn = connect(db_path)
    init_db(conn)
    try:
        payload = evaluate_signal_outcomes(conn, as_of=as_of)
        _print_json(payload)
    finally:
        conn.close()


@advisor_opp_app.command("scorecard")
def advisor_opportunities_scorecard(
    ctx: typer.Context,
    as_of: Optional[str] = typer.Option(None, "--as-of", help="Optional YYYY-MM-DD"),
    window_days: int = typer.Option(DEFAULT_WINDOW_DAYS, "--window-days", min=7, max=3650),
):
    db_path = resolve_db_path(ctx.obj.config.db_path)
    conn = connect(db_path)
    init_db(conn)
    try:
        active_row = active_variant(conn)
        challenger_row = challenger_variant(conn)
        active_score = build_variant_scorecard(conn, variant_id=int(active_row.id), as_of=as_of, window_days=int(window_days)) if active_row else {}
        challenger_score = build_variant_scorecard(conn, variant_id=int(challenger_row.id), as_of=as_of, window_days=int(window_days)) if challenger_row else {}
        compare = compare_scorecards(active_score, challenger_score)
        _print_json(
            {
                "as_of": as_of,
                "window_days": int(window_days),
                "active_variant": active_row.to_dict() if active_row else None,
                "challenger_variant": challenger_row.to_dict() if challenger_row else None,
                "active_scorecard": active_score,
                "challenger_scorecard": challenger_score,
                "comparison": compare,
            }
        )
    finally:
        conn.close()


@advisor_autopilot_app.command("run")
def advisor_autopilot_run(
    ctx: typer.Context,
    cadence: str = typer.Option(..., "--cadence", help="daily|weekly"),
    as_of: Optional[str] = typer.Option(None, "--as-of", help="Optional YYYY-MM-DD"),
    budget_ars: float = typer.Option(100000.0, "--budget-ars"),
    top: int = typer.Option(10, "--top", min=1, max=100),
    mode: str = typer.Option("both", "--mode", help="new|rebuy|both"),
    universe: str = typer.Option("bcba_cedears", "--universe", help="bcba_cedears"),
    source_policy: str = typer.Option(DEFAULT_SOURCE_POLICY, "--source-policy", help="strict_official_reuters"),
    out: Optional[str] = typer.Option(None, "--out", help="Optional markdown output file"),
    opportunity_report_out: Optional[str] = typer.Option(None, "--opportunity-report-out", help="Optional weekly opportunities markdown"),
    force: bool = typer.Option(False, "--force", help="Persist a new briefing even if same cadence+as_of already exists"),
):
    cadence_v = cadence.strip().lower()
    if cadence_v not in ("daily", "weekly"):
        raise typer.BadParameter("--cadence must be daily|weekly")
    source_policy_v = _normalize_enum(source_policy, "--source-policy", _SOURCE_POLICIES)
    db_path = resolve_db_path(ctx.obj.config.db_path)
    conn = connect(db_path)
    init_db(conn)
    try:
        ensure_default_model_variants(conn)
        eval_summary = evaluate_signal_outcomes(conn, as_of=as_of)
        active_before = active_variant(conn)
        challenger_before = challenger_variant(conn)
        active_score_before = build_variant_scorecard(conn, variant_id=int(active_before.id), as_of=as_of, window_days=DEFAULT_WINDOW_DAYS) if active_before else {}
        challenger_score_before = build_variant_scorecard(conn, variant_id=int(challenger_before.id), as_of=as_of, window_days=DEFAULT_WINDOW_DAYS) if challenger_before else {}
        comparison_before = compare_scorecards(active_score_before, challenger_score_before)
    finally:
        conn.close()

    latest_run = None
    run_payload: Dict[str, Any] = {}
    if cadence_v == "weekly":
        _snapshot_universe_impl(ctx.obj, as_of=as_of, universe=universe)
        run_payload = _run_opportunity_pipeline_impl(
            ctx.obj,
            budget_ars=float(budget_ars),
            mode=mode,
            as_of=as_of,
            top=int(top),
            universe=universe,
            fetch_evidence=True,
            evidence_max_symbols=15,
            evidence_per_source_limit=2,
            evidence_news=True,
            evidence_sec=True,
            evidence_timeout_sec=10,
            web_link=True,
            web_top_k=15,
            web_source_policy=source_policy_v,
            web_lookback_days=120,
            web_min_trusted_refs=2,
            web_conflict_mode="manual_review",
            web_reuters=True,
            web_official=True,
            exclude_crypto_new=True,
            min_volume_amount=50000.0,
            min_operations=5,
            liquidity_priority=True,
            diversify_sectors=True,
            max_per_sector=2,
            variant="both",
            cadence=cadence_v,
            reuse_existing=True,
        )
        if opportunity_report_out and run_payload:
            active_rows = list(run_payload.get("variant_runs") or [])
            active_report = next((row for row in active_rows if int(row.get("variant_id") or 0) == int(run_payload.get("active_variant_id") or 0)), None) or run_payload
            conn = connect(db_path)
            init_db(conn)
            try:
                latest_run = load_latest_opportunity_payload(db_path) or active_report
            finally:
                conn.close()
            md = report_markdown(latest_run, latest_run.get("candidates") or [])
            with open(opportunity_report_out, "w", encoding="utf-8") as f:
                f.write(md)
    else:
        run_payload = _run_opportunity_pipeline_impl(
            ctx.obj,
            budget_ars=float(budget_ars),
            mode=mode,
            as_of=as_of,
            top=int(top),
            universe=universe,
            fetch_evidence=True,
            evidence_max_symbols=15,
            evidence_per_source_limit=2,
            evidence_news=True,
            evidence_sec=True,
            evidence_timeout_sec=10,
            web_link=True,
            web_top_k=15,
            web_source_policy=source_policy_v,
            web_lookback_days=120,
            web_min_trusted_refs=2,
            web_conflict_mode="manual_review",
            web_reuters=True,
            web_official=True,
            exclude_crypto_new=True,
            min_volume_amount=50000.0,
            min_operations=5,
            liquidity_priority=True,
            diversify_sectors=True,
            max_per_sector=2,
            variant="both",
            cadence=cadence_v,
            reuse_existing=True,
        )

    conn = connect(db_path)
    init_db(conn)
    try:
        ensure_default_model_variants(conn)
        active_current = active_variant(conn)
        challenger_current = challenger_variant(conn)
        active_score_after = build_variant_scorecard(conn, variant_id=int(active_current.id), as_of=as_of, window_days=DEFAULT_WINDOW_DAYS) if active_current else {}
        challenger_score_after = build_variant_scorecard(conn, variant_id=int(challenger_current.id), as_of=as_of, window_days=DEFAULT_WINDOW_DAYS) if challenger_current else {}
        comparison_after = compare_scorecards(active_score_after, challenger_score_after)
        promotion = {"promoted": False, "reason": "not_weekly", "flags": []}
        if cadence_v == "weekly" and active_before and challenger_before and active_score_after and challenger_score_after:
            promotion = maybe_promote_challenger(
                conn,
                active_variant_id=int(active_before.id),
                challenger_variant_id=int(challenger_before.id),
                active_scorecard=active_score_after,
                challenger_scorecard=challenger_score_after,
            )
            if promotion.get("promoted"):
                active_current = active_variant(conn)
                challenger_current = challenger_variant(conn)
                active_score_after = build_variant_scorecard(conn, variant_id=int(active_current.id), as_of=as_of, window_days=DEFAULT_WINDOW_DAYS) if active_current else {}
                challenger_score_after = build_variant_scorecard(conn, variant_id=int(challenger_current.id), as_of=as_of, window_days=DEFAULT_WINDOW_DAYS) if challenger_current else {}
                comparison_after = compare_scorecards(active_score_after, challenger_score_after)
                comparison_after["regression_flags"] = list(dict.fromkeys(list(comparison_after.get("regression_flags") or []) + list(promotion.get("flags") or [])))

        for row in (run_payload.get("variant_runs") or []):
            variant_id = int(row.get("variant_id") or 0)
            scorecard = active_score_after if active_current and variant_id == int(active_current.id) else challenger_score_after
            gate_status = comparison_after.get("gate_status") if active_current and variant_id == int(active_current.id) else str((scorecard or {}).get("status") or "ok")
            regression_flags = comparison_after.get("regression_flags") if active_current and variant_id == int(active_current.id) else list(promotion.get("flags") or [])
            baseline_id = int(active_before.id if active_before else variant_id)
            if row.get("run_id"):
                insert_run_regression(
                    conn,
                    run_id=int(row["run_id"]),
                    cadence=cadence_v,
                    variant_id=variant_id,
                    baseline_variant_id=baseline_id,
                    window_days=DEFAULT_WINDOW_DAYS,
                    scorecard=dict(scorecard or {}),
                    gate_status=str(gate_status or "ok"),
                    regression_flags=list(regression_flags or []),
                )

        active_runs = list(run_payload.get("variant_runs") or [])
        latest_run = next((row for row in active_runs if active_current and int(row.get("variant_id") or 0) == int(active_current.id)), None)
        if latest_run is None:
            latest_run = load_latest_opportunity_payload(db_path)
        regression_payload = {
            "gate_status": comparison_after.get("gate_status"),
            "regression_flags": list(comparison_after.get("regression_flags") or []),
            "scorecard": dict(active_score_after or {}),
            "comparison": comparison_after,
            "promotion": promotion,
        }
        active_variant_payload = active_current.to_dict() if active_current else None
    finally:
        conn.close()

    context = build_unified_context(
        db_path,
        as_of=as_of,
        limit=10,
        history_days=365,
        include_cash=True,
        include_orders=False,
    )

    if cadence_v == "daily" and latest_run:
        try:
            snapshot_date = str(((context or {}).get("snapshot") or {}).get("snapshot_date") or "")
            if snapshot_date and latest_run.get("as_of"):
                age_days = (date.fromisoformat(snapshot_date) - date.fromisoformat(str(latest_run.get("as_of")))).days
                if age_days > 7:
                    latest_run = None
        except Exception:
            pass

    bundle = persist_briefing_bundle(
        db_path=db_path,
        cadence=cadence_v,
        env=ctx.obj.env,
        base_url=ctx.obj.base_url,
        context=context,
        latest_run=latest_run,
        regression=regression_payload,
        active_variant=active_variant_payload,
        source_policy=source_policy_v,
        force=bool(force),
    )
    briefing = bundle.briefing
    if out and briefing:
        with open(out, "w", encoding="utf-8") as f:
            f.write(str(briefing.get("summary_md") or ""))
    _print_json(
        {
            "briefing": briefing,
            "reused": bool(bundle.reused),
            "weekly_run_id": (latest_run or {}).get("id"),
            "evaluation": eval_summary,
            "comparison_before": comparison_before,
            "comparison_after": regression_payload,
            "active_variant": active_variant_payload,
            "out": out,
            "opportunity_report_out": opportunity_report_out,
        }
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
