import json
from datetime import date, datetime, time, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

from .config import Config
from .db import connect, init_db, resolve_db_path
from .iol_client import IOLClient
from .util import normalize_country


def _parse_close_time(close_time: str) -> Tuple[int, int]:
    parts = close_time.split(":")
    if len(parts) != 2:
        raise ValueError(f"Invalid close time: {close_time}")
    return int(parts[0]), int(parts[1])


def _previous_business_day(d: date) -> date:
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d


def _target_snapshot_date(now_local: datetime, close_time: str) -> date:
    if now_local.date().weekday() >= 5:
        return _previous_business_day(now_local.date())
    hour, minute = _parse_close_time(close_time)
    close_dt = datetime.combine(now_local.date(), time(hour, minute), tzinfo=now_local.tzinfo)
    if now_local >= close_dt:
        return now_local.date()
    prev_day = now_local.date() - timedelta(days=1)
    return _previous_business_day(prev_day)


def _close_dt_for(snapshot_date: date, tz: ZoneInfo, close_time: str) -> datetime:
    hour, minute = _parse_close_time(close_time)
    return datetime.combine(snapshot_date, time(hour, minute), tzinfo=tz)


def _minutes_from_close(retrieved_local: datetime, close_dt: datetime) -> int:
    diff = abs((retrieved_local - close_dt).total_seconds())
    return int(round(diff / 60.0))


def _normalize_assets(portfolio: Dict[str, Any], store_raw: bool) -> List[Dict[str, Any]]:
    assets = []
    for asset in portfolio.get("activos", []) or []:
        titulo = asset.get("titulo", {}) or {}
        item = {
            "symbol": titulo.get("simbolo"),
            "description": titulo.get("descripcion"),
            "market": titulo.get("mercado"),
            "type": titulo.get("tipo"),
            "currency": titulo.get("moneda"),
            "plazo": titulo.get("plazo"),
            "quantity": asset.get("cantidad"),
            "last_price": asset.get("ultimoPrecio"),
            "ppc": asset.get("ppc"),
            "total_value": asset.get("valorizado"),
            "daily_var_pct": asset.get("variacionDiaria"),
            "daily_var_points": asset.get("puntosVariacion"),
            "gain_pct": asset.get("gananciaPorcentaje"),
            "gain_amount": asset.get("gananciaDinero"),
            "committed": asset.get("comprometido"),
            "raw_json": json.dumps(asset, ensure_ascii=True) if store_raw else None,
        }
        assets.append(item)
    return assets


def _infer_currency(assets: List[Dict[str, Any]]) -> Optional[str]:
    currencies = {a.get("currency") for a in assets if a.get("currency")}
    if not currencies:
        return None
    if len(currencies) == 1:
        return currencies.pop()
    return "mixed"

def _normalize_accounts(state: Dict[str, Any], store_raw: bool) -> List[Dict[str, Any]]:
    accounts = []
    for acct in state.get("cuentas", []) or []:
        accounts.append(
            {
                "account_number": acct.get("numero"),
                "account_type": acct.get("tipo"),
                "currency": acct.get("moneda"),
                "disponible": acct.get("disponible"),
                "comprometido": acct.get("comprometido"),
                "saldo": acct.get("saldo"),
                "titulos_valorizados": acct.get("titulosValorizados"),
                "total": acct.get("total"),
                "margen_descubierto": acct.get("margenDescubierto"),
                "status": acct.get("estado"),
                "raw_json": json.dumps(acct, ensure_ascii=True) if store_raw else None,
            }
        )
    return accounts


def _sum_disponible(accounts: List[Dict[str, Any]], currency: str) -> float:
    total = 0.0
    for acct in accounts:
        if acct.get("currency") == currency:
            total += float(acct.get("disponible") or 0.0)
    return total


def _upsert_orders(conn, orders: List[Dict[str, Any]], store_raw: bool) -> int:
    cur = conn.cursor()
    count = 0
    for op in orders:
        order_number = op.get("numero") or op.get("numeroOperacion") or op.get("id")
        if order_number is None:
            continue
        titulo = op.get("titulo", {}) or {}
        payload = {
            "order_number": int(order_number),
            "status": op.get("estado"),
            "symbol": op.get("simbolo") or titulo.get("simbolo"),
            "market": op.get("mercado") or titulo.get("mercado"),
            "side": op.get("tipoOperacion") or op.get("operacion"),
            "quantity": op.get("cantidad") or op.get("cantidadOperada"),
            "price": op.get("precio") or op.get("precioPromedio"),
            "plazo": op.get("plazo"),
            "order_type": op.get("tipoOrden"),
            "created_at": op.get("fecha") or op.get("fechaOrden"),
            "updated_at": op.get("fechaEstado") or op.get("fechaActualizacion"),
            "raw_json": json.dumps(op, ensure_ascii=True) if store_raw else None,
        }
        cur.execute(
            """
            INSERT INTO orders (
                order_number, status, symbol, market, side, quantity, price, plazo,
                order_type, created_at, updated_at, raw_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(order_number) DO UPDATE SET
                status=excluded.status,
                symbol=excluded.symbol,
                market=excluded.market,
                side=excluded.side,
                quantity=excluded.quantity,
                price=excluded.price,
                plazo=excluded.plazo,
                order_type=excluded.order_type,
                created_at=excluded.created_at,
                updated_at=excluded.updated_at,
                raw_json=excluded.raw_json
            """,
            (
                payload["order_number"],
                payload["status"],
                payload["symbol"],
                payload["market"],
                payload["side"],
                payload["quantity"],
                payload["price"],
                payload["plazo"],
                payload["order_type"],
                payload["created_at"],
                payload["updated_at"],
                payload["raw_json"],
            ),
        )
        count += 1
    return count


def _log_run(conn, snapshot_date: str, retrieved_at: str, source: str, status: str, error_message: Optional[str]) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO snapshot_runs (snapshot_date, retrieved_at, source, status, error_message)
        VALUES (?, ?, ?, ?, ?)
        """,
        (snapshot_date, retrieved_at, source, status, error_message),
    )


def _save_snapshot(
    conn,
    snapshot_date: str,
    total_value: float,
    currency: Optional[str],
    retrieved_at: str,
    close_time: str,
    minutes_from_close: int,
    source: str,
    assets: List[Dict[str, Any]],
    accounts: List[Dict[str, Any]],
    titles_value: float,
    cash_disponible_ars: float,
    cash_disponible_usd: float,
    raw_json: Optional[str],
    replace_assets: bool,
) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO portfolio_snapshots (
            snapshot_date, total_value, currency, retrieved_at, close_time,
            minutes_from_close, source, titles_value, cash_disponible_ars, cash_disponible_usd, raw_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(snapshot_date) DO UPDATE SET
            total_value=excluded.total_value,
            currency=excluded.currency,
            retrieved_at=excluded.retrieved_at,
            close_time=excluded.close_time,
            minutes_from_close=excluded.minutes_from_close,
            source=excluded.source,
            titles_value=excluded.titles_value,
            cash_disponible_ars=excluded.cash_disponible_ars,
            cash_disponible_usd=excluded.cash_disponible_usd,
            raw_json=excluded.raw_json
        """,
        (
            snapshot_date,
            total_value,
            currency,
            retrieved_at,
            close_time,
            minutes_from_close,
            source,
            titles_value,
            cash_disponible_ars,
            cash_disponible_usd,
            raw_json,
        ),
    )
    if replace_assets:
        cur.execute("DELETE FROM portfolio_assets WHERE snapshot_date = ?", (snapshot_date,))
    for a in assets:
        cur.execute(
            """
            INSERT INTO portfolio_assets (
                snapshot_date, symbol, description, market, type, currency, plazo,
                quantity, last_price, ppc, total_value,
                daily_var_pct, daily_var_points, gain_pct, gain_amount,
                committed, raw_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(snapshot_date, symbol) DO UPDATE SET
                description=excluded.description,
                market=excluded.market,
                type=excluded.type,
                currency=excluded.currency,
                plazo=excluded.plazo,
                quantity=excluded.quantity,
                last_price=excluded.last_price,
                ppc=excluded.ppc,
                total_value=excluded.total_value,
                daily_var_pct=excluded.daily_var_pct,
                daily_var_points=excluded.daily_var_points,
                gain_pct=excluded.gain_pct,
                gain_amount=excluded.gain_amount,
                committed=excluded.committed,
                raw_json=excluded.raw_json
            """,
            (
                snapshot_date,
                a.get("symbol"),
                a.get("description"),
                a.get("market"),
                a.get("type"),
                a.get("currency"),
                a.get("plazo"),
                a.get("quantity"),
                a.get("last_price"),
                a.get("ppc"),
                a.get("total_value"),
                a.get("daily_var_pct"),
                a.get("daily_var_points"),
                a.get("gain_pct"),
                a.get("gain_amount"),
                a.get("committed"),
                a.get("raw_json"),
            ),
        )

    # Account balances (cash + totals per account/currency)
    cur.execute("DELETE FROM account_balances WHERE snapshot_date = ?", (snapshot_date,))
    for acct in accounts:
        cur.execute(
            """
            INSERT INTO account_balances (
                snapshot_date, account_number, account_type, currency,
                disponible, comprometido, saldo, titulos_valorizados, total,
                margen_descubierto, status, raw_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(snapshot_date, account_type, currency) DO UPDATE SET
                account_number=excluded.account_number,
                disponible=excluded.disponible,
                comprometido=excluded.comprometido,
                saldo=excluded.saldo,
                titulos_valorizados=excluded.titulos_valorizados,
                total=excluded.total,
                margen_descubierto=excluded.margen_descubierto,
                status=excluded.status,
                raw_json=excluded.raw_json
            """,
            (
                snapshot_date,
                acct.get("account_number"),
                acct.get("account_type"),
                acct.get("currency"),
                acct.get("disponible"),
                acct.get("comprometido"),
                acct.get("saldo"),
                acct.get("titulos_valorizados"),
                acct.get("total"),
                acct.get("margen_descubierto"),
                acct.get("status"),
                acct.get("raw_json"),
            ),
        )


def run_snapshot(
    client: IOLClient,
    config: Config,
    country: str,
    source: str,
    replace_assets: bool = True,
) -> Dict[str, Any]:
    tz = ZoneInfo(config.market_tz)
    now_local = datetime.now(tz)
    snapshot_day = _target_snapshot_date(now_local, config.market_close_time)
    close_dt = _close_dt_for(snapshot_day, tz, config.market_close_time)
    retrieved_at = datetime.now(timezone.utc).isoformat()
    minutes = _minutes_from_close(now_local, close_dt)

    portfolio = client.get_portfolio(normalize_country(country))
    assets = _normalize_assets(portfolio, config.store_raw)
    titles_value = float(sum([a.get("total_value") or 0 for a in assets]))
    currency = _infer_currency(assets)
    raw_json = json.dumps(portfolio, ensure_ascii=True) if config.store_raw else None

    # Estado de cuenta incluye cash disponible y un total en pesos (con conversiones)
    state = client.get_account_status()
    accounts = _normalize_accounts(state, config.store_raw)
    cash_disponible_ars = _sum_disponible(accounts, "peso_Argentino")
    cash_disponible_usd = _sum_disponible(accounts, "dolar_Estadounidense")

    total_value = state.get("totalEnPesos")
    if total_value is None:
        # Fallback: if API doesn't return totalEnPesos, keep titles only.
        total_value = titles_value
    total_value = float(total_value)

    orders = client.list_orders(params={"filtro.pais": normalize_country(country)})

    db_path = resolve_db_path(config.db_path)
    conn = connect(db_path)
    init_db(conn)
    try:
        _save_snapshot(
            conn,
            snapshot_date=snapshot_day.isoformat(),
            total_value=total_value,
            currency=currency,
            retrieved_at=retrieved_at,
            close_time=close_dt.astimezone(timezone.utc).isoformat(),
            minutes_from_close=minutes,
            source=source,
            assets=assets,
            accounts=accounts,
            titles_value=titles_value,
            cash_disponible_ars=cash_disponible_ars,
            cash_disponible_usd=cash_disponible_usd,
            raw_json=raw_json,
            replace_assets=replace_assets,
        )
        orders_saved = _upsert_orders(conn, orders or [], config.store_raw)
        _log_run(conn, snapshot_day.isoformat(), retrieved_at, source, "ok", None)
        conn.commit()
    except Exception as exc:
        _log_run(conn, snapshot_day.isoformat(), retrieved_at, source, "error", str(exc))
        conn.commit()
        raise
    finally:
        conn.close()

    return {
        "snapshot_date": snapshot_day.isoformat(),
        "retrieved_at": retrieved_at,
        "minutes_from_close": minutes,
        "total_value": total_value,
        "titles_value": titles_value,
        "cash_disponible_ars": cash_disponible_ars,
        "cash_disponible_usd": cash_disponible_usd,
        "assets": len(assets),
        "orders_saved": orders_saved,
    }


def catchup_snapshot(client: IOLClient, config: Config, country: str) -> Dict[str, Any]:
    tz = ZoneInfo(config.market_tz)
    now_local = datetime.now(tz)
    snapshot_day = _target_snapshot_date(now_local, config.market_close_time)
    close_dt = _close_dt_for(snapshot_day, tz, config.market_close_time)
    minutes = _minutes_from_close(now_local, close_dt)

    db_path = resolve_db_path(config.db_path)
    conn = connect(db_path)
    init_db(conn)
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT minutes_from_close FROM portfolio_snapshots WHERE snapshot_date = ?",
            (snapshot_day.isoformat(),),
        )
        row = cur.fetchone()
    finally:
        conn.close()

    if row and row[0] is not None:
        existing_minutes = int(row[0])
        if minutes >= existing_minutes:
            return {
                "snapshot_date": snapshot_day.isoformat(),
                "action": "skip",
                "reason": "existing snapshot closer to close",
                "existing_minutes": existing_minutes,
                "new_minutes": minutes,
            }

    return run_snapshot(client, config, country, source="startup", replace_assets=True)


def backfill_orders_and_snapshot(
    client: IOLClient,
    config: Config,
    country: str,
    date_from: date,
    date_to: date,
) -> Dict[str, Any]:
    if date_from > date_to:
        raise ValueError("from date must be <= to date")

    orders_params = {
        "filtro.fechaDesde": f"{date_from.isoformat()}T00:00:00",
        "filtro.fechaHasta": f"{date_to.isoformat()}T23:59:59",
    }
    orders_params["filtro.pais"] = normalize_country(country)
    orders = client.list_orders(params=orders_params)

    db_path = resolve_db_path(config.db_path)
    conn = connect(db_path)
    init_db(conn)
    try:
        orders_saved = _upsert_orders(conn, orders or [], config.store_raw)
        conn.commit()
    finally:
        conn.close()

    tz = ZoneInfo(config.market_tz)
    now_local = datetime.now(tz)
    target_date = _target_snapshot_date(now_local, config.market_close_time)
    snapshot_result = None
    if date_from <= target_date <= date_to:
        snapshot_result = run_snapshot(client, config, country, source="backfill", replace_assets=True)

    return {
        "orders_saved": orders_saved,
        "snapshot_result": snapshot_result,
    }
