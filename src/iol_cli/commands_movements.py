"""
commands_movements.py — Automated movement ingestion and commission validation.

Commands:
  iol movements sync       — fetch non-trade orders (dividends, coupons, amortizations, fees)
                             from IOL API and ingest them as account_cash_movements.
                             Also probes POST /api/v2/Asesor/Movimientos (graceful 403 fallback).
  iol movements link-fees  — match fee/commission orders to their parent trade order
                             by symbol + timestamp proximity and populate order_fees table.
  iol movements check-fees — compare expected commissions (based on tier) against actual
                             fees linked to each trade order; emit advisor_alerts for discrepancies.
"""
from __future__ import annotations

import json
import unicodedata
from datetime import date, datetime, timezone, timedelta
from typing import Any, Callable, Dict, List, Optional, Tuple

import typer
from rich.console import Console

from .db import connect, init_db, resolve_db_path
from .iol_client import IOLAPIError

console = Console()

# ---------------------------------------------------------------------------
# IOL commission tiers (public tariff schedule, 2024)
# ---------------------------------------------------------------------------
COMMISSION_TIERS: Dict[str, Dict[str, float]] = {
    "gold":     {"stocks_bonds_cedears": 0.005,  "derechos_byma_stocks": 0.0005, "derechos_byma_bonds": 0.0001},
    "platinum": {"stocks_bonds_cedears": 0.003,  "derechos_byma_stocks": 0.0005, "derechos_byma_bonds": 0.0001},
    "black":    {"stocks_bonds_cedears": 0.001,  "derechos_byma_stocks": 0.0005, "derechos_byma_bonds": 0.0001},
}
IVA_RATE = 0.21
FEE_DISCREPANCY_THRESHOLD_PCT = 0.05  # alert if actual fee deviates > 5% from expected

# Side values from IOL API that represent non-trade cash flows
_NON_TRADE_SIDES = frozenset({
    "pago de dividendos",
    "pago de renta",
    "pago de amortizacion",
    "comision",
    "comision de mercado",
    "comision de bolsa",
    "gastos",
    "gastos operativos",
    "iva",
    "impuesto",
    "derechos de mercado",
    "derecho de mercado",
    "fee",
    "tax",
})

# Sides that correspond to fees/commissions
_FEE_SIDES = frozenset({
    "comision", "comision de mercado", "comision de bolsa",
    "gastos", "gastos operativos",
    "iva", "impuesto",
    "derechos de mercado", "derecho de mercado",
    "fee", "tax",
})

# Side → movement kind mapping
_SIDE_TO_KIND: Dict[str, str] = {
    "pago de dividendos":   "dividend_income",
    "pago de renta":        "coupon_income",
    "pago de amortizacion": "bond_amortization_income",
    "comision":             "operational_fee_or_tax",
    "comision de mercado":  "operational_fee_or_tax",
    "comision de bolsa":    "operational_fee_or_tax",
    "gastos":               "operational_fee_or_tax",
    "gastos operativos":    "operational_fee_or_tax",
    "iva":                  "operational_fee_or_tax",
    "impuesto":             "operational_fee_or_tax",
    "derechos de mercado":  "operational_fee_or_tax",
    "derecho de mercado":   "operational_fee_or_tax",
    "fee":                  "operational_fee_or_tax",
    "tax":                  "operational_fee_or_tax",
}

# fee_kind label used in order_fees table
_SIDE_TO_FEE_KIND: Dict[str, str] = {
    "comision":             "comision",
    "comision de mercado":  "comision_mercado",
    "comision de bolsa":    "comision_bolsa",
    "gastos":               "gastos",
    "gastos operativos":    "gastos_operativos",
    "iva":                  "iva",
    "impuesto":             "impuesto",
    "derechos de mercado":  "derechos_mercado",
    "derecho de mercado":   "derechos_mercado",
    "fee":                  "fee",
    "tax":                  "impuesto",
}


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _norm_side(v: Any) -> str:
    s = str(v or "").strip().lower()
    s = "".join(ch for ch in unicodedata.normalize("NFKD", s) if not unicodedata.combining(ch))
    return " ".join(s.split())


def _norm_symbol(v: Any) -> str:
    import re
    s = str(v or "").strip().upper()
    return re.sub(r"\s+(US\$|USD)$", "", s).strip()


def _print_json(data: Any) -> None:
    try:
        console.print_json(json.dumps(data, ensure_ascii=True, indent=2))
    except Exception:
        console.print(data)


def _parse_iso_date_optional(value: Optional[str], label: str) -> Optional[str]:
    if not value:
        return None
    try:
        date.fromisoformat(value.strip())
        return value.strip()
    except Exception as exc:
        raise typer.BadParameter(f"{label} must be YYYY-MM-DD") from exc


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def _upsert_movement(
    conn: Any,
    movement_id: str,
    occurred_at: Optional[str],
    movement_date: str,
    currency: str,
    amount: float,
    kind: str,
    symbol: Optional[str],
    description: Optional[str],
    source: str,
    raw_json: str,
) -> str:
    """Insert or update an account_cash_movement. Returns 'inserted'|'updated'."""
    exists = conn.execute(
        "SELECT id FROM account_cash_movements WHERE movement_id = ?",
        (movement_id,),
    ).fetchone()
    if exists:
        conn.execute(
            """
            UPDATE account_cash_movements
            SET occurred_at=?, movement_date=?, currency=?, amount=?, kind=?,
                symbol=?, description=?, source=?, raw_json=?
            WHERE movement_id=?
            """,
            (occurred_at, movement_date, currency, amount, kind,
             symbol, description, source, raw_json, movement_id),
        )
        return "updated"
    conn.execute(
        """
        INSERT INTO account_cash_movements(
            movement_id, occurred_at, movement_date, currency, amount, kind,
            symbol, description, source, raw_json, created_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
        """,
        (movement_id, occurred_at, movement_date, currency, amount, kind,
         symbol, description, source, raw_json, _utc_now_iso()),
    )
    return "inserted"


def _amount_sign_for_kind(kind: str, raw_amount: float) -> float:
    if kind in ("dividend_income", "coupon_income", "bond_amortization_income"):
        return abs(raw_amount)
    if kind == "operational_fee_or_tax":
        return -abs(raw_amount)
    return raw_amount


# ---------------------------------------------------------------------------
# Order → movement conversion
# ---------------------------------------------------------------------------

def _order_to_movement_row(order: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Convert an IOL operacion record to a movement row dict. Returns None if not a non-trade order."""
    side_raw = _norm_side(order.get("tipo") or order.get("side") or "")
    if side_raw not in _NON_TRADE_SIDES:
        return None

    kind = _SIDE_TO_KIND.get(side_raw, "correction_unknown")
    symbol = _norm_symbol(order.get("simbolo") or order.get("symbol") or "")
    movement_id = f"order:{order.get('numero') or order.get('number') or ''}"

    # Prefer fechaOperada (execution time) over fechaOrden
    occurred_raw = order.get("fechaOperada") or order.get("operatedAt") or order.get("fechaOrden")
    movement_date: Optional[str] = None
    occurred_at: Optional[str] = None
    if occurred_raw:
        raw = str(occurred_raw).strip()
        try:
            if len(raw) == 10:
                movement_date = raw
                occurred_at = f"{raw}T00:00:00"
            else:
                dt = datetime.fromisoformat(raw.replace("Z", "+00:00")).replace(tzinfo=None)
                occurred_at = dt.isoformat(timespec="seconds")
                movement_date = dt.date().isoformat()
        except Exception:
            movement_date = raw[:10] if len(raw) >= 10 else None

    if movement_date is None:
        return None

    amount_raw = order.get("montoOperado") or order.get("operatedAmount") or order.get("monto") or 0.0
    try:
        amount_f = float(amount_raw)
    except Exception:
        amount_f = 0.0

    amount_norm = _amount_sign_for_kind(kind, amount_f)

    currency_raw = str(order.get("moneda") or order.get("currency") or "peso_Argentino").lower()
    if "dolar" in currency_raw or "usd" in currency_raw:
        currency = "USD"
    else:
        currency = "ARS"

    description = f"{side_raw}"
    if symbol:
        description = f"{side_raw} — {symbol}"

    return {
        "movement_id": movement_id,
        "occurred_at": occurred_at,
        "movement_date": movement_date,
        "currency": currency,
        "amount": amount_norm,
        "kind": kind,
        "symbol": symbol or None,
        "description": description,
        "raw_json": json.dumps(order, ensure_ascii=True),
    }


# ---------------------------------------------------------------------------
# Asesor/Movimientos → movement row conversion
# ---------------------------------------------------------------------------

def _asesor_mov_to_row(mov: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Convert a record from POST /api/v2/Asesor/Movimientos to a movement row."""
    from .commands_cashflow_reconcile import (
        _infer_movement_kind,
        _normalize_movement_amount,
        _normalize_symbol_base,
        _norm_text,
        _safe_float,
        _normalize_datetime_or_date,
        _extract_first,
        _norm_currency,
    )

    occurred_raw = _extract_first(
        mov,
        ["fechaHora", "fecha", "occurred_at", "timestamp", "datetime"],
    )
    occurred_at, movement_date = _normalize_datetime_or_date(occurred_raw)
    if movement_date is None:
        return None

    amount_raw = _extract_first(mov, ["importe", "monto", "amount", "value"])
    amount = _safe_float(amount_raw)
    if amount is None:
        return None

    description = _extract_first(mov, ["descripcion", "description", "concepto", "concept", "detalle"])
    kind_raw = _extract_first(mov, ["tipo", "kind", "movementType", "category"])
    kind = _infer_movement_kind(kind_raw, description)
    amount_norm = _normalize_movement_amount(kind, amount)

    currency = _norm_currency(_extract_first(mov, ["moneda", "currency"]))

    symbol_raw = _extract_first(mov, ["simbolo", "symbol", "ticker", "especie"])
    symbol = _normalize_symbol_base(symbol_raw) if symbol_raw else None

    id_raw = _extract_first(mov, ["id", "numero", "nro", "movementId", "transactionId"])
    if id_raw is not None:
        movement_id = f"asesor:{id_raw}"
    else:
        import hashlib
        stable = json.dumps({
            "occurred_at": occurred_at,
            "movement_date": movement_date,
            "currency": currency,
            "amount": round(float(amount_norm), 8),
            "kind": kind,
            "description": str(description or "").strip(),
        }, sort_keys=True, ensure_ascii=True)
        movement_id = f"asesor:hash:{hashlib.sha1(stable.encode()).hexdigest()}"

    return {
        "movement_id": movement_id,
        "occurred_at": occurred_at,
        "movement_date": movement_date,
        "currency": currency,
        "amount": float(amount_norm),
        "kind": kind,
        "symbol": symbol,
        "description": str(description or "").strip() or None,
        "raw_json": json.dumps(mov, ensure_ascii=True),
    }


# ---------------------------------------------------------------------------
# App builder
# ---------------------------------------------------------------------------

def build_movements_app(*, get_client: Callable[[Any], Any]) -> typer.Typer:
    app = typer.Typer(help="Automated movement ingestion and commission validation")

    @app.command("sync")
    def movements_sync(
        ctx: typer.Context,
        date_from: Optional[str] = typer.Option(None, "--from", help="YYYY-MM-DD (default: 90 days ago)"),
        date_to: Optional[str] = typer.Option(None, "--to", help="YYYY-MM-DD (default: today)"),
        country: str = typer.Option("argentina", "--country"),
        dry_run: bool = typer.Option(False, "--dry-run", help="Parse and print without writing to DB"),
    ):
        """Fetch non-trade orders (dividends, coupons, amortizations, fees) from IOL
        and ingest them as account_cash_movements.  Also probes Asesor/Movimientos."""
        f = _parse_iso_date_optional(date_from, "--from")
        t = _parse_iso_date_optional(date_to, "--to")
        today = date.today().isoformat()
        t = t or today
        f = f or (date.today() - timedelta(days=90)).isoformat()

        client = get_client(ctx.obj)
        rows_to_write: List[Dict[str, Any]] = []
        stats: Dict[str, Any] = {
            "orders_fetched": 0,
            "non_trade_from_orders": 0,
            "from_asesor_api": 0,
            "asesor_api_status": "not_tried",
        }

        # --- Phase 1: fetch from /api/v2/operaciones ---
        try:
            params: Dict[str, Any] = {
                "filtro.FechaDesde": f,
                "filtro.FechaHasta": t,
                "filtro.Estado": "terminada",
            }
            if country:
                params["filtro.pais"] = country
            orders_resp = client.list_orders(params=params)
            orders: List[Dict[str, Any]] = []
            if isinstance(orders_resp, list):
                orders = orders_resp
            elif isinstance(orders_resp, dict):
                for key in ("operaciones", "items", "data", "result", "orders"):
                    val = orders_resp.get(key)
                    if isinstance(val, list):
                        orders = val
                        break
                if not orders and orders_resp:
                    orders = [orders_resp]
            stats["orders_fetched"] = len(orders)
            for order in orders:
                row = _order_to_movement_row(order)
                if row:
                    rows_to_write.append({"row": row, "source": "iol_orders_sync"})
                    stats["non_trade_from_orders"] += 1
        except IOLAPIError as exc:
            console.print(f"[yellow]Warning: could not fetch orders: {exc}[/yellow]")

        # --- Phase 2: probe Asesor/Movimientos ---
        try:
            asesor_resp = client.get_asesor_movimientos(
                fecha_desde=f, fecha_hasta=t, pais=country
            )
            asesor_rows: List[Dict[str, Any]] = []
            if isinstance(asesor_resp, list):
                asesor_rows = asesor_resp
            elif isinstance(asesor_resp, dict):
                for key in ("movimientos", "items", "data", "result"):
                    val = asesor_resp.get(key)
                    if isinstance(val, list):
                        asesor_rows = val
                        break
            stats["asesor_api_status"] = "ok"
            for mov in asesor_rows:
                row = _asesor_mov_to_row(mov)
                if row:
                    rows_to_write.append({"row": row, "source": "iol_asesor_sync"})
                    stats["from_asesor_api"] += 1
        except IOLAPIError as exc:
            status_code = ""
            msg = str(exc)
            if "403" in msg:
                status_code = "403_forbidden"
            elif "404" in msg:
                status_code = "404_not_found"
            else:
                status_code = f"error: {msg[:120]}"
            stats["asesor_api_status"] = status_code

        if dry_run:
            _print_json({
                "dry_run": True,
                "stats": stats,
                "rows_preview": [r["row"] for r in rows_to_write[:10]],
            })
            return

        db_path = resolve_db_path(ctx.obj.config.db_path)
        conn = connect(db_path)
        init_db(conn)
        inserted = updated = skipped = 0
        try:
            for item in rows_to_write:
                row = item["row"]
                source = item["source"]
                try:
                    result = _upsert_movement(
                        conn,
                        movement_id=row["movement_id"],
                        occurred_at=row.get("occurred_at"),
                        movement_date=row["movement_date"],
                        currency=row["currency"],
                        amount=float(row["amount"]),
                        kind=row["kind"],
                        symbol=row.get("symbol"),
                        description=row.get("description"),
                        source=source,
                        raw_json=row.get("raw_json", "{}"),
                    )
                    if result == "inserted":
                        inserted += 1
                    else:
                        updated += 1
                except Exception:
                    skipped += 1
            conn.commit()
        finally:
            conn.close()

        _print_json({
            "stats": stats,
            "inserted": inserted,
            "updated": updated,
            "skipped": skipped,
            "window": {"from": f, "to": t},
        })

    @app.command("link-fees")
    def movements_link_fees(
        ctx: typer.Context,
        date_from: Optional[str] = typer.Option(None, "--from", help="YYYY-MM-DD"),
        date_to: Optional[str] = typer.Option(None, "--to", help="YYYY-MM-DD"),
        window_minutes: int = typer.Option(60, "--window-minutes", help="Max minutes between trade and fee order"),
        force: bool = typer.Option(False, "--force", help="Re-link already linked fee orders"),
    ):
        """Match fee/commission orders to their parent buy/sell trade orders by
        symbol + timestamp proximity. Populates order_fees table."""
        f = _parse_iso_date_optional(date_from, "--from")
        t = _parse_iso_date_optional(date_to, "--to")

        db_path = resolve_db_path(ctx.obj.config.db_path)
        conn = connect(db_path)
        init_db(conn)
        try:
            _run_link_fees(conn, f, t, window_minutes=window_minutes, force=force)
        finally:
            conn.close()

    @app.command("check-fees")
    def movements_check_fees(
        ctx: typer.Context,
        date_from: Optional[str] = typer.Option(None, "--from", help="YYYY-MM-DD"),
        date_to: Optional[str] = typer.Option(None, "--to", help="YYYY-MM-DD"),
        tier: str = typer.Option("gold", "--tier", help="gold|platinum|black"),
        instrument_type: str = typer.Option("stocks_bonds_cedears", "--instrument-type",
                                            help="stocks_bonds_cedears"),
        create_alerts: bool = typer.Option(False, "--create-alerts",
                                           help="Write fee_discrepancy alerts to advisor_alerts"),
    ):
        """Compare expected IOL commissions (based on tier tariff) against actual fee
        orders linked to each trade.  Reports discrepancies."""
        f = _parse_iso_date_optional(date_from, "--from")
        t = _parse_iso_date_optional(date_to, "--to")
        tier_v = tier.strip().lower()
        if tier_v not in COMMISSION_TIERS:
            raise typer.BadParameter("--tier must be gold|platinum|black")

        db_path = resolve_db_path(ctx.obj.config.db_path)
        conn = connect(db_path)
        init_db(conn)
        try:
            result = _run_check_fees(conn, f, t, tier=tier_v,
                                     instrument_type=instrument_type,
                                     create_alerts=create_alerts)
            _print_json(result)
        finally:
            conn.close()

    return app


# ---------------------------------------------------------------------------
# Link-fees logic
# ---------------------------------------------------------------------------

def _run_link_fees(
    conn: Any,
    date_from: Optional[str],
    date_to: Optional[str],
    window_minutes: int,
    force: bool,
) -> None:
    where_parts = ["status = 'terminada'", "side_norm IN ('buy', 'sell')"]
    params: List[Any] = []
    if date_from:
        where_parts.append("COALESCE(operated_at, updated_at, created_at) >= ?")
        params.append(f"{date_from}T00:00:00")
    if date_to:
        where_parts.append("COALESCE(operated_at, updated_at, created_at) <= ?")
        params.append(f"{date_to}T23:59:59")

    trade_orders = conn.execute(
        f"""
        SELECT order_number, symbol, side_norm,
               COALESCE(operated_at, updated_at, created_at) AS event_ts,
               operated_amount, quantity, price
        FROM orders
        WHERE {' AND '.join(where_parts)}
        """,
        tuple(params),
    ).fetchall()

    # Fetch fee orders in same date range
    fee_where = ["status = 'terminada'"]
    fee_params: List[Any] = []
    # side_norm is NULL for fees (they get mapped to 'fee' via _norm_order_side)
    # but raw side IS the fee type; check both
    fee_orders_raw = conn.execute(
        """
        SELECT order_number, symbol,
               COALESCE(side_norm, side) AS side_raw,
               side AS side_orig,
               COALESCE(operated_at, updated_at, created_at) AS event_ts,
               operated_amount, quantity, price
        FROM orders
        WHERE status = 'terminada'
          AND (side_norm = 'fee' OR LOWER(TRIM(side)) IN (
                'comision','comision de mercado','comision de bolsa',
                'gastos','gastos operativos','iva','impuesto',
                'derechos de mercado','derecho de mercado','fee','tax'
              ))
        """,
    ).fetchall()

    # If force=False, skip fee orders already linked
    if not force:
        already_linked = {
            r[0] for r in conn.execute("SELECT fee_order_number FROM order_fees").fetchall()
        }
    else:
        already_linked = set()
        conn.execute("DELETE FROM order_fees WHERE link_method = 'auto_symbol_timestamp'")
        conn.commit()

    window_sec = window_minutes * 60
    linked = 0
    unlinked = 0

    for fee_row in fee_orders_raw:
        fee_num = fee_row["order_number"]
        if fee_num in already_linked:
            continue

        fee_symbol = _norm_symbol(fee_row["symbol"] or "")
        fee_ts_raw = str(fee_row["event_ts"] or "")
        try:
            fee_dt = datetime.fromisoformat(fee_ts_raw.replace("Z", "+00:00")).replace(tzinfo=None)
        except Exception:
            unlinked += 1
            continue

        fee_amount = 0.0
        try:
            fee_amount = abs(float(fee_row["operated_amount"] or 0))
        except Exception:
            pass
        if fee_amount == 0 and fee_row["quantity"] is not None and fee_row["price"] is not None:
            try:
                fee_amount = abs(float(fee_row["quantity"]) * float(fee_row["price"]))
            except Exception:
                pass

        side_orig = _norm_side(fee_row["side_orig"] or "")
        fee_kind = _SIDE_TO_FEE_KIND.get(side_orig, "other_fee")

        best_match: Optional[int] = None
        best_delta = window_sec + 1
        for trade_row in trade_orders:
            if _norm_symbol(trade_row["symbol"] or "") != fee_symbol:
                continue
            trade_ts_raw = str(trade_row["event_ts"] or "")
            try:
                trade_dt = datetime.fromisoformat(trade_ts_raw.replace("Z", "+00:00")).replace(tzinfo=None)
            except Exception:
                continue
            delta = abs((fee_dt - trade_dt).total_seconds())
            if delta < best_delta:
                best_delta = delta
                best_match = trade_row["order_number"]

        if best_match is not None:
            conn.execute(
                """
                INSERT INTO order_fees(
                    trade_order_number, fee_order_number, fee_kind, symbol,
                    amount_ars, occurred_at, linked_at_utc, link_method
                ) VALUES (?,?,?,?,?,?,?,?)
                """,
                (
                    best_match,
                    fee_num,
                    fee_kind,
                    fee_symbol or None,
                    fee_amount,
                    fee_ts_raw,
                    _utc_now_iso(),
                    "auto_symbol_timestamp",
                ),
            )
            linked += 1
        else:
            unlinked += 1

    conn.commit()
    _print_json({
        "linked": linked,
        "unlinked_fee_orders": unlinked,
        "window_minutes": window_minutes,
        "force": force,
    })


# ---------------------------------------------------------------------------
# Check-fees logic
# ---------------------------------------------------------------------------

def _run_check_fees(
    conn: Any,
    date_from: Optional[str],
    date_to: Optional[str],
    tier: str,
    instrument_type: str,
    create_alerts: bool,
) -> Dict[str, Any]:
    rates = COMMISSION_TIERS[tier]
    commission_rate = rates.get(instrument_type, rates["stocks_bonds_cedears"])
    # derechos_byma: use stocks rate as conservative default; bonds would be lower
    derechos_rate = rates["derechos_byma_stocks"]

    where_parts = ["o.status = 'terminada'", "o.side_norm IN ('buy', 'sell')"]
    params: List[Any] = []
    if date_from:
        where_parts.append("COALESCE(o.operated_at, o.updated_at, o.created_at) >= ?")
        params.append(f"{date_from}T00:00:00")
    if date_to:
        where_parts.append("COALESCE(o.operated_at, o.updated_at, o.created_at) <= ?")
        params.append(f"{date_to}T23:59:59")

    trades = conn.execute(
        f"""
        SELECT o.order_number, o.symbol, o.side_norm,
               COALESCE(o.operated_amount, o.quantity * o.price) AS amount
        FROM orders o
        WHERE {' AND '.join(where_parts)}
        """,
        tuple(params),
    ).fetchall()

    # Load linked fees grouped by trade order
    fee_rows = conn.execute(
        """
        SELECT trade_order_number, SUM(amount_ars) AS total_fee
        FROM order_fees
        GROUP BY trade_order_number
        """,
    ).fetchall()
    actual_fees_by_trade: Dict[int, float] = {
        int(r["trade_order_number"]): float(r["total_fee"] or 0.0)
        for r in fee_rows
    }

    discrepancies = []
    ok_count = 0
    missing_link_count = 0

    for trade in trades:
        order_num = int(trade["order_number"])
        amount = float(trade["amount"] or 0.0)
        if amount <= 0:
            continue

        expected_commission = amount * commission_rate
        expected_iva = expected_commission * IVA_RATE
        expected_derechos = amount * derechos_rate
        expected_total = expected_commission + expected_iva + expected_derechos

        if order_num not in actual_fees_by_trade:
            missing_link_count += 1
            continue

        actual_total = actual_fees_by_trade[order_num]
        if expected_total == 0:
            continue

        diff_pct = abs(actual_total - expected_total) / expected_total
        if diff_pct > FEE_DISCREPANCY_THRESHOLD_PCT:
            discrepancies.append({
                "order_number": order_num,
                "symbol": trade["symbol"],
                "side": trade["side_norm"],
                "trade_amount_ars": round(amount, 2),
                "expected_total_fee_ars": round(expected_total, 2),
                "actual_total_fee_ars": round(actual_total, 2),
                "diff_pct": round(diff_pct * 100, 2),
                "breakdown": {
                    "commission": round(expected_commission, 2),
                    "iva": round(expected_iva, 2),
                    "derechos_byma": round(expected_derechos, 2),
                },
            })
            if create_alerts:
                _write_fee_alert(conn, order_num, trade["symbol"], amount,
                                 expected_total, actual_total, diff_pct, tier)
        else:
            ok_count += 1

    if create_alerts and discrepancies:
        conn.commit()

    return {
        "tier": tier,
        "commission_rate_pct": round(commission_rate * 100, 3),
        "trades_checked": len(trades),
        "ok": ok_count,
        "discrepancies": len(discrepancies),
        "missing_fee_link": missing_link_count,
        "threshold_pct": FEE_DISCREPANCY_THRESHOLD_PCT * 100,
        "details": discrepancies,
    }


def _write_fee_alert(
    conn: Any,
    order_number: int,
    symbol: Optional[str],
    trade_amount: float,
    expected_fee: float,
    actual_fee: float,
    diff_pct: float,
    tier: str,
) -> None:
    now = _utc_now_iso()
    direction = "overcharged" if actual_fee > expected_fee else "undercharged"
    conn.execute(
        """
        INSERT INTO advisor_alerts(
            created_at, updated_at, status, severity, alert_type,
            title, description, symbol, snapshot_date, due_date, closed_at, closed_reason
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            now, now,
            "open",
            "medium",
            "fee_discrepancy",
            f"Fee discrepancy on order #{order_number} ({symbol or 'N/A'})",
            (
                f"Order #{order_number} ({symbol}) trade amount ARS {trade_amount:,.0f} "
                f"— expected fee ARS {expected_fee:,.2f}, actual ARS {actual_fee:,.2f} "
                f"({direction}, {diff_pct*100:.1f}% diff, tier={tier})"
            ),
            symbol,
            None,
            None,
            None,
            None,
        ),
    )
