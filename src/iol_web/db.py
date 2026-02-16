import os
import sqlite3
import unicodedata
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


@dataclass(frozen=True)
class Snapshot:
    snapshot_date: str
    total_value: float
    currency: Optional[str] = None
    titles_value: Optional[float] = None
    cash_total_ars: Optional[float] = None
    cash_disponible_ars: Optional[float] = None
    cash_disponible_usd: Optional[float] = None
    retrieved_at: Optional[str] = None
    close_time: Optional[str] = None
    minutes_from_close: Optional[int] = None
    source: Optional[str] = None


def resolve_db_path() -> str:
    raw = (os.getenv("IOL_DB_PATH") or "data/iol_history.db").strip()
    if os.path.isabs(raw):
        return raw
    return os.path.abspath(os.path.join(os.getcwd(), raw))


def _connect_ro(db_path: str) -> sqlite3.Connection:
    p = Path(db_path)
    if not p.exists():
        raise FileNotFoundError(db_path)
    uri_path = p.resolve().as_posix()
    conn = sqlite3.connect(f"file:{uri_path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def get_conn() -> sqlite3.Connection:
    return _connect_ro(resolve_db_path())


def _row_to_snapshot(row: sqlite3.Row) -> Snapshot:
    keys = set(row.keys())
    return Snapshot(
        snapshot_date=str(row["snapshot_date"]),
        total_value=float(row["total_value"] or 0.0),
        currency=row["currency"] if "currency" in keys else None,
        titles_value=float(row["titles_value"]) if ("titles_value" in keys and row["titles_value"] is not None) else None,
        cash_total_ars=float(row["cash_total_ars"])
        if ("cash_total_ars" in keys and row["cash_total_ars"] is not None)
        else None,
        cash_disponible_ars=float(row["cash_disponible_ars"])
        if ("cash_disponible_ars" in keys and row["cash_disponible_ars"] is not None)
        else None,
        cash_disponible_usd=float(row["cash_disponible_usd"])
        if ("cash_disponible_usd" in keys and row["cash_disponible_usd"] is not None)
        else None,
        retrieved_at=str(row["retrieved_at"]) if ("retrieved_at" in keys and row["retrieved_at"] is not None) else None,
        close_time=str(row["close_time"]) if ("close_time" in keys and row["close_time"] is not None) else None,
        minutes_from_close=int(row["minutes_from_close"])
        if ("minutes_from_close" in keys and row["minutes_from_close"] is not None)
        else None,
        source=str(row["source"]) if ("source" in keys and row["source"] is not None) else None,
    )


def latest_snapshot(conn: sqlite3.Connection) -> Optional[Snapshot]:
    row = conn.execute(
        """
        SELECT *
        FROM portfolio_snapshots
        ORDER BY snapshot_date DESC
        LIMIT 1
        """
    ).fetchone()
    return _row_to_snapshot(row) if row else None


def earliest_snapshot(conn: sqlite3.Connection) -> Optional[Snapshot]:
    row = conn.execute(
        """
        SELECT *
        FROM portfolio_snapshots
        ORDER BY snapshot_date ASC
        LIMIT 1
        """
    ).fetchone()
    return _row_to_snapshot(row) if row else None


def snapshot_before(conn: sqlite3.Connection, before_date: str) -> Optional[Snapshot]:
    row = conn.execute(
        """
        SELECT *
        FROM portfolio_snapshots
        WHERE snapshot_date < ?
        ORDER BY snapshot_date DESC
        LIMIT 1
        """,
        (before_date,),
    ).fetchone()
    return _row_to_snapshot(row) if row else None


def snapshot_on_or_before(conn: sqlite3.Connection, target_date: str) -> Optional[Snapshot]:
    row = conn.execute(
        """
        SELECT *
        FROM portfolio_snapshots
        WHERE snapshot_date <= ?
        ORDER BY snapshot_date DESC
        LIMIT 1
        """,
        (target_date,),
    ).fetchone()
    return _row_to_snapshot(row) if row else None


def first_snapshot_of_year(conn: sqlite3.Connection, year: int, latest_date: str) -> Optional[Snapshot]:
    start = date(year, 1, 1).isoformat()
    row = conn.execute(
        """
        SELECT *
        FROM portfolio_snapshots
        WHERE snapshot_date >= ? AND snapshot_date <= ?
        ORDER BY snapshot_date ASC
        LIMIT 1
        """,
        (start, latest_date),
    ).fetchone()
    return _row_to_snapshot(row) if row else None


def first_snapshot_in_range(conn: sqlite3.Connection, start_date: str, end_date: str) -> Optional[Snapshot]:
    row = conn.execute(
        """
        SELECT *
        FROM portfolio_snapshots
        WHERE snapshot_date >= ? AND snapshot_date <= ?
        ORDER BY snapshot_date ASC
        LIMIT 1
        """,
        (start_date, end_date),
    ).fetchone()
    return _row_to_snapshot(row) if row else None


def last_snapshot_in_range(conn: sqlite3.Connection, start_date: str, end_date: str) -> Optional[Snapshot]:
    row = conn.execute(
        """
        SELECT *
        FROM portfolio_snapshots
        WHERE snapshot_date >= ? AND snapshot_date <= ?
        ORDER BY snapshot_date DESC
        LIMIT 1
        """,
        (start_date, end_date),
    ).fetchone()
    return _row_to_snapshot(row) if row else None


def snapshots_series(conn: sqlite3.Connection, date_from: Optional[str], date_to: Optional[str]) -> List[Tuple[str, float]]:
    latest = latest_snapshot(conn)
    earliest = earliest_snapshot(conn)
    if not latest or not earliest:
        return []
    f = date_from or earliest.snapshot_date
    t = date_to or latest.snapshot_date
    rows = conn.execute(
        """
        SELECT snapshot_date, total_value
        FROM portfolio_snapshots
        WHERE snapshot_date >= ? AND snapshot_date <= ?
        ORDER BY snapshot_date ASC
        """,
        (f, t),
    ).fetchall()
    return [(str(r["snapshot_date"]), float(r["total_value"] or 0.0)) for r in rows]


def monthly_first_last_series(conn: sqlite3.Connection, date_from: str, date_to: str) -> List[Dict[str, Any]]:
    """
    Calendar-month aggregation of snapshots.

    Returns one row per month (YYYY-MM) with first/last snapshot dates and values inside [date_from, date_to].
    """
    rows = conn.execute(
        """
        WITH monthly AS (
          SELECT
            substr(snapshot_date, 1, 7) AS month,
            MIN(snapshot_date) AS first_date,
            MAX(snapshot_date) AS last_date
          FROM portfolio_snapshots
          WHERE snapshot_date >= ? AND snapshot_date <= ?
          GROUP BY month
        )
        SELECT
          m.month AS month,
          m.first_date AS first_date,
          m.last_date AS last_date,
          s1.total_value AS first_value,
          s2.total_value AS last_value
        FROM monthly m
        JOIN portfolio_snapshots s1 ON s1.snapshot_date = m.first_date
        JOIN portfolio_snapshots s2 ON s2.snapshot_date = m.last_date
        ORDER BY m.month ASC
        """,
        (date_from, date_to),
    ).fetchall()

    out: List[Dict[str, Any]] = []
    for r in rows or []:
        out.append(
            {
                "month": str(r["month"]),
                "first_date": str(r["first_date"]),
                "last_date": str(r["last_date"]),
                "first_value": float(r["first_value"] or 0.0),
                "last_value": float(r["last_value"] or 0.0),
            }
        )
    return out


def assets_for_snapshot(conn: sqlite3.Connection, snapshot_date: str) -> List[Dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT
            symbol, description, market, type, currency, plazo,
            quantity, last_price, ppc, total_value,
            daily_var_pct, daily_var_points, gain_pct, gain_amount, committed
        FROM portfolio_assets
        WHERE snapshot_date = ?
        """,
        (snapshot_date,),
    ).fetchall()
    out: List[Dict[str, Any]] = []
    for r in rows:
        out.append(
            {
                "symbol": r["symbol"],
                "description": r["description"],
                "market": r["market"],
                "type": r["type"],
                "currency": r["currency"],
                "plazo": r["plazo"],
                "quantity": float(r["quantity"] or 0.0),
                "last_price": float(r["last_price"] or 0.0),
                "ppc": float(r["ppc"] or 0.0) if r["ppc"] is not None else None,
                "total_value": float(r["total_value"] or 0.0),
                "daily_var_pct": float(r["daily_var_pct"] or 0.0) if r["daily_var_pct"] is not None else None,
                "daily_var_points": float(r["daily_var_points"] or 0.0) if r["daily_var_points"] is not None else None,
                "gain_pct": float(r["gain_pct"] or 0.0) if r["gain_pct"] is not None else None,
                "gain_amount": float(r["gain_amount"] or 0.0) if r["gain_amount"] is not None else None,
                "committed": float(r["committed"] or 0.0) if r["committed"] is not None else None,
            }
        )
    return out


def allocation(conn: sqlite3.Connection, snapshot_date: str, group_by: str) -> List[Tuple[str, float]]:
    allowed = {"symbol", "type", "market", "currency"}
    if group_by not in allowed:
        raise ValueError(f"invalid group_by: {group_by}")

    rows = conn.execute(
        f"""
        SELECT {group_by} AS k, SUM(total_value) AS v
        FROM portfolio_assets
        WHERE snapshot_date = ?
        GROUP BY {group_by}
        """,
        (snapshot_date,),
    ).fetchall()
    out: List[Tuple[str, float]] = []
    for r in rows:
        key = r["k"] if r["k"] is not None and str(r["k"]).strip() else "unknown"
        out.append((str(key), float(r["v"] or 0.0)))
    out.sort(key=lambda kv: kv[1], reverse=True)
    return out


def _table_columns(conn: sqlite3.Connection, table: str) -> set:
    try:
        rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
        return {r[1] for r in rows}  # r[1] = column name
    except Exception:
        return set()


def orders_cashflows_by_symbol(
    conn: sqlite3.Connection,
    dt_from: str,
    dt_to: str,
    currency: str = "peso_Argentino",
) -> Tuple[Dict[str, Dict[str, float]], Dict[str, int]]:
    """
    Aggregate executed order cashflows for the period [dt_from, dt_to].

    Returns (cashflows_by_symbol, stats)
      cashflows_by_symbol[symbol] = {"buy_amount": x, "sell_amount": y}
      stats = {"total": n, "classified": n, "unclassified": n, "amount_missing": n, "ignored": n}
    """
    cols = _table_columns(conn, "orders")
    if not cols:
        return {}, {"total": 0, "classified": 0, "unclassified": 0, "amount_missing": 0, "ignored": 0}

    ts_col = "operated_at" if "operated_at" in cols else ("updated_at" if "updated_at" in cols else "created_at")
    side_expr = None
    if "side_norm" in cols and "side" in cols:
        side_expr = "COALESCE(NULLIF(TRIM(side_norm), ''), side)"
    elif "side_norm" in cols:
        side_expr = "side_norm"
    elif "side" in cols:
        side_expr = "side"
    has_currency = "currency" in cols

    if side_expr is None:
        return {}, {"total": 0, "classified": 0, "unclassified": 0, "amount_missing": 0, "ignored": 0}

    where = [
        "status = 'terminada'",
        "symbol IS NOT NULL",
        "TRIM(symbol) <> ''",
        f"COALESCE({ts_col}, created_at) >= ?",
        f"COALESCE({ts_col}, created_at) <= ?",
    ]
    params: List[Any] = [dt_from, dt_to]
    if has_currency and currency and currency not in ("all",):
        if currency == "unknown":
            where.append("(currency IS NULL OR TRIM(currency) = '')")
        elif currency == "peso_Argentino":
            # Back-compat: older/newer IOL payloads may not include currency. Treat missing as ARS for dashboard PnL,
            # otherwise we'd incorrectly show sold positions as -100% due to missing cashflows.
            where.append("(currency = ? OR currency IS NULL OR TRIM(currency) = '')")
            params.append(currency)
        else:
            where.append("currency = ?")
            params.append(currency)

    operated_amount_expr = "operated_amount" if "operated_amount" in cols else "NULL"
    quantity_expr = "quantity" if "quantity" in cols else "NULL"
    price_expr = "price" if "price" in cols else "NULL"

    sql = f"""
        SELECT
            symbol AS symbol,
            {side_expr} AS side,
            {operated_amount_expr} AS operated_amount,
            {quantity_expr} AS quantity,
            {price_expr} AS price
        FROM orders
        WHERE {" AND ".join(where)}
    """
    rows = conn.execute(sql, tuple(params)).fetchall()

    def _norm_side(v: Any) -> Optional[str]:
        s = str(v or "").strip().lower()
        if not s:
            return None
        s = "".join(ch for ch in unicodedata.normalize("NFKD", s) if not unicodedata.combining(ch))
        s = " ".join(s.split())
        if s in ("buy", "compra", "suscripcion fci"):
            return "buy"
        if s in ("sell", "venta", "rescate fci", "pago de amortizacion"):
            return "sell"
        if s in ("pago de dividendos", "pago de renta", "ignore"):
            return "ignore"
        return None

    out: Dict[str, Dict[str, float]] = {}
    total = classified = unclassified = amount_missing = ignored = 0
    for r in rows:
        total += 1
        sym = str(r["symbol"])
        side = _norm_side(r["side"])
        if side is None:
            unclassified += 1
            continue
        if side == "ignore":
            ignored += 1
            continue

        amt_f: Optional[float]
        op_amount = r["operated_amount"]
        qty = r["quantity"]
        price = r["price"]
        if op_amount is not None:
            try:
                amt_f = float(op_amount)
            except Exception:
                amt_f = None
        elif qty is not None and price is not None:
            try:
                amt_f = float(qty) * float(price)
            except Exception:
                amt_f = None
        else:
            amt_f = None

        if amt_f is None:
            amount_missing += 1
            continue

        classified += 1
        bucket = out.setdefault(sym, {"buy_amount": 0.0, "sell_amount": 0.0})
        if side == "buy":
            bucket["buy_amount"] += amt_f
        else:
            bucket["sell_amount"] += amt_f

    stats = {
        "total": total,
        "classified": classified,
        "unclassified": unclassified,
        "amount_missing": amount_missing,
        "ignored": ignored,
    }
    return out, stats
