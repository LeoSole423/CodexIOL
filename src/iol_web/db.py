import os
import sqlite3
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
    cash_disponible_ars: Optional[float] = None
    cash_disponible_usd: Optional[float] = None


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
        cash_disponible_ars=float(row["cash_disponible_ars"])
        if ("cash_disponible_ars" in keys and row["cash_disponible_ars"] is not None)
        else None,
        cash_disponible_usd=float(row["cash_disponible_usd"])
        if ("cash_disponible_usd" in keys and row["cash_disponible_usd"] is not None)
        else None,
    )


def latest_snapshot(conn: sqlite3.Connection) -> Optional[Snapshot]:
    row = conn.execute(
        """
        SELECT snapshot_date, total_value, currency, titles_value, cash_disponible_ars, cash_disponible_usd
        FROM portfolio_snapshots
        ORDER BY snapshot_date DESC
        LIMIT 1
        """
    ).fetchone()
    return _row_to_snapshot(row) if row else None


def earliest_snapshot(conn: sqlite3.Connection) -> Optional[Snapshot]:
    row = conn.execute(
        """
        SELECT snapshot_date, total_value, currency, titles_value, cash_disponible_ars, cash_disponible_usd
        FROM portfolio_snapshots
        ORDER BY snapshot_date ASC
        LIMIT 1
        """
    ).fetchone()
    return _row_to_snapshot(row) if row else None


def snapshot_before(conn: sqlite3.Connection, before_date: str) -> Optional[Snapshot]:
    row = conn.execute(
        """
        SELECT snapshot_date, total_value, currency, titles_value, cash_disponible_ars, cash_disponible_usd
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
        SELECT snapshot_date, total_value, currency, titles_value, cash_disponible_ars, cash_disponible_usd
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
        SELECT snapshot_date, total_value, currency, titles_value, cash_disponible_ars, cash_disponible_usd
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
        SELECT snapshot_date, total_value, currency, titles_value, cash_disponible_ars, cash_disponible_usd
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
        SELECT snapshot_date, total_value, currency, titles_value, cash_disponible_ars, cash_disponible_usd
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
