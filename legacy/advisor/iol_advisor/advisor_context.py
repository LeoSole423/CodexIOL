from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any, Dict, List, Optional, Tuple

from iol_shared.metrics import compute_return as shared_compute_return
from iol_shared.metrics import target_date as shared_target_date
from iol_shared.movers import build_union_movers as shared_build_union_movers
from iol_shared.portfolio_db import (
    allocation as shared_allocation,
    assets_for_snapshot as shared_assets_for_snapshot,
    connect_ro as shared_connect_ro,
    earliest_snapshot as shared_earliest_snapshot,
    first_snapshot_in_range as shared_first_snapshot_in_range,
    first_snapshot_of_year as shared_first_snapshot_of_year,
    last_snapshot_in_range as shared_last_snapshot_in_range,
    latest_snapshot as shared_latest_snapshot,
    snapshot_before as shared_snapshot_before,
    snapshot_on_or_before as shared_snapshot_on_or_before,
    snapshots_series as shared_snapshots_series,
)


def _connect_ro(db_path: str) -> sqlite3.Connection:
    return shared_connect_ro(db_path)


def _parse_iso_date(v: Optional[str]) -> Optional[str]:
    if v is None:
        return None
    v = str(v).strip()
    if not v:
        return None
    date.fromisoformat(v)
    return v


def _today_iso() -> str:
    return date.today().isoformat()


def _warn_snapshot_old(snapshot_date: Optional[str], max_age_days: int = 7) -> bool:
    if not snapshot_date:
        return False
    try:
        d = date.fromisoformat(snapshot_date)
    except Exception:
        return False
    return (date.today() - d).days > int(max_age_days)


def _safe_float(v: Any) -> Optional[float]:
    if v is None:
        return None
    try:
        return float(v)
    except Exception:
        return None


def _safe_str(v: Any) -> Optional[str]:
    if v is None:
        return None
    s = str(v)
    return s


def _pick_meta(base: Dict[str, Any] | None, end: Dict[str, Any] | None, key: str) -> Any:
    if end and end.get(key) not in (None, ""):
        return end.get(key)
    if base and base.get(key) not in (None, ""):
        return base.get(key)
    return None


def _snapshot_to_dict(snapshot: Any) -> Optional[Dict[str, Any]]:
    if snapshot is None:
        return None
    return {
        "snapshot_date": str(snapshot.snapshot_date),
        "total_value": float(snapshot.total_value or 0.0),
        "currency": snapshot.currency,
        "titles_value": snapshot.titles_value,
        "cash_disponible_ars": snapshot.cash_disponible_ars,
        "cash_disponible_usd": snapshot.cash_disponible_usd,
        "retrieved_at": snapshot.retrieved_at,
        "minutes_from_close": snapshot.minutes_from_close,
        "source": snapshot.source,
    }


def latest_snapshot(conn: sqlite3.Connection) -> Optional[Dict[str, Any]]:
    return _snapshot_to_dict(shared_latest_snapshot(conn))


def earliest_snapshot(conn: sqlite3.Connection) -> Optional[Dict[str, Any]]:
    return _snapshot_to_dict(shared_earliest_snapshot(conn))


def snapshot_before(conn: sqlite3.Connection, before_date: str) -> Optional[Dict[str, Any]]:
    return _snapshot_to_dict(shared_snapshot_before(conn, before_date))


def snapshot_on_or_before(conn: sqlite3.Connection, target_date: str) -> Optional[Dict[str, Any]]:
    return _snapshot_to_dict(shared_snapshot_on_or_before(conn, target_date))


def first_snapshot_of_year(conn: sqlite3.Connection, year: int, latest_date: str) -> Optional[Dict[str, Any]]:
    return _snapshot_to_dict(shared_first_snapshot_of_year(conn, year, latest_date))


def first_snapshot_in_range(conn: sqlite3.Connection, start_date: str, end_date: str) -> Optional[Dict[str, Any]]:
    return _snapshot_to_dict(shared_first_snapshot_in_range(conn, start_date, end_date))


def last_snapshot_in_range(conn: sqlite3.Connection, start_date: str, end_date: str) -> Optional[Dict[str, Any]]:
    return _snapshot_to_dict(shared_last_snapshot_in_range(conn, start_date, end_date))


def snapshots_series(conn: sqlite3.Connection, date_from: Optional[str], date_to: Optional[str]) -> List[Dict[str, Any]]:
    return [{"date": d, "total_value_ars": total} for d, total in shared_snapshots_series(conn, date_from, date_to)]


def assets_for_snapshot(conn: sqlite3.Connection, snapshot_date: str) -> List[Dict[str, Any]]:
    return shared_assets_for_snapshot(conn, snapshot_date)


def allocation(conn: sqlite3.Connection, snapshot_date: str, group_by: str) -> List[Dict[str, Any]]:
    rows = shared_allocation(conn, snapshot_date, (group_by or "").strip().lower())
    return [{"key": key, "value": value} for key, value in rows]


@dataclass(frozen=True)
class ReturnBlock:
    from_date: Optional[str]
    to_date: Optional[str]
    delta_ars: Optional[float]
    pct: Optional[float]

    def to_dict(self) -> Dict[str, Any]:
        return {"from": self.from_date, "to": self.to_date, "delta_ars": self.delta_ars, "pct": self.pct}


def compute_return(latest: Optional[Dict[str, Any]], base: Optional[Dict[str, Any]]) -> ReturnBlock:
    latest_ns = _dict_snapshot(latest)
    base_ns = _dict_snapshot(base)
    shared = shared_compute_return(latest_ns, base_ns)
    return ReturnBlock(
        from_date=shared.from_date,
        to_date=shared.to_date,
        delta_ars=shared.delta,
        pct=shared.pct,
    )


def target_date(latest_date: str, days: int) -> str:
    return shared_target_date(latest_date, int(days))


def build_union_movers(base_assets: List[Dict[str, Any]], end_assets: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return shared_build_union_movers(base_assets, end_assets)


def _dict_snapshot(v: Optional[Dict[str, Any]]) -> Any:
    if not v:
        return None

    class _SnapshotView:
        snapshot_date = str(v.get("snapshot_date"))
        total_value = float(v.get("total_value") or 0.0)
        titles_value = v.get("titles_value")

    return _SnapshotView()


def _daily_movers_from_latest_assets(assets: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    enriched: List[Dict[str, Any]] = []
    for a in assets or []:
        cur_val = float(a.get("total_value") or 0.0)
        pct = _safe_float(a.get("daily_var_pct"))
        delta = None if pct is None else (cur_val * pct / 100.0)
        aa = dict(a)
        aa["base_total_value"] = None
        aa["delta_value"] = delta
        aa["delta_pct"] = pct
        enriched.append(aa)
    return enriched


def _top_by_metric(rows: List[Dict[str, Any]], metric_key: str, limit: int, reverse: bool) -> List[Dict[str, Any]]:
    def metric(r: Dict[str, Any]) -> float:
        v = r.get(metric_key)
        if v is None:
            return 0.0
        try:
            return float(v)
        except Exception:
            return 0.0

    return sorted(rows or [], key=metric, reverse=reverse)[: int(limit)]


def _calendar_month_range(latest_date: str) -> Tuple[str, str, int, int]:
    d = date.fromisoformat(latest_date)
    y, m = d.year, d.month
    # Compute last day without importing calendar (keeps deps tiny).
    if m == 12:
        next_m = date(y + 1, 1, 1)
    else:
        next_m = date(y, m + 1, 1)
    last_day = (next_m - timedelta(days=1)).day
    start = f"{y:04d}-{m:02d}-01"
    end = f"{y:04d}-{m:02d}-{last_day:02d}"
    return start, end, y, m


def _calendar_year_range(latest_date: str) -> Tuple[str, str, int]:
    y = date.fromisoformat(latest_date).year
    start = f"{y:04d}-01-01"
    end = f"{y:04d}-12-31"
    return start, end, y


def _period_movers(conn: sqlite3.Connection, end_snap: Dict[str, Any], period: str, limit: int) -> Dict[str, Any]:
    end_date = str(end_snap["snapshot_date"])
    end_assets = assets_for_snapshot(conn, end_date)

    p = (period or "").strip().lower()
    if p == "daily":
        enriched = _daily_movers_from_latest_assets(end_assets)
        gainers = _top_by_metric(enriched, "delta_value", limit=limit, reverse=True)
        losers = _top_by_metric(enriched, "delta_value", limit=limit, reverse=False)
        return {"period": "daily", "from": end_date, "to": end_date, "gainers": gainers, "losers": losers}

    if p == "weekly":
        base_snap = snapshot_on_or_before(conn, target_date(end_date, 7))
        if not base_snap:
            return {"period": "weekly", "from": None, "to": end_date, "gainers": [], "losers": []}
        base_assets = assets_for_snapshot(conn, str(base_snap["snapshot_date"]))
        enriched = build_union_movers(base_assets, end_assets)
        gainers = _top_by_metric(enriched, "delta_value", limit=limit, reverse=True)
        losers = _top_by_metric(enriched, "delta_value", limit=limit, reverse=False)
        return {
            "period": "weekly",
            "from": str(base_snap["snapshot_date"]),
            "to": end_date,
            "gainers": gainers,
            "losers": losers,
        }

    if p == "monthly":
        start, end, _, _ = _calendar_month_range(end_date)
        base_snap = first_snapshot_in_range(conn, start, end)
        period_end_snap = last_snapshot_in_range(conn, start, end)
        if not base_snap or not period_end_snap:
            return {"period": "monthly", "from": None, "to": None, "gainers": [], "losers": []}
        base_assets = assets_for_snapshot(conn, str(base_snap["snapshot_date"]))
        end_assets_p = assets_for_snapshot(conn, str(period_end_snap["snapshot_date"]))
        enriched = build_union_movers(base_assets, end_assets_p)
        gainers = _top_by_metric(enriched, "delta_value", limit=limit, reverse=True)
        losers = _top_by_metric(enriched, "delta_value", limit=limit, reverse=False)
        return {
            "period": "monthly",
            "from": str(base_snap["snapshot_date"]),
            "to": str(period_end_snap["snapshot_date"]),
            "gainers": gainers,
            "losers": losers,
        }

    if p == "yearly":
        start, end, _ = _calendar_year_range(end_date)
        base_snap = first_snapshot_in_range(conn, start, end)
        period_end_snap = last_snapshot_in_range(conn, start, end)
        if not base_snap or not period_end_snap:
            return {"period": "yearly", "from": None, "to": None, "gainers": [], "losers": []}
        base_assets = assets_for_snapshot(conn, str(base_snap["snapshot_date"]))
        end_assets_p = assets_for_snapshot(conn, str(period_end_snap["snapshot_date"]))
        enriched = build_union_movers(base_assets, end_assets_p)
        gainers = _top_by_metric(enriched, "delta_value", limit=limit, reverse=True)
        losers = _top_by_metric(enriched, "delta_value", limit=limit, reverse=False)
        return {
            "period": "yearly",
            "from": str(base_snap["snapshot_date"]),
            "to": str(period_end_snap["snapshot_date"]),
            "gainers": gainers,
            "losers": losers,
        }

    if p == "ytd":
        y = date.fromisoformat(end_date).year
        base_snap = first_snapshot_of_year(conn, y, end_date) or earliest_snapshot(conn)
        if not base_snap:
            return {"period": "ytd", "from": None, "to": end_date, "gainers": [], "losers": []}
        base_assets = assets_for_snapshot(conn, str(base_snap["snapshot_date"]))
        enriched = build_union_movers(base_assets, end_assets)
        gainers = _top_by_metric(enriched, "delta_value", limit=limit, reverse=True)
        losers = _top_by_metric(enriched, "delta_value", limit=limit, reverse=False)
        return {
            "period": "ytd",
            "from": str(base_snap["snapshot_date"]),
            "to": end_date,
            "gainers": gainers,
            "losers": losers,
        }

    raise ValueError(f"invalid period: {period}")


def _orders_latest(conn: sqlite3.Connection, limit: int) -> List[Dict[str, Any]]:
    limit = int(limit)
    if limit <= 0:
        return []
    rows = conn.execute(
        """
        SELECT order_number, status, symbol, market, side, quantity, price, plazo, order_type, created_at, updated_at
        FROM orders
        ORDER BY COALESCE(updated_at, created_at) DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    return [dict(r) for r in rows]


def build_advisor_context(
    conn: sqlite3.Connection,
    as_of: Optional[str] = None,
    limit: int = 10,
    history_days: int = 365,
    include_cash: bool = True,
    include_orders: bool = False,
    orders_limit: int = 20,
) -> Dict[str, Any]:
    as_of = _parse_iso_date(as_of)
    limit = int(limit)
    history_days = int(history_days)

    warnings: List[str] = ["RETURNS_IGNORE_CASHFLOWS"]

    latest = latest_snapshot(conn)
    if not latest:
        return {
            "as_of": as_of or _today_iso(),
            "snapshot": None,
            "assets": {"count": 0, "rows": []},
            "history": {"days": history_days, "series_total_value_ars": []},
            "returns": {},
            "movers": {},
            "allocation": {},
            "orders": {"included": bool(include_orders), "limit": int(orders_limit) if include_orders else 0, "rows": []},
            "warnings": ["NO_SNAPSHOTS"] + warnings,
            "notes": {
                "returns_definition": "deltas de total_value (ARS) entre snapshots; no ajusta aportes/retiros",
                "movers_definition": "daily usa daily_var_pct; period usa delta de total_value por simbolo entre snapshots",
            },
        }

    selected = snapshot_on_or_before(conn, as_of) if as_of else latest
    if not selected:
        # as_of earlier than the earliest snapshot: still return something useful.
        earliest = earliest_snapshot(conn)
        selected = earliest or latest
        warnings.append("AS_OF_OUT_OF_RANGE")

    snap_date = str(selected["snapshot_date"])
    if selected.get("currency") == "mixed":
        warnings.append("MIXED_CURRENCY")
    if _warn_snapshot_old(snap_date):
        warnings.append("SNAPSHOT_OLD")

    assets = assets_for_snapshot(conn, snap_date)
    assets_sorted_by_value = sorted(assets, key=lambda a: float(a.get("total_value") or 0.0), reverse=True)

    # Returns are computed relative to the selected snapshot date (not necessarily "latest").
    base_daily = snapshot_before(conn, snap_date)
    base_weekly = snapshot_on_or_before(conn, target_date(snap_date, 7))
    base_monthly = snapshot_on_or_before(conn, target_date(snap_date, 30))
    base_yearly = snapshot_on_or_before(conn, target_date(snap_date, 365))
    y = date.fromisoformat(snap_date).year
    base_ytd = first_snapshot_of_year(conn, y, snap_date) or earliest_snapshot(conn)

    returns = {
        "daily": compute_return(selected, base_daily).to_dict(),
        "weekly": compute_return(selected, base_weekly).to_dict(),
        "monthly": compute_return(selected, base_monthly).to_dict(),
        "yearly": compute_return(selected, base_yearly).to_dict(),
        "ytd": compute_return(selected, base_ytd).to_dict(),
    }

    # History series: last N days ending at as_of snapshot date (inclusive).
    hist_from = None
    if history_days and history_days > 0:
        hist_from = target_date(snap_date, history_days)
    history_series = snapshots_series(conn, hist_from, snap_date)

    # Movers:
    # - daily/weekly/monthly/yearly/ytd are computed anchored at the selected snapshot date
    #   (monthly/yearly use calendar ranges for the selected month/year).
    movers: Dict[str, Any] = {}
    movers["daily"] = _period_movers(conn, selected, "daily", limit=limit)
    movers["weekly"] = _period_movers(conn, selected, "weekly", limit=limit)
    movers["monthly"] = _period_movers(conn, selected, "monthly", limit=limit)
    movers["yearly"] = _period_movers(conn, selected, "yearly", limit=limit)
    movers["ytd"] = _period_movers(conn, selected, "ytd", limit=limit)
    movers["total_unrealized"] = {
        "metric": "gain_amount",
        "gainers": _top_by_metric(assets, "gain_amount", limit=limit, reverse=True),
        "losers": _top_by_metric(assets, "gain_amount", limit=limit, reverse=False),
    }

    alloc: Dict[str, Any] = {
        "by_symbol": allocation(conn, snap_date, "symbol"),
        "by_type": allocation(conn, snap_date, "type"),
        "by_market": allocation(conn, snap_date, "market"),
        "by_currency": allocation(conn, snap_date, "currency"),
        "include_cash_ars": bool(include_cash),
    }
    if include_cash and selected.get("cash_disponible_ars") is not None:
        alloc["by_symbol_with_cash"] = sorted(
            alloc["by_symbol"] + [{"key": "Cash disponible (ARS)", "value": float(selected.get("cash_disponible_ars") or 0.0)}],
            key=lambda kv: float(kv.get("value") or 0.0),
            reverse=True,
        )

    orders_rows = _orders_latest(conn, orders_limit) if include_orders else []

    # Keep a stable, LLM-friendly top-level shape.
    return {
        "as_of": snap_date,
        "snapshot": {
            "snapshot_date": snap_date,
            "total_value_ars": float(selected.get("total_value") or 0.0),
            "titles_value": selected.get("titles_value"),
            "cash_disponible_ars": selected.get("cash_disponible_ars"),
            "cash_disponible_usd": selected.get("cash_disponible_usd"),
            "currency_hint": selected.get("currency"),
            "retrieved_at_utc": selected.get("retrieved_at"),
            "minutes_from_close": selected.get("minutes_from_close"),
            "source": selected.get("source"),
        },
        "assets": {
            "count": len(assets),
            "rows": assets_sorted_by_value,
            "top_by_value": assets_sorted_by_value[: int(limit)],
            "top_by_gain_amount": _top_by_metric(assets, "gain_amount", limit=limit, reverse=True),
            "top_by_daily_points": _top_by_metric(assets, "daily_var_points", limit=limit, reverse=True),
            "top_losers_by_daily_points": _top_by_metric(assets, "daily_var_points", limit=limit, reverse=False),
        },
        "history": {"days": history_days, "series_total_value_ars": history_series},
        "returns": returns,
        "movers": movers,
        "allocation": alloc,
        "orders": {"included": bool(include_orders), "limit": int(orders_limit) if include_orders else 0, "rows": orders_rows},
        "warnings": warnings,
        "notes": {
            "returns_definition": "deltas de total_value (ARS) entre snapshots; no ajusta aportes/retiros",
            "movers_definition": "daily usa daily_var_pct; period usa delta de total_value por simbolo entre snapshots",
        },
    }


def build_advisor_context_from_db_path(
    db_path: str,
    as_of: Optional[str] = None,
    limit: int = 10,
    history_days: int = 365,
    include_cash: bool = True,
    include_orders: bool = False,
    orders_limit: int = 20,
) -> Dict[str, Any]:
    try:
        conn = _connect_ro(db_path)
    except FileNotFoundError:
        return {
            "as_of": _parse_iso_date(as_of) or _today_iso(),
            "snapshot": None,
            "assets": {"count": 0, "rows": []},
            "history": {"days": int(history_days), "series_total_value_ars": []},
            "returns": {},
            "movers": {},
            "allocation": {},
            "orders": {"included": bool(include_orders), "limit": int(orders_limit) if include_orders else 0, "rows": []},
            "warnings": ["DB_NOT_FOUND", "RETURNS_IGNORE_CASHFLOWS"],
            "notes": {
                "returns_definition": "deltas de total_value (ARS) entre snapshots; no ajusta aportes/retiros",
                "movers_definition": "daily usa daily_var_pct; period usa delta de total_value por simbolo entre snapshots",
            },
        }
    try:
        return build_advisor_context(
            conn,
            as_of=as_of,
            limit=limit,
            history_days=history_days,
            include_cash=include_cash,
            include_orders=include_orders,
            orders_limit=orders_limit,
        )
    finally:
        conn.close()


def render_advisor_context_md(ctx: Dict[str, Any]) -> str:
    snap = (ctx or {}).get("snapshot") or {}
    assets = (ctx or {}).get("assets") or {}
    returns = (ctx or {}).get("returns") or {}
    warnings = (ctx or {}).get("warnings") or []

    def _fmt_money(v: Any) -> str:
        f = _safe_float(v)
        if f is None:
            return "-"
        # Keep it simple: avoid locale dependencies.
        return f"{f:,.0f} ARS".replace(",", ".")

    def _fmt_pct(v: Any) -> str:
        f = _safe_float(v)
        if f is None:
            return "-"
        return f"{f:+.2f}%"

    lines: List[str] = []
    lines.append(f"# Advisor context ({ctx.get('as_of') if ctx else '-'})")
    if warnings:
        lines.append("")
        lines.append("Warnings:")
        for w in warnings:
            lines.append(f"- {w}")

    lines.append("")
    lines.append("## Snapshot")
    lines.append(f"- Date: {snap.get('snapshot_date', '-')}")
    lines.append(f"- Total: {_fmt_money(snap.get('total_value_ars'))}")
    lines.append(f"- Cash ARS: {_fmt_money(snap.get('cash_disponible_ars'))}")
    lines.append(f"- Cash USD: {snap.get('cash_disponible_usd', '-')}")

    lines.append("")
    lines.append("## Returns")
    for k in ("daily", "weekly", "monthly", "ytd", "yearly"):
        b = returns.get(k) or {}
        lines.append(f"- {k}: {_fmt_pct(b.get('pct'))} ({_fmt_money(b.get('delta_ars'))})")

    lines.append("")
    lines.append("## Top assets by value")
    for r in (assets.get("top_by_value") or [])[:10]:
        lines.append(f"- {r.get('symbol', '-')}: {_fmt_money(r.get('total_value'))}")

    return "\n".join(lines) + "\n"
