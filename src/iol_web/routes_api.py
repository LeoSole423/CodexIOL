from __future__ import annotations

import calendar
from datetime import date
from typing import Any, Dict, Optional

from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse

from . import db as dbmod
from .metrics import compute_return, target_date
from .movers import build_union_movers


router = APIRouter(prefix="/api")


def _parse_date(v: Optional[str]) -> Optional[str]:
    if v is None:
        return None
    v = v.strip()
    if not v:
        return None
    date.fromisoformat(v)
    return v


@router.get("/health")
def health() -> Dict[str, Any]:
    return {"ok": True}


@router.get("/snapshots")
def snapshots(date_from: Optional[str] = Query(None, alias="from"), date_to: Optional[str] = Query(None, alias="to")):
    try:
        f = _parse_date(date_from)
        t = _parse_date(date_to)
        conn = dbmod.get_conn()
    except FileNotFoundError:
        return []

    try:
        rows = dbmod.snapshots_series(conn, f, t)
        return [{"date": d, "total_value": v} for d, v in rows]
    finally:
        conn.close()


@router.get("/latest")
def latest():
    try:
        conn = dbmod.get_conn()
    except FileNotFoundError:
        return {"snapshot": None, "assets": [], "message": "DB not found"}
    try:
        snap = dbmod.latest_snapshot(conn)
        if not snap:
            return {"snapshot": None, "assets": [], "message": "No snapshots"}
        assets = dbmod.assets_for_snapshot(conn, snap.snapshot_date)
        assets.sort(key=lambda a: float(a.get("total_value") or 0.0), reverse=True)
        return {
            "snapshot": {
                "snapshot_date": snap.snapshot_date,
                "total_value": snap.total_value,
                "currency": snap.currency,
                "titles_value": snap.titles_value,
                "cash_disponible_ars": snap.cash_disponible_ars,
                "cash_disponible_usd": snap.cash_disponible_usd,
            },
            "assets": assets,
        }
    finally:
        conn.close()


@router.get("/returns")
def returns():
    try:
        conn = dbmod.get_conn()
    except FileNotFoundError:
        empty = compute_return(None, None).to_dict()
        return {"daily": empty, "weekly": empty, "monthly": empty, "yearly": empty, "ytd": empty}

    try:
        latest = dbmod.latest_snapshot(conn)
        if not latest:
            empty = compute_return(None, None).to_dict()
            return {"daily": empty, "weekly": empty, "monthly": empty, "yearly": empty, "ytd": empty}

        base_daily = dbmod.snapshot_before(conn, latest.snapshot_date)
        base_weekly = dbmod.snapshot_on_or_before(conn, target_date(latest.snapshot_date, 7))
        base_monthly = dbmod.snapshot_on_or_before(conn, target_date(latest.snapshot_date, 30))
        base_yearly = dbmod.snapshot_on_or_before(conn, target_date(latest.snapshot_date, 365))

        y = date.fromisoformat(latest.snapshot_date).year
        base_ytd = dbmod.first_snapshot_of_year(conn, y, latest.snapshot_date) or dbmod.earliest_snapshot(conn)

        return {
            "daily": compute_return(latest, base_daily).to_dict(),
            "weekly": compute_return(latest, base_weekly).to_dict(),
            "monthly": compute_return(latest, base_monthly).to_dict(),
            "yearly": compute_return(latest, base_yearly).to_dict(),
            "ytd": compute_return(latest, base_ytd).to_dict(),
        }
    finally:
        conn.close()


@router.get("/movers")
def movers(
    kind: str = "daily",
    limit: int = 10,
    period: Optional[str] = None,
    month: Optional[int] = None,
    year: Optional[int] = None,
):
    kind = (kind or "").strip().lower()
    if kind not in ("daily", "total", "period"):
        return JSONResponse(status_code=400, content={"error": "kind must be daily|total|period"})
    limit = int(limit)
    if limit < 1 or limit > 100:
        return JSONResponse(status_code=400, content={"error": "limit must be 1..100"})

    try:
        conn = dbmod.get_conn()
    except FileNotFoundError:
        return {"gainers": [], "losers": []}

    try:
        end_snap = dbmod.latest_snapshot(conn)
        if not end_snap:
            return {"gainers": [], "losers": []}
        end_assets = dbmod.assets_for_snapshot(conn, end_snap.snapshot_date)

        if kind == "period":
            p = (period or "daily").strip().lower()
            if p not in ("daily", "weekly", "monthly", "yearly", "ytd"):
                return JSONResponse(status_code=400, content={"error": "period must be daily|weekly|monthly|yearly|ytd"})

            if p == "daily":
                # Daily movers directly from IOL-provided daily variation on latest snapshot.
                enriched = []
                for a in end_assets:
                    cur_val = float(a.get("total_value") or 0.0)
                    pct = a.get("daily_var_pct")
                    try:
                        pct_f = float(pct) if pct is not None else None
                    except Exception:
                        pct_f = None
                    delta = None if pct_f is None else (cur_val * pct_f / 100.0)
                    aa = dict(a)
                    aa["base_total_value"] = None
                    aa["delta_value"] = delta
                    aa["delta_pct"] = pct_f
                    enriched.append(aa)

                def metric(a: Dict[str, Any]) -> float:
                    try:
                        return float(a.get("delta_value") or 0.0)
                    except Exception:
                        return 0.0

                gainers = sorted(enriched, key=metric, reverse=True)
                losers = sorted(enriched, key=metric, reverse=False)
                return {
                    "period": p,
                    "from": end_snap.snapshot_date,
                    "to": end_snap.snapshot_date,
                    "gainers": gainers[:limit],
                    "losers": losers[:limit],
                }

            latest_d = date.fromisoformat(end_snap.snapshot_date)
            latest_year = latest_d.year
            latest_month = latest_d.month

            base_snap = None
            period_end_snap = end_snap

            if p == "weekly":
                base_snap = dbmod.snapshot_on_or_before(conn, target_date(end_snap.snapshot_date, 7))
            elif p == "monthly":
                y = int(year) if year is not None else latest_year
                m = int(month) if month is not None else latest_month
                if m < 1 or m > 12:
                    return JSONResponse(status_code=400, content={"error": "month must be 1..12"})
                if y != latest_year:
                    return JSONResponse(status_code=400, content={"error": "monthly period only supports latest year"})
                last_day = calendar.monthrange(y, m)[1]
                start = f"{y:04d}-{m:02d}-01"
                end = f"{y:04d}-{m:02d}-{last_day:02d}"
                base_snap = dbmod.first_snapshot_in_range(conn, start, end)
                period_end_snap = dbmod.last_snapshot_in_range(conn, start, end)
            elif p == "yearly":
                y = int(year) if year is not None else latest_year
                start = f"{y:04d}-01-01"
                end = f"{y:04d}-12-31"
                base_snap = dbmod.first_snapshot_in_range(conn, start, end)
                period_end_snap = dbmod.last_snapshot_in_range(conn, start, end)
            else:  # ytd
                base_snap = dbmod.first_snapshot_of_year(conn, latest_year, end_snap.snapshot_date) or dbmod.earliest_snapshot(conn)

            if not base_snap or not period_end_snap:
                # For calendar periods, if there are no snapshots in that range we return null dates.
                if p in ("monthly", "yearly"):
                    return {"period": p, "from": None, "to": None, "gainers": [], "losers": []}
                return {"period": p, "from": None, "to": end_snap.snapshot_date, "gainers": [], "losers": []}

            base_assets = dbmod.assets_for_snapshot(conn, base_snap.snapshot_date)
            end_assets_period = dbmod.assets_for_snapshot(conn, period_end_snap.snapshot_date)
            enriched = build_union_movers(base_assets, end_assets_period)

            def metric(a: Dict[str, Any]) -> float:
                try:
                    return float(a.get("delta_value") or 0.0)
                except Exception:
                    return 0.0

            gainers = sorted(enriched, key=metric, reverse=True)
            losers = sorted(enriched, key=metric, reverse=False)
            return {
                "period": p,
                "from": base_snap.snapshot_date,
                "to": period_end_snap.snapshot_date,
                "gainers": gainers[:limit],
                "losers": losers[:limit],
            }

        key = "daily_var_points" if kind == "daily" else "gain_amount"

        def metric(a: Dict[str, Any]) -> float:
            v = a.get(key)
            if v is None:
                return 0.0
            try:
                return float(v)
            except Exception:
                return 0.0

        gainers = sorted(end_assets, key=metric, reverse=True)
        losers = sorted(end_assets, key=metric, reverse=False)
        return {"gainers": gainers[:limit], "losers": losers[:limit]}
    finally:
        conn.close()


@router.get("/allocation")
def allocation(group_by: str = "symbol", include_cash: int = 0):
    group_by = (group_by or "").strip().lower()
    include_cash = int(include_cash)
    if include_cash not in (0, 1):
        return JSONResponse(status_code=400, content={"error": "include_cash must be 0|1"})
    if group_by not in ("symbol", "type", "market", "currency"):
        return JSONResponse(status_code=400, content={"error": "group_by must be symbol|type|market|currency"})

    try:
        conn = dbmod.get_conn()
    except FileNotFoundError:
        return []

    try:
        snap = dbmod.latest_snapshot(conn)
        if not snap:
            return []
        items = [{"key": k, "value": v} for (k, v) in dbmod.allocation(conn, snap.snapshot_date, group_by=group_by)]
        if include_cash == 1 and snap.cash_disponible_ars is not None:
            items.append({"key": "Cash disponible (ARS)", "value": float(snap.cash_disponible_ars or 0.0)})
            items.sort(key=lambda kv: float(kv["value"] or 0.0), reverse=True)
        return items
    finally:
        conn.close()
