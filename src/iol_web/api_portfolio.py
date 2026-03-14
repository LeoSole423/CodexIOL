from __future__ import annotations

import calendar
from datetime import date
from typing import Any, Dict, Optional

from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse

from . import db as dbmod
from .metrics import target_date
from .movers import build_union_movers, build_union_movers_pnl


router = APIRouter(prefix="/api")


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
                "cash_total_ars": snap.cash_total_ars,
                "cash_disponible_ars": snap.cash_disponible_ars,
                "cash_disponible_usd": snap.cash_disponible_usd,
                "retrieved_at": snap.retrieved_at,
                "close_time": snap.close_time,
                "minutes_from_close": snap.minutes_from_close,
                "source": snap.source,
            },
            "assets": assets,
        }
    finally:
        conn.close()


def _filter_assets_by_currency(rows, currency: Optional[str]):
    if currency in ("all", None):
        return list(rows or [])
    if currency == "unknown":
        return [a for a in (rows or []) if (a.get("currency") in (None, ""))]
    return [a for a in (rows or []) if a.get("currency") == currency]


@router.get("/movers")
def movers(
    kind: str = "daily",
    limit: int = 10,
    period: Optional[str] = None,
    month: Optional[int] = None,
    year: Optional[int] = None,
    metric: str = "pnl",
    currency: str = "peso_Argentino",
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

            metric_norm = (metric or "pnl").strip().lower()
            if metric_norm not in ("pnl", "valuation"):
                return JSONResponse(status_code=400, content={"error": "metric must be pnl|valuation"})

            currency_norm = (currency or "peso_Argentino").strip()
            if not currency_norm:
                currency_norm = "peso_Argentino"

            if p == "daily":
                enriched = []
                for asset in _filter_assets_by_currency(end_assets, currency_norm):
                    cur_val = float(asset.get("total_value") or 0.0)
                    pct = asset.get("daily_var_pct")
                    try:
                        pct_f = float(pct) if pct is not None else None
                    except Exception:
                        pct_f = None
                    delta = None if pct_f is None else (cur_val * pct_f / 100.0)
                    row = dict(asset)
                    row["base_total_value"] = None
                    row["delta_value"] = delta
                    row["delta_pct"] = pct_f
                    enriched.append(row)

                def daily_metric(asset: Dict[str, Any]) -> float:
                    try:
                        return float(asset.get("delta_value") or 0.0)
                    except Exception:
                        return 0.0

                gainers = sorted(enriched, key=daily_metric, reverse=True)
                losers = sorted(enriched, key=daily_metric, reverse=False)
                return {
                    "period": p,
                    "from": end_snap.snapshot_date,
                    "to": end_snap.snapshot_date,
                    "metric": metric_norm,
                    "currency": currency_norm,
                    "warnings": [],
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
            else:
                base_snap = dbmod.first_snapshot_of_year(conn, latest_year, end_snap.snapshot_date) or dbmod.earliest_snapshot(conn)

            if not base_snap or not period_end_snap:
                if p in ("monthly", "yearly"):
                    return {
                        "period": p,
                        "from": None,
                        "to": None,
                        "metric": metric_norm,
                        "currency": currency_norm,
                        "warnings": [],
                        "gainers": [],
                        "losers": [],
                    }
                return {
                    "period": p,
                    "from": None,
                    "to": end_snap.snapshot_date,
                    "metric": metric_norm,
                    "currency": currency_norm,
                    "warnings": [],
                    "gainers": [],
                    "losers": [],
                }

            base_assets = _filter_assets_by_currency(dbmod.assets_for_snapshot(conn, base_snap.snapshot_date), currency_norm)
            end_assets_period = _filter_assets_by_currency(dbmod.assets_for_snapshot(conn, period_end_snap.snapshot_date), currency_norm)

            warnings = []
            orders_stats = None
            if metric_norm == "valuation":
                enriched = build_union_movers(base_assets, end_assets_period)
            else:
                dt_from = f"{base_snap.snapshot_date}T23:59:59"
                dt_to = f"{period_end_snap.snapshot_date}T23:59:59"
                cashflows, stats = dbmod.orders_cashflows_by_symbol(conn, dt_from, dt_to, currency=currency_norm)
                orders_stats = stats
                if stats.get("total", 0) == 0:
                    warnings.append("ORDERS_NONE")
                elif stats.get("unclassified", 0) > 0 or stats.get("amount_missing", 0) > 0:
                    warnings.append("ORDERS_INCOMPLETE")
                enriched = build_union_movers_pnl(base_assets, end_assets_period, cashflows)

            def period_metric(asset: Dict[str, Any]) -> float:
                try:
                    return float(asset.get("delta_value") or 0.0)
                except Exception:
                    return 0.0

            gainers = sorted(enriched, key=period_metric, reverse=True)
            losers = sorted(enriched, key=period_metric, reverse=False)
            return {
                "period": p,
                "from": base_snap.snapshot_date,
                "to": period_end_snap.snapshot_date,
                "metric": metric_norm,
                "currency": currency_norm,
                "warnings": warnings,
                "orders_stats": orders_stats,
                "gainers": gainers[:limit],
                "losers": losers[:limit],
            }

        key = "daily_var_points" if kind == "daily" else "gain_amount"

        def metric_value(asset: Dict[str, Any]) -> float:
            value = asset.get(key)
            if value is None:
                return 0.0
            try:
                return float(value)
            except Exception:
                return 0.0

        gainers = sorted(end_assets, key=metric_value, reverse=True)
        losers = sorted(end_assets, key=metric_value, reverse=False)
        return {"gainers": gainers[:limit], "losers": losers[:limit]}
    finally:
        conn.close()


@router.get("/assets/performance")
def assets_performance(
    period: str = Query(...),
    month: Optional[int] = None,
    year: Optional[int] = None,
):
    p = (period or "").strip().lower()
    if p not in ("daily", "weekly", "monthly", "yearly", "accumulated"):
        return JSONResponse(status_code=400, content={"error": "period must be daily|weekly|monthly|yearly|accumulated"})

    try:
        conn = dbmod.get_conn()
    except FileNotFoundError:
        return {"period": p, "from": None, "to": None, "warnings": [], "orders_stats": None, "rows": []}

    try:
        end_snap = dbmod.latest_snapshot(conn)
        if not end_snap:
            return {"period": p, "from": None, "to": None, "warnings": [], "orders_stats": None, "rows": []}

        latest_d = date.fromisoformat(end_snap.snapshot_date)
        latest_year = latest_d.year
        latest_month = latest_d.month

        base_snap = None
        period_end_snap = end_snap

        if p == "daily":
            pass
        elif p == "weekly":
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
        else:
            base_snap = dbmod.earliest_snapshot(conn)

        if p != "daily" and (not base_snap or not period_end_snap):
            if p in ("monthly", "yearly"):
                return {"period": p, "from": None, "to": None, "warnings": [], "orders_stats": None, "rows": []}
            return {
                "period": p,
                "from": None,
                "to": end_snap.snapshot_date,
                "warnings": [],
                "orders_stats": None,
                "rows": [],
            }

        rows = []
        warnings = []
        orders_stats = None

        if p == "daily":
            end_assets = dbmod.assets_for_snapshot(conn, end_snap.snapshot_date)
            for asset in end_assets:
                cur_val = float(asset.get("total_value") or 0.0)
                pct = asset.get("daily_var_pct")
                try:
                    pct_f = float(pct) if pct is not None else None
                except Exception:
                    pct_f = None
                selected = None if pct_f is None else (cur_val * pct_f / 100.0)
                rows.append(
                    {
                        "symbol": asset.get("symbol"),
                        "description": asset.get("description"),
                        "currency": asset.get("currency") or "unknown",
                        "market": asset.get("market"),
                        "type": asset.get("type"),
                        "total_value": cur_val,
                        "base_total_value": None,
                        "selected_value": selected,
                        "selected_pct": pct_f,
                        "flow_tag": "none",
                    }
                )
            from_date = end_snap.snapshot_date
            to_date = end_snap.snapshot_date
        else:
            base_assets = dbmod.assets_for_snapshot(conn, base_snap.snapshot_date)
            end_assets_period = dbmod.assets_for_snapshot(conn, period_end_snap.snapshot_date)
            dt_from = f"{base_snap.snapshot_date}T23:59:59"
            dt_to = f"{period_end_snap.snapshot_date}T23:59:59"
            cashflows, stats = dbmod.orders_cashflows_by_symbol(conn, dt_from, dt_to, currency="all")
            orders_stats = stats
            if stats.get("total", 0) == 0:
                warnings.append("ORDERS_NONE")
            elif stats.get("unclassified", 0) > 0 or stats.get("amount_missing", 0) > 0:
                warnings.append("ORDERS_INCOMPLETE")
            enriched = build_union_movers_pnl(base_assets, end_assets_period, cashflows)
            for row in enriched:
                rows.append(
                    {
                        "symbol": row.get("symbol"),
                        "description": row.get("description"),
                        "currency": row.get("currency") or "unknown",
                        "market": row.get("market"),
                        "type": row.get("type"),
                        "total_value": float(row.get("total_value") or 0.0),
                        "base_total_value": float(row.get("base_total_value") or 0.0),
                        "selected_value": row.get("delta_value"),
                        "selected_pct": row.get("delta_pct"),
                        "flow_tag": row.get("flow_tag") or "none",
                    }
                )
            from_date = base_snap.snapshot_date
            to_date = period_end_snap.snapshot_date

        total_visible = sum(float(row.get("total_value") or 0.0) for row in rows)
        for row in rows:
            value = float(row.get("total_value") or 0.0)
            row["weight_pct"] = ((value / total_visible) * 100.0) if total_visible > 0 else 0.0

        def sort_key(row: Dict[str, Any]):
            try:
                selected = float(row.get("selected_value") or 0.0)
            except Exception:
                selected = 0.0
            try:
                total = float(row.get("total_value") or 0.0)
            except Exception:
                total = 0.0
            symbol = str(row.get("symbol") or "")
            return (-selected, -total, symbol)

        rows.sort(key=sort_key)

        return {
            "period": p,
            "from": from_date,
            "to": to_date,
            "warnings": warnings,
            "orders_stats": orders_stats,
            "rows": rows,
        }
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
        items = [{"key": key, "value": value} for (key, value) in dbmod.allocation(conn, snap.snapshot_date, group_by=group_by)]
        if include_cash == 1:
            cash_v = snap.cash_total_ars if snap.cash_total_ars is not None else snap.cash_disponible_ars
            if cash_v is not None:
                items.append({"key": "Cash (ARS eq.)", "value": float(cash_v or 0.0)})
            items.sort(key=lambda item: float(item["value"] or 0.0), reverse=True)
        return items
    finally:
        conn.close()
