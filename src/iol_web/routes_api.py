from __future__ import annotations

import calendar
from datetime import date
from typing import Any, Dict, Optional

from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse

from . import db as dbmod
from .inflation_ar import get_inflation_series
from .inflation_compare import compounded_inflation_pct, inflation_factor_for_date, month_key
from .metrics import compute_daily_return_from_assets, compute_return, target_date
from .movers import build_union_movers, build_union_movers_pnl


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


def _add_months(year: int, month: int, delta_months: int) -> tuple[int, int]:
    # month: 1..12
    n = year * 12 + (month - 1) + int(delta_months)
    y = n // 12
    m = (n % 12) + 1
    return int(y), int(m)


def _month_range_end_date(year: int, month: int) -> str:
    last_day = calendar.monthrange(int(year), int(month))[1]
    return f"{int(year):04d}-{int(month):02d}-{int(last_day):02d}"


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

        daily_block = None
        if base_daily:
            daily_block = compute_return(latest, base_daily)
        else:
            # If there is only one snapshot in the DB, returns between snapshots can't be computed.
            # Still show a useful daily delta using IOL-provided per-asset daily variation.
            assets = dbmod.assets_for_snapshot(conn, latest.snapshot_date)
            daily_block = compute_daily_return_from_assets(latest, assets)

        return {
            "daily": daily_block.to_dict(),
            "weekly": compute_return(latest, base_weekly).to_dict(),
            "monthly": compute_return(latest, base_monthly).to_dict(),
            "yearly": compute_return(latest, base_yearly).to_dict(),
            "ytd": compute_return(latest, base_ytd).to_dict(),
        }
    finally:
        conn.close()


@router.get("/inflation")
def inflation(date_from: Optional[str] = Query(None, alias="from"), date_to: Optional[str] = Query(None, alias="to")):
    """
    Argentina monthly inflation (IPC INDEC) as percentage, by month.

    Uses datos.gob.ar series API with local cache (see src/iol_web/inflation_ar.py).
    """
    f = _parse_date(date_from)
    t = _parse_date(date_to)
    try:
        res = get_inflation_series(start_date=f, end_date=t)
    except Exception as e:
        return JSONResponse(status_code=502, content={"error": f"failed to fetch inflation: {type(e).__name__}"})

    months = []
    for d, v in res.data or []:
        try:
            pct = float(v) * 100.0
        except Exception:
            pct = None
        months.append({"month": str(d)[:7], "date": str(d), "inflation_pct": pct})

    dates = [d for d, _ in (res.data or [])]
    available_from = min(dates)[:7] if dates else None
    available_to = max(dates)[:7] if dates else None

    return {
        "series_id": res.series_id,
        "source": res.source,
        "stale": bool(res.stale),
        "fetched_at": res.fetched_at,
        "available_from": available_from,
        "available_to": available_to,
        "months": months,
    }


@router.get("/compare/inflation")
def compare_inflation(months: int = 12):
    """
    Compare portfolio monthly return (calendar month) vs Argentina inflation (IPC INDEC).
    """
    months = int(months)
    if months < 1 or months > 120:
        return JSONResponse(status_code=400, content={"error": "months must be 1..120"})

    try:
        conn = dbmod.get_conn()
    except FileNotFoundError:
        return {"stale": False, "rows": []}

    try:
        latest = dbmod.latest_snapshot(conn)
        if not latest:
            return {"stale": False, "rows": []}

        latest_d = date.fromisoformat(latest.snapshot_date)
        y_to = latest_d.year
        m_to = latest_d.month
        y_from, m_from = _add_months(y_to, m_to, -(months - 1))

        date_from = f"{y_from:04d}-{m_from:02d}-01"
        date_to = _month_range_end_date(y_to, m_to)

        monthly = dbmod.monthly_first_last_series(conn, date_from=date_from, date_to=date_to)
        if not monthly:
            return {"stale": False, "rows": []}

        infl_start = f"{y_from:04d}-{m_from:02d}-01"
        infl_end = f"{y_to:04d}-{m_to:02d}-01"
        infl = get_inflation_series(start_date=infl_start, end_date=infl_end)
        infl_pct = infl.inflation_pct_by_month()
        infl_dates = [d for d, _ in (infl.data or [])]
        infl_available_from = min(infl_dates)[:7] if infl_dates else None
        infl_available_to = max(infl_dates)[:7] if infl_dates else None

        # Projection: until INDEC publishes the current month, use the last available month inflation as estimate.
        last_known_inflation_pct = None
        if infl_pct:
            try:
                last_month = sorted(infl_pct.keys())[-1]
                last_known_inflation_pct = float(infl_pct.get(last_month))
            except Exception:
                last_known_inflation_pct = None

        rows = []
        projected_count = 0
        for r in monthly:
            month = r["month"]
            first_date = r["first_date"]
            last_date = r["last_date"]
            first_v = float(r["first_value"] or 0.0)
            last_v = float(r["last_value"] or 0.0)

            portfolio_pct = None
            if first_date != last_date and first_v != 0.0:
                portfolio_pct = (last_v / first_v - 1.0) * 100.0

            inflation_projected = False
            inflation_pct = infl_pct.get(month)
            if (
                inflation_pct is None
                and last_known_inflation_pct is not None
                and infl_available_to is not None
                and month > infl_available_to
            ):
                inflation_pct = last_known_inflation_pct
                inflation_projected = True
                projected_count += 1
            real_pct = None
            if portfolio_pct is not None and inflation_pct is not None:
                try:
                    real_pct = ((1.0 + portfolio_pct / 100.0) / (1.0 + inflation_pct / 100.0) - 1.0) * 100.0
                except Exception:
                    real_pct = None

            rows.append(
                {
                    "month": month,
                    "from": first_date,
                    "to": last_date,
                    "portfolio_pct": portfolio_pct,
                    "inflation_pct": inflation_pct,
                    "inflation_projected": inflation_projected,
                    "real_pct": real_pct,
                }
            )

        return {
            "stale": bool(infl.stale),
            "inflation_available_from": infl_available_from,
            "inflation_available_to": infl_available_to,
            "projection_used": projected_count > 0,
            "projection_source_month": infl_available_to,
            "projection_inflation_pct": last_known_inflation_pct,
            "projection_months": projected_count,
            "rows": rows,
        }
    finally:
        conn.close()


@router.get("/compare/inflation/series")
def compare_inflation_series(
    date_from: Optional[str] = Query(None, alias="from"),
    date_to: Optional[str] = Query(None, alias="to"),
):
    """
    Time series of portfolio vs inflation (step-monthly) using the same snapshot dates.

    Returns both value series (ARS) and index series (base 100).
    """
    try:
        f = _parse_date(date_from)
        t = _parse_date(date_to)
        conn = dbmod.get_conn()
    except FileNotFoundError:
        return {"stale": False, "labels": [], "portfolio_value": [], "inflation_value": [], "portfolio_index": [], "inflation_index": []}

    try:
        latest = dbmod.latest_snapshot(conn)
        earliest = dbmod.earliest_snapshot(conn)
        if not latest or not earliest:
            return {"stale": False, "labels": [], "portfolio_value": [], "inflation_value": [], "portfolio_index": [], "inflation_index": []}

        f2 = f or earliest.snapshot_date
        t2 = t or latest.snapshot_date
        series = dbmod.snapshots_series(conn, f2, t2)
        if not series:
            return {"stale": False, "labels": [], "portfolio_value": [], "inflation_value": [], "portfolio_index": [], "inflation_index": []}

        base_date = series[0][0]
        base_value = float(series[0][1] or 0.0)

        # Fetch inflation for the months covering the series range.
        # Query monthly points by first-of-month dates. Include at least the previous year so we can
        # project from the last known inflation even if the series range starts after the latest IPC published.
        y0 = max(2017, date.fromisoformat(str(base_date)).year - 1)
        infl_start = f"{y0:04d}-01-01"
        infl_end = f"{month_key(t2)}-01"
        infl = get_inflation_series(start_date=infl_start, end_date=infl_end)
        infl_pct = infl.inflation_pct_by_month()
        infl_dates = [d for d, _ in (infl.data or [])]
        infl_available_to = max(infl_dates)[:7] if infl_dates else None

        # Projection: last known month inflation can be used as estimate for months after available_to.
        last_known_inflation_pct = None
        if infl_pct:
            try:
                last_month = sorted(infl_pct.keys())[-1]
                last_known_inflation_pct = float(infl_pct.get(last_month))
            except Exception:
                last_known_inflation_pct = None

        labels = []
        p_vals = []
        i_vals = []
        p_idx = []
        i_idx = []
        projected = 0

        for d, v in series:
            d_s = str(d)
            cur_v = float(v or 0.0)
            labels.append(d_s)
            p_vals.append(cur_v)
            if base_value:
                p_idx.append(cur_v / base_value * 100.0)
            else:
                p_idx.append(None)

            proj_month = None
            if infl_available_to and month_key(d_s) > infl_available_to and last_known_inflation_pct is not None:
                proj_month = month_key(d_s)

            factor = inflation_factor_for_date(
                base_date=base_date,
                target_date=d_s,
                infl_pct_by_month=infl_pct,
                projection_month=proj_month,
                projection_pct=last_known_inflation_pct if proj_month else None,
            )
            if factor is None or not base_value:
                i_vals.append(None)
                i_idx.append(None)
            else:
                # Only count projection as "used" when target month differs from the base month;
                # within the base month the step-monthly model applies no inflation yet.
                if proj_month and month_key(d_s) != month_key(base_date):
                    projected += 1
                i_vals.append(base_value * factor)
                i_idx.append(factor * 100.0)

        return {
            "stale": bool(infl.stale),
            "inflation_available_to": infl_available_to,
            "projection_used": projected > 0,
            "labels": labels,
            "portfolio_value": p_vals,
            "inflation_value": i_vals,
            "portfolio_index": p_idx,
            "inflation_index": i_idx,
        }
    finally:
        conn.close()


@router.get("/compare/inflation/annual")
def compare_inflation_annual(years: int = 10):
    """
    Annual (calendar year) portfolio return vs compounded inflation for the same snapshot interval.

    Adds a YTD row for the latest year.
    """
    years = int(years)
    if years < 1 or years > 50:
        return JSONResponse(status_code=400, content={"error": "years must be 1..50"})

    try:
        conn = dbmod.get_conn()
    except FileNotFoundError:
        return {"stale": False, "rows": []}

    try:
        latest = dbmod.latest_snapshot(conn)
        earliest = dbmod.earliest_snapshot(conn)
        if not latest or not earliest:
            return {"stale": False, "rows": []}

        latest_d = date.fromisoformat(latest.snapshot_date)
        latest_year = latest_d.year
        earliest_year = date.fromisoformat(earliest.snapshot_date).year

        start_year = max(earliest_year, latest_year - years + 1)

        # Include at least one year before the start year so we have "last known" inflation for projection.
        infl_start = f"{max(2017, start_year - 1):04d}-01-01"
        infl_end = f"{latest_year:04d}-{latest_d.month:02d}-01"
        infl = get_inflation_series(start_date=infl_start, end_date=infl_end)
        infl_pct = infl.inflation_pct_by_month()
        infl_dates = [d for d, _ in (infl.data or [])]
        infl_available_to = max(infl_dates)[:7] if infl_dates else None

        last_known_inflation_pct = None
        if infl_pct:
            try:
                last_month = sorted(infl_pct.keys())[-1]
                last_known_inflation_pct = float(infl_pct.get(last_month))
            except Exception:
                last_known_inflation_pct = None

        rows = []
        projection_used = False

        def _calc_row(label: str, from_snap: dbmod.Snapshot, to_snap: dbmod.Snapshot, partial: bool) -> Dict[str, Any]:
            nonlocal projection_used
            base_v = float(from_snap.total_value or 0.0)
            end_v = float(to_snap.total_value or 0.0)
            portfolio_pct = None if base_v == 0.0 else (end_v / base_v - 1.0) * 100.0

            proj_month = None
            if infl_available_to and last_known_inflation_pct is not None and month_key(to_snap.snapshot_date) > infl_available_to:
                proj_month = month_key(to_snap.snapshot_date)

            infl_pct_comp, _, projected = compounded_inflation_pct(
                from_date=from_snap.snapshot_date,
                to_date=to_snap.snapshot_date,
                infl_pct_by_month=infl_pct,
                projection_month=proj_month,
                projection_pct=last_known_inflation_pct if proj_month else None,
            )
            if projected:
                projection_used = True

            real_pct = None
            if portfolio_pct is not None and infl_pct_comp is not None:
                try:
                    real_pct = ((1.0 + portfolio_pct / 100.0) / (1.0 + infl_pct_comp / 100.0) - 1.0) * 100.0
                except Exception:
                    real_pct = None

            return {
                "label": label,
                "from": from_snap.snapshot_date,
                "to": to_snap.snapshot_date,
                "portfolio_pct": portfolio_pct,
                "inflation_pct": infl_pct_comp,
                "real_pct": real_pct,
                "partial": bool(partial),
                "inflation_projected": bool(projected),
            }

        for y in range(start_year, latest_year + 1):
            start = f"{y:04d}-01-01"
            end = f"{y:04d}-12-31"
            from_snap = dbmod.first_snapshot_in_range(conn, start, end)
            to_snap = dbmod.last_snapshot_in_range(conn, start, end)
            if not from_snap or not to_snap:
                continue
            partial = (from_snap.snapshot_date != start) or (to_snap.snapshot_date != end)
            row = _calc_row(str(y), from_snap, to_snap, partial=partial)
            if row:
                rows.append(row)

        # YTD row
        y_start_snap = dbmod.first_snapshot_of_year(conn, latest_year, latest.snapshot_date) or earliest
        if y_start_snap:
            ytd_label = f"YTD {latest_year}"
            partial_ytd = True
            row = _calc_row(ytd_label, y_start_snap, latest, partial=partial_ytd)
            if row:
                rows.append(row)

        return {
            "stale": bool(infl.stale),
            "inflation_available_to": infl_available_to,
            "projection_used": bool(projection_used),
            "rows": rows,
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

            def _filter_assets(rows):
                if currency_norm in ("all", None):
                    return list(rows or [])
                if currency_norm == "unknown":
                    return [a for a in (rows or []) if (a.get("currency") in (None, ""))]
                return [a for a in (rows or []) if a.get("currency") == currency_norm]

            if p == "daily":
                # Daily movers directly from IOL-provided daily variation on latest snapshot.
                enriched = []
                for a in _filter_assets(end_assets):
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
            else:  # ytd
                base_snap = dbmod.first_snapshot_of_year(conn, latest_year, end_snap.snapshot_date) or dbmod.earliest_snapshot(conn)

            if not base_snap or not period_end_snap:
                # For calendar periods, if there are no snapshots in that range we return null dates.
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

            base_assets = _filter_assets(dbmod.assets_for_snapshot(conn, base_snap.snapshot_date))
            end_assets_period = _filter_assets(dbmod.assets_for_snapshot(conn, period_end_snap.snapshot_date))

            warnings = []
            orders_stats = None
            if metric_norm == "valuation":
                enriched = build_union_movers(base_assets, end_assets_period)
            else:
                dt_from = f"{base_snap.snapshot_date}T00:00:00"
                dt_to = f"{period_end_snap.snapshot_date}T23:59:59"
                cashflows, stats = dbmod.orders_cashflows_by_symbol(conn, dt_from, dt_to, currency=currency_norm)
                orders_stats = stats
                if stats.get("total", 0) == 0:
                    warnings.append("ORDERS_NONE")
                elif stats.get("classified", 0) == 0 or stats.get("unclassified", 0) > 0 or stats.get("amount_missing", 0) > 0:
                    warnings.append("ORDERS_INCOMPLETE")
                enriched = build_union_movers_pnl(base_assets, end_assets_period, cashflows)

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
                "metric": metric_norm,
                "currency": currency_norm,
                "warnings": warnings,
                "orders_stats": orders_stats,
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
        if include_cash == 1:
            # Prefer "total cash in ARS terms" derived from totalEnPesos - titles_value.
            cash_v = snap.cash_total_ars if snap.cash_total_ars is not None else snap.cash_disponible_ars
            if cash_v is not None:
                items.append({"key": "Cash (ARS eq.)", "value": float(cash_v or 0.0)})
            items.sort(key=lambda kv: float(kv["value"] or 0.0), reverse=True)
        return items
    finally:
        conn.close()
