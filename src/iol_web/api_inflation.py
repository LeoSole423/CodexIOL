from __future__ import annotations

import calendar
from datetime import date
from typing import Any, Callable, Dict, Optional, Tuple

from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse

from . import db as dbmod
from .inflation_compare import compounded_inflation_pct, inflation_factor_for_date, month_key


def _add_months(year: int, month: int, delta_months: int) -> tuple[int, int]:
    n = year * 12 + (month - 1) + int(delta_months)
    y = n // 12
    m = (n % 12) + 1
    return int(y), int(m)


def _month_range_end_date(year: int, month: int) -> str:
    last_day = calendar.monthrange(int(year), int(month))[1]
    return f"{int(year):04d}-{int(month):02d}-{int(last_day):02d}"


def _monthly_vs_inflation_payload(
    month: Optional[str],
    from_date: Optional[str],
    to_date: Optional[str],
    status: str,
    market_pct: Optional[float] = None,
    market_delta_ars: Optional[float] = None,
    contributions_ars: Optional[float] = None,
    net_pct: Optional[float] = None,
    net_delta_ars: Optional[float] = None,
    inflation_pct: Optional[float] = None,
    inflation_projected: bool = False,
    real_vs_inflation_pct: Optional[float] = None,
    beats_inflation: Optional[bool] = None,
    quality_warnings: Optional[list[str]] = None,
    inflation_available_to: Optional[str] = None,
    flow_confidence: Optional[str] = None,
    data_freshness: Optional[Dict[str, Any]] = None,
    orders_coverage: Optional[Dict[str, Any]] = None,
    movements_coverage: Optional[Dict[str, Any]] = None,
    estimated: Optional[bool] = None,
) -> Dict[str, Any]:
    return {
        "month": month,
        "from": from_date,
        "to": to_date,
        "market_pct": market_pct,
        "market_delta_ars": market_delta_ars,
        "contributions_ars": contributions_ars,
        "net_pct": net_pct,
        "net_delta_ars": net_delta_ars,
        "inflation_pct": inflation_pct,
        "inflation_projected": bool(inflation_projected),
        "real_vs_inflation_pct": real_vs_inflation_pct,
        "beats_inflation": beats_inflation,
        "quality_warnings": list(quality_warnings or []),
        "inflation_available_to": inflation_available_to,
        "flow_confidence": flow_confidence,
        "data_freshness": data_freshness,
        "orders_coverage": orders_coverage,
        "movements_coverage": movements_coverage,
        "estimated": bool(estimated) if estimated is not None else False,
        "status": status,
    }


def build_inflation_router(
    *,
    parse_date: Callable[[Optional[str]], Optional[str]],
    compute_return: Callable[[Any, Any], Dict[str, Any]],
    return_with_flows: Callable[[Any, Any, Any, Dict[str, Any]], Dict[str, Any]],
    get_inflation_series: Callable[..., Any],
) -> Tuple[APIRouter, Callable[..., Any], Callable[..., Any], Callable[..., Any], Callable[..., Any], Callable[..., Any]]:
    router = APIRouter(prefix="/api")

    @router.get("/inflation")
    def inflation(date_from: Optional[str] = Query(None, alias="from"), date_to: Optional[str] = Query(None, alias="to")):
        f = parse_date(date_from)
        t = parse_date(date_to)
        try:
            res = get_inflation_series(start_date=f, end_date=t)
        except Exception as exc:
            return JSONResponse(status_code=502, content={"error": f"failed to fetch inflation: {type(exc).__name__}"})

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

    @router.get("/kpi/monthly-vs-inflation")
    def kpi_monthly_vs_inflation():
        try:
            conn = dbmod.get_conn()
        except FileNotFoundError:
            return _monthly_vs_inflation_payload(month=None, from_date=None, to_date=None, status="insufficient_snapshots")

        try:
            latest = dbmod.latest_snapshot(conn)
            if not latest:
                return _monthly_vs_inflation_payload(month=None, from_date=None, to_date=None, status="insufficient_snapshots")

            latest_d = date.fromisoformat(latest.snapshot_date)
            month = f"{latest_d.year:04d}-{latest_d.month:02d}"
            month_start = f"{latest_d.year:04d}-{latest_d.month:02d}-01"
            from_snap_in_month = dbmod.first_snapshot_in_range(conn, month_start, latest.snapshot_date)
            from_snap = from_snap_in_month
            if not from_snap_in_month or from_snap_in_month.snapshot_date == latest.snapshot_date:
                from_snap = dbmod.snapshot_on_or_before(conn, month_start)
            if not from_snap or from_snap.snapshot_date == latest.snapshot_date:
                return _monthly_vs_inflation_payload(
                    month=month,
                    from_date=from_snap.snapshot_date if from_snap else None,
                    to_date=latest.snapshot_date,
                    status="insufficient_snapshots",
                )

            gross = compute_return(latest, from_snap)
            with_flows = return_with_flows(conn, latest, from_snap, gross)

            market_pct = with_flows.get("pct")
            market_delta_ars = with_flows.get("delta")
            contributions_ars = with_flows.get("flow_total_ars")
            net_pct = with_flows.get("real_pct")
            net_delta_ars = with_flows.get("real_delta")
            quality_warnings = with_flows.get("quality_warnings") or []
            flow_confidence = with_flows.get("flow_confidence")
            data_freshness = with_flows.get("data_freshness")
            orders_coverage = with_flows.get("orders_coverage")
            movements_coverage = with_flows.get("movements_coverage")
            estimated = with_flows.get("estimated")

            inflation_available_to = None
            inflation_pct = None
            inflation_projected = False
            try:
                infl_start = f"{max(2017, latest_d.year - 1):04d}-01-01"
                infl_end = f"{latest_d.year:04d}-{latest_d.month:02d}-01"
                infl = get_inflation_series(start_date=infl_start, end_date=infl_end)
                infl_pct = infl.inflation_pct_by_month()
                infl_dates = [d for d, _ in (infl.data or [])]
                inflation_available_to = max(infl_dates)[:7] if infl_dates else None

                inflation_pct = infl_pct.get(month)
                last_known_inflation_pct = None
                if infl_pct:
                    try:
                        last_month = sorted(infl_pct.keys())[-1]
                        last_known_inflation_pct = float(infl_pct.get(last_month))
                    except Exception:
                        last_known_inflation_pct = None

                if (
                    inflation_pct is None
                    and last_known_inflation_pct is not None
                    and inflation_available_to is not None
                    and month > inflation_available_to
                ):
                    inflation_pct = last_known_inflation_pct
                    inflation_projected = True
            except Exception:
                inflation_pct = None

            if inflation_pct is None:
                return _monthly_vs_inflation_payload(
                    month=month,
                    from_date=from_snap.snapshot_date,
                    to_date=latest.snapshot_date,
                    status="inflation_unavailable",
                    market_pct=market_pct,
                    market_delta_ars=market_delta_ars,
                    contributions_ars=contributions_ars,
                    net_pct=net_pct,
                    net_delta_ars=net_delta_ars,
                    quality_warnings=quality_warnings,
                    inflation_available_to=inflation_available_to,
                    flow_confidence=flow_confidence,
                    data_freshness=data_freshness,
                    orders_coverage=orders_coverage,
                    movements_coverage=movements_coverage,
                    estimated=estimated,
                )

            real_vs_inflation_pct = None
            if net_pct is not None:
                try:
                    real_vs_inflation_pct = ((1.0 + float(net_pct) / 100.0) / (1.0 + float(inflation_pct) / 100.0) - 1.0) * 100.0
                except Exception:
                    real_vs_inflation_pct = None

            beats_inflation = None if real_vs_inflation_pct is None else bool(real_vs_inflation_pct > 0.0)
            return _monthly_vs_inflation_payload(
                month=month,
                from_date=from_snap.snapshot_date,
                to_date=latest.snapshot_date,
                status="ok",
                market_pct=market_pct,
                market_delta_ars=market_delta_ars,
                contributions_ars=contributions_ars,
                net_pct=net_pct,
                net_delta_ars=net_delta_ars,
                inflation_pct=inflation_pct,
                inflation_projected=inflation_projected,
                real_vs_inflation_pct=real_vs_inflation_pct,
                beats_inflation=beats_inflation,
                quality_warnings=quality_warnings,
                inflation_available_to=inflation_available_to,
                flow_confidence=flow_confidence,
                data_freshness=data_freshness,
                orders_coverage=orders_coverage,
                movements_coverage=movements_coverage,
                estimated=estimated,
            )
        finally:
            conn.close()

    @router.get("/compare/inflation")
    def compare_inflation(months: int = 12):
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

            last_known_inflation_pct = None
            if infl_pct:
                try:
                    last_month = sorted(infl_pct.keys())[-1]
                    last_known_inflation_pct = float(infl_pct.get(last_month))
                except Exception:
                    last_known_inflation_pct = None

            rows = []
            projected_count = 0
            for row in monthly:
                month = row["month"]
                first_date = row["first_date"]
                last_date = row["last_date"]
                first_v = float(row["first_value"] or 0.0)
                last_v = float(row["last_value"] or 0.0)

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
        try:
            f = parse_date(date_from)
            t = parse_date(date_to)
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

            y0 = max(2017, date.fromisoformat(str(base_date)).year - 1)
            infl_start = f"{y0:04d}-01-01"
            infl_end = f"{month_key(t2)}-01"
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
                p_idx.append(cur_v / base_value * 100.0 if base_value else None)

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

            def calc_row(label: str, from_snap: Any, to_snap: Any, partial: bool) -> Dict[str, Any]:
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
                rows.append(calc_row(str(y), from_snap, to_snap, partial=partial))

            y_start_snap = dbmod.first_snapshot_of_year(conn, latest_year, latest.snapshot_date) or earliest
            if y_start_snap:
                rows.append(calc_row(f"YTD {latest_year}", y_start_snap, latest, partial=True))

            return {
                "stale": bool(infl.stale),
                "inflation_available_to": infl_available_to,
                "projection_used": bool(projection_used),
                "rows": rows,
            }
        finally:
            conn.close()

    return router, inflation, kpi_monthly_vs_inflation, compare_inflation, compare_inflation_series, compare_inflation_annual
