from __future__ import annotations

import calendar
from datetime import date
from typing import Any, Dict, List, Optional, Tuple

from fastapi import APIRouter, Body, Query
from fastapi.responses import JSONResponse

from . import db as dbmod
from .inflation_ar import get_inflation_series
from .inflation_compare import compounded_inflation_pct, inflation_factor_for_date, month_key
from .metrics import compute_daily_return_from_assets, compute_return, enrich_return_block, target_date
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


def _snapshot_cash_ars(snap: Optional[dbmod.Snapshot]) -> Optional[float]:
    if not snap:
        return None
    if snap.cash_disponible_ars is not None:
        try:
            return float(snap.cash_disponible_ars)
        except Exception:
            return None
    return None


def _return_with_flows(
    conn,
    latest: Optional[dbmod.Snapshot],
    base: Optional[dbmod.Snapshot],
    gross_block,
):
    if not latest:
        return enrich_return_block(
            gross=gross_block,
            base=base,
            flow_inferred_ars=None,
            flow_manual_adjustment_ars=None,
            quality_warnings=["INFERENCE_PARTIAL"],
            orders_stats=None,
        ).to_dict()

    # One-snapshot fallback (no base): keep useful daily estimate, mark as partial.
    if not base:
        return enrich_return_block(
            gross=gross_block,
            base=base,
            flow_inferred_ars=0.0,
            flow_manual_adjustment_ars=0.0,
            quality_warnings=["INFERENCE_PARTIAL"],
            orders_stats=None,
            fallback_real_pct=gross_block.pct,
        ).to_dict()

    warnings = []
    cash_base = _snapshot_cash_ars(base)
    cash_latest = _snapshot_cash_ars(latest)
    cash_missing = cash_base is None or cash_latest is None
    cash_delta = 0.0
    if cash_missing:
        warnings.extend(["CASH_MISSING", "INFERENCE_PARTIAL"])
    else:
        cash_delta = float(cash_latest or 0.0) - float(cash_base or 0.0)

    dt_from = f"{base.snapshot_date}T23:59:59"
    dt_to = f"{latest.snapshot_date}T23:59:59"
    order_amounts, order_stats = dbmod.orders_flow_summary(conn, dt_from, dt_to, currency="peso_Argentino")

    if order_stats.get("total", 0) == 0:
        warnings.append("ORDERS_NONE")
    if order_stats.get("unclassified", 0) > 0 or order_stats.get("amount_missing", 0) > 0:
        warnings.append("ORDERS_INCOMPLETE")

    flow_manual = dbmod.manual_cashflow_sum(conn, base.snapshot_date, latest.snapshot_date)
    
    flow_inferred = (
        float(cash_delta)
        + float(order_amounts.get("buy_amount") or 0.0)
        - float(order_amounts.get("sell_amount") or 0.0)
        - float(order_amounts.get("income_amount") or 0.0)
    )
    return enrich_return_block(
        gross=gross_block,
        base=base,
        flow_inferred_ars=flow_inferred,
        flow_manual_adjustment_ars=flow_manual,
        quality_warnings=warnings,
        orders_stats=order_stats,
    ).to_dict()


def _flow_quality_incomplete(row: Dict[str, Any]) -> bool:
    warns = set(str(w) for w in (row.get("quality_warnings") or []))
    return ("CASH_MISSING" in warns) or ("ORDERS_INCOMPLETE" in warns)


def _flow_date_or_none(v: Any) -> Optional[date]:
    try:
        return date.fromisoformat(str(v or ""))
    except Exception:
        return None


def _annotate_flow_rows(rows: List[Dict[str, Any]]) -> None:
    """
    Enrich rows in-place with display classification for inferred flows.
    Expects per-row keys:
      flow_date, kind, amount_ars, quality_warnings, buy_amount_ars, sell_amount_ars, income_amount_ars
    Optional:
      residual_ratio, _traded_gross
    """
    for row in rows:
        if "_traded_gross" not in row:
            b = abs(float(row.get("buy_amount_ars") or 0.0))
            s = abs(float(row.get("sell_amount_ars") or 0.0))
            i = abs(float(row.get("income_amount_ars") or 0.0))
            row["_traded_gross"] = b + s + i
        if row.get("residual_ratio") is None:
            tg = float(row.get("_traded_gross") or 0.0)
            amt = abs(float(row.get("amount_ars") or 0.0))
            row["residual_ratio"] = (amt / tg) if tg > 0 else None

    pair_by_idx: Dict[int, int] = {}
    candidates: List[Tuple[float, int, int, int]] = []
    for i in range(len(rows)):
        ri = rows[i]
        if _flow_quality_incomplete(ri):
            continue
        ai = float(ri.get("amount_ars") or 0.0)
        if abs(ai) <= 1e-9:
            continue
        di = _flow_date_or_none(ri.get("flow_date"))
        if di is None:
            continue
        ti = float(ri.get("_traded_gross") or 0.0)

        for j in range(i + 1, len(rows)):
            rj = rows[j]
            if _flow_quality_incomplete(rj):
                continue
            aj = float(rj.get("amount_ars") or 0.0)
            if abs(aj) <= 1e-9 or (ai * aj) >= 0:
                continue
            dj = _flow_date_or_none(rj.get("flow_date"))
            if dj is None:
                continue
            delta_days = abs((dj - di).days)
            if delta_days > 2:
                continue

            tj = float(rj.get("_traded_gross") or 0.0)
            mag_similarity = abs(abs(ai) - abs(aj)) / max(abs(ai), abs(aj))
            pair_net_ratio = abs(ai + aj) / (abs(ai) + abs(aj))
            denom = abs(ai) + abs(aj)
            pair_trade_coverage = ((ti + tj) / denom) if denom > 0 else 0.0
            if mag_similarity <= 0.25 and pair_net_ratio <= 0.20 and pair_trade_coverage >= 0.75:
                score = pair_net_ratio + mag_similarity
                candidates.append((score, delta_days, i, j))

    candidates.sort(key=lambda it: (it[0], it[1], it[2], it[3]))
    used_idx = set()
    for _, _, i, j in candidates:
        if i in used_idx or j in used_idx:
            continue
        pair_by_idx[i] = j
        pair_by_idx[j] = i
        used_idx.add(i)
        used_idx.add(j)

    for i, row in enumerate(rows):
        kind = str(row.get("kind") or "").lower()
        amount = float(row.get("amount_ars") or 0.0)
        residual_ratio = row.get("residual_ratio")

        display_kind = "external_flow_probable"
        display_label = "Flujo externo probable (+)" if amount >= 0 else "Flujo externo probable (-)"
        confidence = "medium"
        reason_code = "EXTERNAL_FLOW_SIGN"
        reason_detail = "Clasificado por signo del flujo neto inferido."
        paired_flow_date = None
        paired_amount_ars = None

        if _flow_quality_incomplete(row):
            display_kind = "correction"
            display_label = "Correcci\u00f3n"
            confidence = "high"
            reason_code = "QUALITY_INCOMPLETE"
            reason_detail = "Datos incompletos de caja/\u00f3rdenes; revisar manualmente."
        elif i in pair_by_idx:
            j = pair_by_idx[i]
            paired = rows[j]
            paired_flow_date = paired.get("flow_date")
            paired_amount_ars = float(paired.get("amount_ars") or 0.0)
            display_kind = "rotation_probable"
            display_label = "Rotaci\u00f3n probable (venta/recompra)"
            confidence = "medium"
            reason_code = "ROTATION_PAIR"
            reason_detail = (
                f"Par opuesto cercano con {paired_flow_date}; neto combinado bajo."
                if paired_flow_date
                else "Par opuesto cercano; neto combinado bajo."
            )
        elif kind == "withdraw" and isinstance(residual_ratio, (int, float)) and float(residual_ratio) <= 0.03:
            display_kind = "operational_cost_probable"
            display_label = "Costo operativo probable"
            confidence = "medium"
            reason_code = "OPERATIONAL_COST_RESIDUAL"
            reason_detail = "Residual chico vs volumen operado (\u22643%)."

        row["display_kind"] = display_kind
        row["display_label"] = display_label
        row["confidence"] = confidence
        row["reason_code"] = reason_code
        row["reason_detail"] = reason_detail
        row["paired_flow_date"] = paired_flow_date
        row["paired_amount_ars"] = paired_amount_ars
        row.pop("_traded_gross", None)


@router.get("/health")
def health() -> Dict[str, Any]:
    return {"ok": True}


@router.get("/snapshots")
def snapshots(
    date_from: Optional[str] = Query(None, alias="from"),
    date_to: Optional[str] = Query(None, alias="to"),
    mode: str = Query("raw"),
):
    try:
        f = _parse_date(date_from)
        t = _parse_date(date_to)
        m = (mode or "raw").strip().lower()
        if m not in ("raw", "market"):
            return JSONResponse(status_code=400, content={"error": "mode must be raw|market"})
        conn = dbmod.get_conn()
    except FileNotFoundError:
        return []

    try:
        rows = dbmod.snapshots_series(conn, f, t)
        if m == "raw":
            return [{"date": d, "total_value": v} for d, v in rows]

        # "market" mode: de-bias series by subtracting inferred external flows
        # on each interval (same inference base logic used in returns, plus
        # classification to avoid netting internal rotation as external flow).
        if not rows:
            return []

        intervals: List[Dict[str, Any]] = []
        for idx in range(1, len(rows)):
            base_d, base_v = rows[idx - 1]
            end_d, end_v = rows[idx]
            base_snap = dbmod.snapshot_on_or_before(conn, base_d)
            end_snap = dbmod.snapshot_on_or_before(conn, end_d)
            gross_delta = float(end_v or 0.0) - float(base_v or 0.0)
            if not base_snap or not end_snap:
                intervals.append(
                    {
                        "flow_date": end_d,
                        "gross_delta": gross_delta,
                        "amount_ars": 0.0,
                        "kind": "correction",
                        "buy_amount_ars": 0.0,
                        "sell_amount_ars": 0.0,
                        "income_amount_ars": 0.0,
                        "quality_warnings": ["INFERENCE_PARTIAL"],
                        "residual_ratio": None,
                    }
                )
                continue

            gross = compute_return(end_snap, base_snap)
            wf = _return_with_flows(conn, end_snap, base_snap, gross)
            dt_from = f"{base_snap.snapshot_date}T23:59:59"
            dt_to = f"{end_snap.snapshot_date}T23:59:59"
            amounts, _ = dbmod.orders_flow_summary(conn, dt_from, dt_to, currency="peso_Argentino")
            buy_amount = float(amounts.get("buy_amount") or 0.0)
            sell_amount = float(amounts.get("sell_amount") or 0.0)
            income_amount = float(amounts.get("income_amount") or 0.0)
            flow_total = float(wf.get("flow_total_ars") or 0.0)
            traded_gross = abs(buy_amount) + abs(sell_amount) + abs(income_amount)
            residual_ratio = (abs(flow_total) / traded_gross) if traded_gross > 0 else None
            warns = list(wf.get("quality_warnings") or [])
            kind = "correction" if _flow_quality_incomplete({"quality_warnings": warns}) else ("deposit" if flow_total >= 0 else "withdraw")
            intervals.append(
                {
                    "flow_date": end_d,
                    "gross_delta": gross_delta,
                    "amount_ars": flow_total,
                    "kind": kind,
                    "buy_amount_ars": buy_amount,
                    "sell_amount_ars": sell_amount,
                    "income_amount_ars": income_amount,
                    "quality_warnings": warns,
                    "residual_ratio": residual_ratio,
                    "_traded_gross": traded_gross,
                }
            )

        _annotate_flow_rows(intervals)

        out: List[Dict[str, Any]] = []
        first_d, first_v = rows[0]
        adjusted_prev = float(first_v or 0.0)
        out.append(
            {
                "date": first_d,
                "total_value": adjusted_prev,
                "raw_total_value": float(first_v or 0.0),
                "flow_total_ars": 0.0,
                "quality_warnings": [],
            }
        )

        for idx in range(1, len(rows)):
            end_d, end_v = rows[idx]
            iv = intervals[idx - 1]
            gross_delta = float(iv.get("gross_delta") or 0.0)
            flow_total = float(iv.get("amount_ars") or 0.0)
            display_kind = str(iv.get("display_kind") or "")
            # Apply adjustment only for external-flow-like movements.
            applied_flow = flow_total if display_kind == "external_flow_probable" else 0.0
            adjusted_prev = adjusted_prev + (gross_delta - applied_flow)
            out.append(
                {
                    "date": end_d,
                    "total_value": adjusted_prev,
                    "raw_total_value": float(end_v or 0.0),
                    "flow_total_ars": flow_total,
                    "applied_flow_ars": applied_flow,
                    "display_kind": display_kind,
                    "quality_warnings": list(iv.get("quality_warnings") or []),
                }
            )
        return out
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
        "status": status,
    }


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
        empty = _return_with_flows(None, None, None, compute_return(None, None))
        return {"daily": empty, "weekly": empty, "monthly": empty, "yearly": empty, "ytd": empty}

    try:
        latest = dbmod.latest_snapshot(conn)
        if not latest:
            empty = _return_with_flows(conn, None, None, compute_return(None, None))
            return {"daily": empty, "weekly": empty, "monthly": empty, "yearly": empty, "ytd": empty}

        base_daily = dbmod.snapshot_before(conn, latest.snapshot_date)
        base_weekly = dbmod.snapshot_on_or_before(conn, target_date(latest.snapshot_date, 7))
        base_monthly = dbmod.snapshot_on_or_before(conn, target_date(latest.snapshot_date, 30))
        base_yearly = dbmod.snapshot_on_or_before(conn, target_date(latest.snapshot_date, 365))

        y = date.fromisoformat(latest.snapshot_date).year
        base_ytd = dbmod.first_snapshot_of_year(conn, y, latest.snapshot_date) or dbmod.earliest_snapshot(conn)

        daily_gross = None
        if base_daily:
            daily_gross = compute_return(latest, base_daily)
        else:
            # If there is only one snapshot in the DB, returns between snapshots can't be computed.
            # Still show a useful daily delta using IOL-provided per-asset daily variation.
            assets = dbmod.assets_for_snapshot(conn, latest.snapshot_date)
            daily_gross = compute_daily_return_from_assets(latest, assets)

        return {
            "daily": _return_with_flows(conn, latest, base_daily, daily_gross),
            "weekly": _return_with_flows(conn, latest, base_weekly, compute_return(latest, base_weekly)),
            "monthly": _return_with_flows(conn, latest, base_monthly, compute_return(latest, base_monthly)),
            "yearly": _return_with_flows(conn, latest, base_yearly, compute_return(latest, base_yearly)),
            "ytd": _return_with_flows(conn, latest, base_ytd, compute_return(latest, base_ytd)),
        }
    finally:
        conn.close()


@router.get("/cashflows/auto")
def cashflows_auto(days: int = 30):
    try:
        days_n = int(days)
    except Exception:
        return JSONResponse(status_code=400, content={"error": "days must be an integer (1..365)"})
    if days_n < 1 or days_n > 365:
        return JSONResponse(status_code=400, content={"error": "days must be 1..365"})

    try:
        conn = dbmod.get_conn()
    except FileNotFoundError:
        return {"from": None, "to": None, "days": days_n, "rows": []}

    try:
        latest = dbmod.latest_snapshot(conn)
        if not latest:
            return {"from": None, "to": None, "days": days_n, "rows": []}

        to_date = latest.snapshot_date
        from_date = target_date(to_date, days_n)
        snap_dates = [d for (d, _) in dbmod.snapshots_series(conn, from_date, to_date)]
        if len(snap_dates) < 2:
            return {"from": from_date, "to": to_date, "days": days_n, "rows": []}

        rows: List[Dict[str, Any]] = []
        for i in range(1, len(snap_dates)):
            base_snap = dbmod.snapshot_on_or_before(conn, snap_dates[i - 1])
            end_snap = dbmod.snapshot_on_or_before(conn, snap_dates[i])
            if not base_snap or not end_snap:
                continue

            warnings = []
            cash_base = _snapshot_cash_ars(base_snap)
            cash_end = _snapshot_cash_ars(end_snap)
            if cash_base is None or cash_end is None:
                cash_delta = 0.0
                warnings.append("CASH_MISSING")
            else:
                cash_delta = float(cash_end or 0.0) - float(cash_base or 0.0)

            dt_from = f"{base_snap.snapshot_date}T23:59:59"
            dt_to = f"{end_snap.snapshot_date}T23:59:59"
            amounts, stats = dbmod.orders_flow_summary(conn, dt_from, dt_to, currency="peso_Argentino")
            if stats.get("unclassified", 0) > 0 or stats.get("amount_missing", 0) > 0:
                warnings.append("ORDERS_INCOMPLETE")

            buy_amount = float(amounts.get("buy_amount") or 0.0)
            sell_amount = float(amounts.get("sell_amount") or 0.0)
            income_amount = float(amounts.get("income_amount") or 0.0)
            flow_inferred = float(cash_delta) + buy_amount - sell_amount - income_amount
            if abs(flow_inferred) <= 1e-9:
                continue
            traded_gross = abs(buy_amount) + abs(sell_amount) + abs(income_amount)
            residual_ratio = (abs(flow_inferred) / traded_gross) if traded_gross > 0 else None

            if "CASH_MISSING" in warnings or "ORDERS_INCOMPLETE" in warnings:
                kind = "correction"
            else:
                kind = "deposit" if flow_inferred > 0 else "withdraw"

            rows.append(
                {
                    "flow_date": end_snap.snapshot_date,
                    "kind": kind,
                    "amount_ars": flow_inferred,
                    "base_snapshot": base_snap.snapshot_date,
                    "end_snapshot": end_snap.snapshot_date,
                    "cash_delta_ars": cash_delta,
                    "buy_amount_ars": buy_amount,
                    "sell_amount_ars": sell_amount,
                    "income_amount_ars": income_amount,
                    "quality_warnings": warnings,
                    "orders_stats": stats,
                    "residual_ratio": residual_ratio,
                    "_traded_gross": traded_gross,
                }
            )

        _annotate_flow_rows(rows)

        rows.sort(key=lambda r: (str(r.get("flow_date") or ""), float(r.get("amount_ars") or 0.0)), reverse=True)
        return {"from": from_date, "to": to_date, "days": days_n, "rows": rows}
    finally:
        conn.close()


@router.get("/cashflows/manual")
def cashflows_manual(date_from: Optional[str] = Query(None, alias="from"), date_to: Optional[str] = Query(None, alias="to")):
    try:
        f = _parse_date(date_from)
        t = _parse_date(date_to)
        conn = dbmod.get_conn_rw()
    except FileNotFoundError:
        return []
    except Exception:
        return JSONResponse(status_code=400, content={"error": "invalid date format (YYYY-MM-DD)"})

    try:
        return dbmod.list_manual_cashflow_adjustments(conn, f, t)
    finally:
        conn.close()


@router.post("/cashflows/manual")
def cashflows_manual_add(payload: Dict[str, Any] = Body(...)):
    try:
        flow_date = _parse_date(str(payload.get("flow_date") or ""))
        if flow_date is None:
            return JSONResponse(status_code=400, content={"error": "flow_date is required (YYYY-MM-DD)"})
        kind = str(payload.get("kind") or "").strip().lower()
        amount_raw = payload.get("amount_ars")
        if amount_raw is None:
            return JSONResponse(status_code=400, content={"error": "amount_ars is required"})
        amount = float(amount_raw)
        note = payload.get("note")
        conn = dbmod.get_conn_rw()
    except FileNotFoundError:
        return JSONResponse(status_code=404, content={"error": "DB not found"})
    except ValueError as e:
        return JSONResponse(status_code=400, content={"error": str(e)})
    except Exception:
        return JSONResponse(status_code=400, content={"error": "invalid payload"})

    try:
        row = dbmod.add_manual_cashflow_adjustment(conn, flow_date, kind, amount, note)
        return row
    except ValueError as e:
        return JSONResponse(status_code=400, content={"error": str(e)})
    finally:
        conn.close()


@router.delete("/cashflows/manual/{row_id}")
def cashflows_manual_delete(row_id: int):
    try:
        conn = dbmod.get_conn_rw()
    except FileNotFoundError:
        return JSONResponse(status_code=404, content={"error": "DB not found"})

    try:
        ok = dbmod.delete_manual_cashflow_adjustment(conn, int(row_id))
        if not ok:
            return JSONResponse(status_code=404, content={"error": "not found"})
        return {"ok": True, "id": int(row_id)}
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


@router.get("/kpi/monthly-vs-inflation")
def kpi_monthly_vs_inflation():
    try:
        conn = dbmod.get_conn()
    except FileNotFoundError:
        return _monthly_vs_inflation_payload(
            month=None,
            from_date=None,
            to_date=None,
            status="insufficient_snapshots",
        )

    try:
        latest = dbmod.latest_snapshot(conn)
        if not latest:
            return _monthly_vs_inflation_payload(
                month=None,
                from_date=None,
                to_date=None,
                status="insufficient_snapshots",
            )

        latest_d = date.fromisoformat(latest.snapshot_date)
        month = f"{latest_d.year:04d}-{latest_d.month:02d}"
        month_start = f"{latest_d.year:04d}-{latest_d.month:02d}-01"
        from_snap_in_month = dbmod.first_snapshot_in_range(conn, month_start, latest.snapshot_date)
        from_snap = from_snap_in_month
        if not from_snap_in_month or from_snap_in_month.snapshot_date == latest.snapshot_date:
            # Month-in-progress fallback: use the latest snapshot on/before month start
            # so we can still show an MTD signal with a single snapshot in the current month.
            from_snap = dbmod.snapshot_on_or_before(conn, month_start)
        if not from_snap or from_snap.snapshot_date == latest.snapshot_date:
            return _monthly_vs_inflation_payload(
                month=month,
                from_date=from_snap.snapshot_date if from_snap else None,
                to_date=latest.snapshot_date,
                status="insufficient_snapshots",
            )

        gross = compute_return(latest, from_snap)
        with_flows = _return_with_flows(conn, latest, from_snap, gross)

        market_pct = with_flows.get("pct")
        market_delta_ars = with_flows.get("delta")
        contributions_ars = with_flows.get("flow_total_ars")
        net_pct = with_flows.get("real_pct")
        net_delta_ars = with_flows.get("real_delta")
        quality_warnings = with_flows.get("quality_warnings") or []

        inflation_available_to = None
        inflation_pct = None
        inflation_projected = False
        try:
            # Same projection rule as compare_inflation:
            # if current month IPC is not published yet, use the last published month as estimate.
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
        )
    finally:
        conn.close()


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
                # Baseline snapshot represents end-of-day state for base date.
                # Exclude base-day operations to avoid counting cashflows that happened before that snapshot.
                dt_from = f"{base_snap.snapshot_date}T23:59:59"
                dt_to = f"{period_end_snap.snapshot_date}T23:59:59"
                cashflows, stats = dbmod.orders_cashflows_by_symbol(conn, dt_from, dt_to, currency=currency_norm)
                orders_stats = stats
                if stats.get("total", 0) == 0:
                    warnings.append("ORDERS_NONE")
                elif stats.get("unclassified", 0) > 0 or stats.get("amount_missing", 0) > 0:
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
        else:  # accumulated
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
            for a in end_assets:
                cur_val = float(a.get("total_value") or 0.0)
                pct = a.get("daily_var_pct")
                try:
                    pct_f = float(pct) if pct is not None else None
                except Exception:
                    pct_f = None
                selected = None if pct_f is None else (cur_val * pct_f / 100.0)
                rows.append(
                    {
                        "symbol": a.get("symbol"),
                        "description": a.get("description"),
                        "currency": a.get("currency") or "unknown",
                        "market": a.get("market"),
                        "type": a.get("type"),
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
            # Baseline snapshot is end-of-day; do not include base-date operations in period cashflows.
            dt_from = f"{base_snap.snapshot_date}T23:59:59"
            dt_to = f"{period_end_snap.snapshot_date}T23:59:59"
            cashflows, stats = dbmod.orders_cashflows_by_symbol(conn, dt_from, dt_to, currency="all")
            orders_stats = stats
            if stats.get("total", 0) == 0:
                warnings.append("ORDERS_NONE")
            elif stats.get("unclassified", 0) > 0 or stats.get("amount_missing", 0) > 0:
                warnings.append("ORDERS_INCOMPLETE")
            enriched = build_union_movers_pnl(base_assets, end_assets_period, cashflows)
            for r in enriched:
                rows.append(
                    {
                        "symbol": r.get("symbol"),
                        "description": r.get("description"),
                        "currency": r.get("currency") or "unknown",
                        "market": r.get("market"),
                        "type": r.get("type"),
                        "total_value": float(r.get("total_value") or 0.0),
                        "base_total_value": float(r.get("base_total_value") or 0.0),
                        "selected_value": r.get("delta_value"),
                        "selected_pct": r.get("delta_pct"),
                        "flow_tag": r.get("flow_tag") or "none",
                    }
                )
            from_date = base_snap.snapshot_date
            to_date = period_end_snap.snapshot_date

        total_visible = sum(float(r.get("total_value") or 0.0) for r in rows)
        for r in rows:
            val = float(r.get("total_value") or 0.0)
            r["weight_pct"] = ((val / total_visible) * 100.0) if total_visible > 0 else 0.0

        def _sort_key(row: Dict[str, Any]):
            try:
                selected = float(row.get("selected_value") or 0.0)
            except Exception:
                selected = 0.0
            try:
                total = float(row.get("total_value") or 0.0)
            except Exception:
                total = 0.0
            sym = str(row.get("symbol") or "")
            return (-selected, -total, sym)

        rows.sort(key=_sort_key)

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
