from __future__ import annotations

from datetime import date
from typing import Any, Callable, Dict, List, Optional, Tuple

from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse

from . import db as dbmod
from .flow_utils import EXTERNAL_DISPLAY_KINDS
from .metrics import compute_daily_return_from_assets, compute_return, target_date


def build_returns_router(
    *,
    compute_interval_flow: Callable,
    annotate_flow_rows: Callable,
    return_with_flows: Callable,
) -> Tuple[APIRouter, Callable, Callable, Callable]:
    router = APIRouter()

    @router.get("/health")
    def health() -> Dict[str, Any]:
        return {"ok": True}

    @router.get("/snapshots")
    def snapshots(
        date_from: Optional[str] = Query(None, alias="from"),
        date_to: Optional[str] = Query(None, alias="to"),
        mode: str = Query("raw"),
    ):
        from .flow_utils import parse_date
        try:
            f = parse_date(date_from)
            t = parse_date(date_to)
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
                            "external_raw_ars": 0.0,
                            "external_adjusted_ars": 0.0,
                            "external_final_ars": 0.0,
                            "fx_revaluation_ars": 0.0,
                            "imported_internal_ars": 0.0,
                            "imported_external_ars": 0.0,
                            "cash_total_delta_ars": 0.0,
                            "cash_ars_delta": None,
                            "cash_usd_delta": None,
                            "buy_amount_ars": 0.0,
                            "sell_amount_ars": 0.0,
                            "income_amount_ars": 0.0,
                            "fee_amount_ars": 0.0,
                            "quality_warnings": ["INFERENCE_PARTIAL"],
                            "residual_ratio": None,
                        }
                    )
                    continue

                iv = compute_interval_flow(conn, base_snap, end_snap, include_threshold=False)
                if iv is None:
                    iv = {
                        "flow_date": end_d,
                        "kind": "correction",
                        "amount_ars": 0.0,
                        "external_raw_ars": 0.0,
                        "external_adjusted_ars": 0.0,
                        "external_final_ars": 0.0,
                        "fx_revaluation_ars": 0.0,
                        "imported_internal_ars": 0.0,
                        "imported_external_ars": 0.0,
                        "cash_delta_ars": 0.0,
                        "cash_total_delta_ars": 0.0,
                        "cash_ars_delta": None,
                        "cash_usd_delta": None,
                        "buy_amount_ars": 0.0,
                        "sell_amount_ars": 0.0,
                        "income_amount_ars": 0.0,
                        "fee_amount_ars": 0.0,
                        "quality_warnings": [],
                        "residual_ratio": None,
                    }
                stats = iv.get("orders_stats") or {}
                if int(stats.get("total", 0) or 0) == 0:
                    warns = list(iv.get("quality_warnings") or [])
                    if "ORDERS_NONE" not in warns:
                        warns.append("ORDERS_NONE")
                    iv["quality_warnings"] = warns
                iv["gross_delta"] = gross_delta
                intervals.append(iv)

            annotate_flow_rows(intervals)

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
                applied_flow = flow_total if display_kind in EXTERNAL_DISPLAY_KINDS else 0.0
                adjusted_prev = adjusted_prev + (gross_delta - applied_flow)
                out.append(
                    {
                        "date": end_d,
                        "total_value": adjusted_prev,
                        "raw_total_value": float(end_v or 0.0),
                        "flow_total_ars": flow_total,
                        "applied_flow_ars": applied_flow,
                        "external_raw_ars": float(iv.get("external_raw_ars") or 0.0),
                        "external_adjusted_ars": float(iv.get("external_adjusted_ars") or 0.0),
                        "external_final_ars": float(iv.get("external_final_ars") or flow_total),
                        "fx_revaluation_ars": float(iv.get("fx_revaluation_ars") or 0.0),
                        "imported_internal_ars": float(iv.get("imported_internal_ars") or 0.0),
                        "imported_external_ars": float(iv.get("imported_external_ars") or 0.0),
                        "display_kind": display_kind,
                        "display_label": iv.get("display_label"),
                        "reason_code": iv.get("reason_code"),
                        "reason_detail": iv.get("reason_detail"),
                        "quality_warnings": list(iv.get("quality_warnings") or []),
                    }
                )
            return out
        finally:
            conn.close()

    @router.get("/returns")
    def returns():
        try:
            conn = dbmod.get_conn()
        except FileNotFoundError:
            empty = return_with_flows(None, None, None, compute_return(None, None))
            return {"daily": empty, "weekly": empty, "monthly": empty, "yearly": empty, "ytd": empty, "inception": empty}

        try:
            latest = dbmod.latest_snapshot(conn)
            if not latest:
                empty = return_with_flows(conn, None, None, compute_return(None, None))
                return {"daily": empty, "weekly": empty, "monthly": empty, "yearly": empty, "ytd": empty, "inception": empty}

            base_daily = dbmod.snapshot_before(conn, latest.snapshot_date)
            base_weekly = dbmod.snapshot_on_or_before(conn, target_date(latest.snapshot_date, 7))
            latest_d = date.fromisoformat(latest.snapshot_date)
            month_start = latest_d.replace(day=1).isoformat()
            base_monthly = dbmod.first_snapshot_in_range(conn, month_start, latest.snapshot_date) or latest

            y = latest_d.year
            base_yearly = dbmod.first_snapshot_of_year(conn, y, latest.snapshot_date) or latest
            base_ytd = base_yearly
            base_inception = dbmod.earliest_snapshot(conn) or latest

            daily_gross = None
            if base_daily:
                daily_gross = compute_return(latest, base_daily)
            else:
                assets = dbmod.assets_for_snapshot(conn, latest.snapshot_date)
                daily_gross = compute_daily_return_from_assets(latest, assets)

            return {
                "daily": return_with_flows(conn, latest, base_daily, daily_gross),
                "weekly": return_with_flows(conn, latest, base_weekly, compute_return(latest, base_weekly)),
                "monthly": return_with_flows(conn, latest, base_monthly, compute_return(latest, base_monthly)),
                "yearly": return_with_flows(conn, latest, base_yearly, compute_return(latest, base_yearly)),
                "ytd": return_with_flows(conn, latest, base_ytd, compute_return(latest, base_ytd)),
                "inception": return_with_flows(conn, latest, base_inception, compute_return(latest, base_inception)),
            }
        finally:
            conn.close()

    return router, health, snapshots, returns
