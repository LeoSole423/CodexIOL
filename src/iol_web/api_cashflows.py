from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional, Tuple

from fastapi import APIRouter, Body, Query
from fastapi.responses import JSONResponse

from . import db as dbmod
from .metrics import target_date


def build_cashflows_router(
    *,
    parse_date: Callable[[Optional[str]], Optional[str]],
    compute_interval_flow: Callable[[Any, Any, Any, bool], Optional[Dict[str, Any]]],
    annotate_flow_rows: Callable[[List[Dict[str, Any]]], None],
) -> Tuple[APIRouter, Callable[..., Any], Callable[..., Any], Callable[..., Any], Callable[..., Any]]:
    router = APIRouter(prefix="/api")

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
                row = compute_interval_flow(conn, base_snap, end_snap, include_threshold=True)
                if row is not None:
                    rows.append(row)

            annotate_flow_rows(rows)
            rows.sort(key=lambda row: (str(row.get("flow_date") or ""), float(row.get("amount_ars") or 0.0)), reverse=True)
            return {"from": from_date, "to": to_date, "days": days_n, "rows": rows}
        finally:
            conn.close()

    @router.get("/cashflows/manual")
    def cashflows_manual(date_from: Optional[str] = Query(None, alias="from"), date_to: Optional[str] = Query(None, alias="to")):
        try:
            f = parse_date(date_from)
            t = parse_date(date_to)
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
            flow_date = parse_date(str(payload.get("flow_date") or ""))
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
        except ValueError as exc:
            return JSONResponse(status_code=400, content={"error": str(exc)})
        except Exception:
            return JSONResponse(status_code=400, content={"error": "invalid payload"})

        try:
            row = dbmod.add_manual_cashflow_adjustment(conn, flow_date, kind, amount, note)
            return row
        except ValueError as exc:
            return JSONResponse(status_code=400, content={"error": str(exc)})
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

    return router, cashflows_auto, cashflows_manual, cashflows_manual_add, cashflows_manual_delete
