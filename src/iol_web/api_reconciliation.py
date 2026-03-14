from __future__ import annotations

from typing import Any, Dict, Optional

from fastapi import APIRouter, Body, Query
from fastapi.responses import JSONResponse

from iol_reconciliation.service import (
    apply_proposal as apply_reconciliation_proposal,
    dismiss_proposal as dismiss_reconciliation_proposal,
    explain_interval as explain_reconciliation_interval,
    get_latest_payload as get_latest_reconciliation_payload,
    get_open_payload as get_open_reconciliation_payload,
)

from . import db as dbmod


router = APIRouter(prefix="/api")


@router.get("/reconciliation/latest")
def reconciliation_latest(as_of: Optional[str] = Query(None, alias="as_of")):
    try:
        conn = dbmod.get_conn_rw()
    except FileNotFoundError:
        return {"summary": {}, "intervals": [], "proposals": []}
    try:
        return get_latest_reconciliation_payload(conn, as_of=as_of, ensure=True)
    finally:
        conn.close()


@router.get("/reconciliation/open")
def reconciliation_open(as_of: Optional[str] = Query(None, alias="as_of")):
    try:
        conn = dbmod.get_conn_rw()
    except FileNotFoundError:
        return {"run": {"summary": {}}, "rows": []}
    try:
        return get_open_reconciliation_payload(conn, as_of=as_of, ensure=True)
    finally:
        conn.close()


@router.post("/reconciliation/apply")
def reconciliation_apply(payload: Dict[str, Any] = Body(...)):
    proposal_id = payload.get("proposal_id")
    if proposal_id is None:
        return JSONResponse(status_code=400, content={"error": "proposal_id is required"})
    try:
        conn = dbmod.get_conn_rw()
    except FileNotFoundError:
        return JSONResponse(status_code=404, content={"error": "DB not found"})
    try:
        return apply_reconciliation_proposal(conn, int(proposal_id), note=payload.get("note"))
    except ValueError as exc:
        return JSONResponse(status_code=400, content={"error": str(exc)})
    finally:
        conn.close()


@router.post("/reconciliation/dismiss")
def reconciliation_dismiss(payload: Dict[str, Any] = Body(...)):
    proposal_id = payload.get("proposal_id")
    reason = str(payload.get("reason") or "").strip()
    if proposal_id is None:
        return JSONResponse(status_code=400, content={"error": "proposal_id is required"})
    if not reason:
        return JSONResponse(status_code=400, content={"error": "reason is required"})
    try:
        conn = dbmod.get_conn_rw()
    except FileNotFoundError:
        return JSONResponse(status_code=404, content={"error": "DB not found"})
    try:
        return dismiss_reconciliation_proposal(conn, int(proposal_id), reason=reason)
    except ValueError as exc:
        return JSONResponse(status_code=400, content={"error": str(exc)})
    finally:
        conn.close()


@router.get("/reconciliation/interval/{interval_id}")
def reconciliation_interval(interval_id: int):
    try:
        conn = dbmod.get_conn_rw()
    except FileNotFoundError:
        return JSONResponse(status_code=404, content={"error": "DB not found"})
    try:
        return explain_reconciliation_interval(conn, int(interval_id))
    except ValueError as exc:
        return JSONResponse(status_code=404, content={"error": str(exc)})
    finally:
        conn.close()
