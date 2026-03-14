from __future__ import annotations

from typing import Optional

from fastapi import APIRouter
from fastapi.responses import JSONResponse

from iol_advisor.service import (
    load_briefing_history_payload,
    load_latest_briefing_payload,
    load_latest_opportunity_payload,
)

from . import db as dbmod


router = APIRouter()


def _advisor_cadence(v: str) -> str:
    c = str(v or "").strip().lower()
    if c not in ("daily", "weekly"):
        raise ValueError("cadence must be daily|weekly")
    return c


@router.get("/advisor/latest")
def advisor_latest(cadence: str = "daily"):
    try:
        cadence_v = _advisor_cadence(cadence)
    except ValueError as exc:
        return JSONResponse(status_code=400, content={"error": str(exc)})
    payload = load_latest_briefing_payload(dbmod.resolve_db_path(), cadence_v)
    return {"cadence": cadence_v, "briefing": payload}


@router.get("/advisor/history")
def advisor_history(cadence: Optional[str] = None, limit: int = 20):
    cadence_v = None
    if cadence is not None and str(cadence).strip():
        try:
            cadence_v = _advisor_cadence(cadence)
        except ValueError as exc:
            return JSONResponse(status_code=400, content={"error": str(exc)})
    limit_v = int(limit)
    if limit_v < 1 or limit_v > 200:
        return JSONResponse(status_code=400, content={"error": "limit must be 1..200"})
    rows = load_briefing_history_payload(dbmod.resolve_db_path(), cadence_v, limit_v)
    return {"cadence": cadence_v, "rows": rows}


@router.get("/advisor/opportunities/latest")
def advisor_opportunities_latest():
    payload = load_latest_opportunity_payload(dbmod.resolve_db_path())
    if not payload:
        return {"run": None}
    return {"run": payload}
