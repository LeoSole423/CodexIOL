"""Web API endpoints for the multi-engine financial advisor.

Endpoints:
  GET  /api/engines/regime?as_of=YYYY-MM-DD
  GET  /api/engines/macro?as_of=YYYY-MM-DD
  GET  /api/engines/smart-money?as_of=YYYY-MM-DD&symbol=AAPL&min_conviction=0
  GET  /api/engines/strategy?as_of=YYYY-MM-DD
  POST /api/engines/run-all
"""
from __future__ import annotations

from datetime import date
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, BackgroundTasks
from fastapi.responses import JSONResponse

from . import db as dbmod

router = APIRouter(prefix="/engines")


def _resolve_date(as_of: Optional[str]) -> str:
    if as_of and as_of.strip():
        return as_of.strip()
    return date.today().isoformat()


@router.get("/regime")
def engines_regime(as_of: Optional[str] = None):
    """Return the latest cached market regime signal."""
    from iol_cli.db import connect, init_db, resolve_db_path as cli_resolve
    from iol_engines.regime.engine import MarketRegimeEngine

    target = _resolve_date(as_of)
    try:
        db_path = dbmod.resolve_db_path()
        conn = connect(db_path)
        init_db(conn)
        sig = MarketRegimeEngine().load_latest(conn, target)
        if sig is None:
            return {"as_of": target, "signal": None, "message": "No regime signal yet. Run: iol engines regime run"}
        return {"as_of": target, "signal": sig.to_dict()}
    except Exception as exc:
        return JSONResponse(status_code=500, content={"error": str(exc)})


@router.get("/macro")
def engines_macro(as_of: Optional[str] = None):
    """Return the latest cached macro momentum signal."""
    from iol_cli.db import connect, init_db
    from iol_engines.macro.engine import MacroMomentumEngine

    target = _resolve_date(as_of)
    try:
        db_path = dbmod.resolve_db_path()
        conn = connect(db_path)
        init_db(conn)
        sig = MacroMomentumEngine().load_latest(conn, target)
        if sig is None:
            return {"as_of": target, "signal": None, "message": "No macro signal yet. Run: iol engines macro run"}
        return {"as_of": target, "signal": sig.to_dict()}
    except Exception as exc:
        return JSONResponse(status_code=500, content={"error": str(exc)})


@router.get("/smart-money")
def engines_smart_money(
    as_of: Optional[str] = None,
    symbol: Optional[str] = None,
    min_conviction: float = 0,
):
    """Return cached institutional 13F conviction signals."""
    from iol_cli.db import connect, init_db
    from iol_engines.smart_money.engine import SmartMoneyEngine

    target = _resolve_date(as_of)
    try:
        db_path = dbmod.resolve_db_path()
        conn = connect(db_path)
        init_db(conn)
        engine = SmartMoneyEngine()
        if symbol:
            sig = engine.load_latest(conn, target, symbol=symbol.upper())
            if sig is None:
                return {"as_of": target, "signal": None, "symbol": symbol}
            return {"as_of": target, "signal": sig.to_dict()}
        else:
            signals = engine.load_latest(conn, target) or []
            signals = [s for s in signals if s.conviction_score >= min_conviction]
            signals_sorted = sorted(signals, key=lambda s: s.conviction_score, reverse=True)
            return {
                "as_of": target,
                "count": len(signals_sorted),
                "signals": [s.to_dict() for s in signals_sorted],
            }
    except Exception as exc:
        return JSONResponse(status_code=500, content={"error": str(exc)})


@router.get("/strategy")
def engines_strategy(as_of: Optional[str] = None):
    """Return the latest cached strategy action plan."""
    from iol_cli.db import connect, init_db
    from iol_engines.strategy.engine import PortfolioStrategyEngine

    target = _resolve_date(as_of)
    try:
        db_path = dbmod.resolve_db_path()
        conn = connect(db_path)
        init_db(conn)
        plan = PortfolioStrategyEngine().load_latest(conn, target)
        if plan is None:
            return {
                "as_of": target,
                "plan": None,
                "message": "No strategy plan yet. Run: iol engines run-all",
            }
        return {"as_of": target, "plan": plan.to_dict()}
    except Exception as exc:
        return JSONResponse(status_code=500, content={"error": str(exc)})


@router.get("/accuracy")
def engines_accuracy(
    days: int = 90,
    engine: Optional[str] = None,
    update: bool = True,
):
    """Return signal accuracy metrics for each engine over the last N days."""
    from iol_cli.db import connect, init_db
    from iol_engines.analysis.accuracy import compute_signal_outcomes, get_accuracy_report

    try:
        db_path = dbmod.resolve_db_path()
        conn = connect(db_path)
        init_db(conn)
        if update:
            compute_signal_outcomes(conn)
        report = get_accuracy_report(conn, days=days, engine=engine)
        return {"window_days": days, "engines": report}
    except Exception as exc:
        return JSONResponse(status_code=500, content={"error": str(exc)})


@router.post("/run-all")
def engines_run_all(
    background_tasks: BackgroundTasks,
    as_of: Optional[str] = None,
    budget_ars: Optional[float] = None,
    skip_smart_money: bool = False,
    skip_external: bool = False,
):
    """Trigger full engine pipeline in the background. Returns immediately."""
    from iol_cli.db import connect, init_db
    from iol_engines.registry import run_full_engine_pipeline

    target = _resolve_date(as_of)

    def _run() -> None:
        try:
            db_path = dbmod.resolve_db_path()
            conn = connect(db_path)
            init_db(conn)
            run_full_engine_pipeline(
                target,
                conn,
                budget_ars=budget_ars,
                skip_smart_money=skip_smart_money,
                skip_external=skip_external,
                verbose=False,
            )
        except Exception:
            pass  # logged server-side

    background_tasks.add_task(_run)
    return {
        "status": "started",
        "as_of": target,
        "message": "Engine pipeline running in background. Poll GET /api/engines/strategy for results.",
    }
