"""Web API endpoints for the paper-trading simulation framework.

Endpoints:
  GET  /api/simulation/bots
  POST /api/simulation/run
  GET  /api/simulation/runs?bot=&limit=
  GET  /api/simulation/runs/{run_id}
  GET  /api/simulation/runs/{run_id}/trades
  GET  /api/simulation/compare?run_ids=1,2,3
"""
from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, BackgroundTasks
from fastapi.responses import JSONResponse

from . import db as dbmod

router = APIRouter(prefix="/simulation")


@router.get("/bots")
def simulation_bots():
    """List all available bot presets."""
    from iol_engines.simulation.bot_config import PRESETS

    return {
        "presets": [cfg.to_dict() for cfg in PRESETS.values()],
    }


@router.post("/run")
def simulation_run(
    background_tasks: BackgroundTasks,
    bot_config: str = "balanced",
    date_from: str = "2024-01-01",
    date_to: str = "2024-12-31",
    initial_cash_ars: float = 1_000_000.0,
):
    """Start a backtest in the background. Returns run_id immediately."""
    from iol_cli.db import connect, init_db
    from iol_engines.simulation.bot_config import get_preset
    from iol_engines.simulation.runner import run_backtest, _create_run_row

    try:
        config = get_preset(bot_config)
    except ValueError as exc:
        return JSONResponse(status_code=400, content={"error": str(exc)})

    # Pre-create the run row synchronously so we can return its id.
    try:
        db_path = dbmod.resolve_db_path()
        conn = connect(db_path)
        init_db(conn)
        run_id = _create_run_row(conn, config, date_from, date_to, initial_cash_ars)
        conn.close()
    except Exception as exc:
        return JSONResponse(status_code=500, content={"error": str(exc)})

    def _run() -> None:
        try:
            db_path2 = dbmod.resolve_db_path()
            conn2 = connect(db_path2)
            init_db(conn2)
            run_backtest(
                conn2, config, date_from, date_to, initial_cash_ars,
                verbose=False, existing_run_id=run_id,
            )
        except Exception:
            pass

    background_tasks.add_task(_run)
    return {
        "status": "started",
        "run_id": run_id,
        "bot_config": bot_config,
        "message": f"Backtest started. Poll GET /api/simulation/runs/{run_id} for results.",
    }


@router.get("/runs")
def simulation_list_runs(
    bot: Optional[str] = None,
    limit: int = 50,
):
    """List recent simulation runs."""
    from iol_cli.db import connect, init_db
    from iol_engines.simulation.report import list_runs

    try:
        db_path = dbmod.resolve_db_path()
        conn = connect(db_path)
        init_db(conn)
        runs = list_runs(conn, limit=limit, bot_name=bot)
        return {"count": len(runs), "runs": runs}
    except Exception as exc:
        return JSONResponse(status_code=500, content={"error": str(exc)})


@router.get("/runs/{run_id}")
def simulation_get_run(run_id: int):
    """Load a single simulation run with full metrics."""
    from iol_cli.db import connect, init_db
    from iol_engines.simulation.report import load_run

    try:
        db_path = dbmod.resolve_db_path()
        conn = connect(db_path)
        init_db(conn)
        result = load_run(conn, run_id)
        if result is None:
            return JSONResponse(status_code=404, content={"error": f"Run #{run_id} not found"})
        return result
    except Exception as exc:
        return JSONResponse(status_code=500, content={"error": str(exc)})


@router.get("/runs/{run_id}/trades")
def simulation_get_trades(run_id: int, limit: int = 200):
    """Load paper trades for a simulation run."""
    from iol_cli.db import connect, init_db
    from iol_engines.simulation.report import load_trades

    try:
        db_path = dbmod.resolve_db_path()
        conn = connect(db_path)
        init_db(conn)
        trades = load_trades(conn, run_id, limit=limit)
        return {"run_id": run_id, "count": len(trades), "trades": trades}
    except Exception as exc:
        return JSONResponse(status_code=500, content={"error": str(exc)})


@router.get("/compare")
def simulation_compare(run_ids: str):
    """Compare multiple simulation runs. run_ids is comma-separated, e.g. '1,2,3'."""
    from iol_cli.db import connect, init_db
    from iol_engines.simulation.report import compare_runs

    try:
        ids = [int(x.strip()) for x in run_ids.split(",") if x.strip()]
    except ValueError:
        return JSONResponse(status_code=400, content={"error": "run_ids must be comma-separated integers"})

    try:
        db_path = dbmod.resolve_db_path()
        conn = connect(db_path)
        init_db(conn)
        return compare_runs(conn, ids)
    except Exception as exc:
        return JSONResponse(status_code=500, content={"error": str(exc)})
