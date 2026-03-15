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


# ── Swing Trading Endpoints ───────────────────────────────────────────────────

@router.get("/swing/bots")
def swing_bots():
    from iol_engines.simulation.swing_bot_config import list_swing_presets
    return {"presets": [
        {"name": c.name, "description": c.description, "min_hold_days": c.min_hold_days,
         "max_hold_days": c.max_hold_days, "stop_loss_pct": c.stop_loss_pct,
         "take_profit_pct": c.take_profit_pct, "trailing_atr_mult": c.trailing_atr_mult,
         "max_positions": c.max_positions, "min_engine_score": c.min_engine_score}
        for c in list_swing_presets()
    ]}


@router.post("/swing/run")
def swing_run(
    background_tasks: BackgroundTasks,
    bot: str = "swing-balanced",
    date_from: str = "2025-01-01",
    date_to: str = "2026-01-01",
    initial_cash_ars: float = 1_000_000.0,
):
    from iol_cli.db import connect, init_db
    from iol_engines.simulation.swing_bot_config import get_swing_preset
    from iol_engines.simulation.swing_runner import run_swing_backtest, _create_run_row as _sw_create

    try:
        config = get_swing_preset(bot)
    except ValueError as exc:
        return JSONResponse(status_code=400, content={"error": str(exc)})

    try:
        db_path = dbmod.resolve_db_path()
        conn = connect(db_path)
        init_db(conn)
        run_id = _sw_create(conn, config.name, date_from, date_to, initial_cash_ars)
        conn.close()
    except Exception as exc:
        return JSONResponse(status_code=500, content={"error": str(exc)})

    def _run() -> None:
        try:
            c = connect(dbmod.resolve_db_path())
            init_db(c)
            run_swing_backtest(c, config, date_from, date_to, initial_cash_ars,
                               verbose=False, existing_run_id=run_id)
        except Exception:
            pass

    background_tasks.add_task(_run)
    return {"run_id": run_id, "status": "started", "bot": bot,
            "message": f"Swing backtest started. Poll GET /api/simulation/swing/runs/{run_id}"}


@router.get("/swing/runs")
def swing_runs(bot: Optional[str] = None, limit: int = 50):
    from iol_cli.db import connect, init_db
    try:
        db_path = dbmod.resolve_db_path()
        conn = connect(db_path)
        init_db(conn)
        where = "WHERE bot_name = ?" if bot else ""
        params = (bot, limit) if bot else (limit,)
        rows = conn.execute(
            f"SELECT id, bot_name, date_from, date_to, initial_cash, final_value, "
            f"total_return_pct, sharpe_ratio, max_drawdown_pct, win_rate_pct, "
            f"avg_hold_days, total_trades, mode, status, created_at "
            f"FROM swing_simulation_runs {where} ORDER BY id DESC LIMIT ?",
            params,
        ).fetchall()
        cols = ["id", "bot_name", "date_from", "date_to", "initial_cash", "final_value",
                "total_return_pct", "sharpe_ratio", "max_drawdown_pct", "win_rate_pct",
                "avg_hold_days", "total_trades", "mode", "status", "created_at"]
        return {"runs": [dict(zip(cols, r)) for r in rows]}
    except Exception as exc:
        return JSONResponse(status_code=500, content={"error": str(exc)})


@router.get("/swing/runs/{run_id}")
def swing_run_detail(run_id: int):
    import json as _json
    from iol_cli.db import connect, init_db
    try:
        db_path = dbmod.resolve_db_path()
        conn = connect(db_path)
        init_db(conn)
        row = conn.execute(
            "SELECT id, bot_name, date_from, date_to, initial_cash, final_value, "
            "total_return_pct, sharpe_ratio, max_drawdown_pct, win_rate_pct, "
            "avg_hold_days, total_trades, mode, status, created_at, plan_json "
            "FROM swing_simulation_runs WHERE id = ?", (run_id,)
        ).fetchone()
        if not row:
            return JSONResponse(status_code=404, content={"error": f"Run {run_id} not found"})
        cols = ["id", "bot_name", "date_from", "date_to", "initial_cash", "final_value",
                "total_return_pct", "sharpe_ratio", "max_drawdown_pct", "win_rate_pct",
                "avg_hold_days", "total_trades", "mode", "status", "created_at", "plan_json"]
        data = dict(zip(cols, row))
        if data.get("plan_json"):
            try:
                data["plan_json"] = _json.loads(data["plan_json"])
            except Exception:
                pass
        return data
    except Exception as exc:
        return JSONResponse(status_code=500, content={"error": str(exc)})


@router.get("/swing/runs/{run_id}/trades")
def swing_run_trades(run_id: int, limit: int = 200):
    from iol_cli.db import connect, init_db
    try:
        db_path = dbmod.resolve_db_path()
        conn = connect(db_path)
        init_db(conn)
        rows = conn.execute(
            "SELECT id, symbol, entry_date, exit_date, entry_price, exit_price, "
            "quantity, amount_ars, pnl_ars, return_pct, hold_days, exit_reason "
            "FROM swing_simulation_trades WHERE run_id = ? ORDER BY entry_date LIMIT ?",
            (run_id, limit),
        ).fetchall()
        cols = ["id", "symbol", "entry_date", "exit_date", "entry_price", "exit_price",
                "quantity", "amount_ars", "pnl_ars", "return_pct", "hold_days", "exit_reason"]
        return {"run_id": run_id, "count": len(rows), "trades": [dict(zip(cols, r)) for r in rows]}
    except Exception as exc:
        return JSONResponse(status_code=500, content={"error": str(exc)})


# ── Event-Driven Endpoints ────────────────────────────────────────────────────

@router.get("/event/bots")
def event_bots():
    from iol_engines.simulation.event_bot_config import list_event_presets
    return {"presets": [
        {"name": c.name, "description": c.description, "max_positions": c.max_positions,
         "cash_reserve_pct": c.cash_reserve_pct, "min_engine_score": c.min_engine_score,
         "hold_after_event_days": c.hold_after_event_days,
         "reaction_rules_count": len(c.reaction_rules)}
        for c in list_event_presets()
    ]}


@router.post("/event/run")
def event_run(
    background_tasks: BackgroundTasks,
    bot: str = "event-adaptive",
    date_from: str = "2025-01-01",
    date_to: str = "2026-01-01",
    initial_cash_ars: float = 1_000_000.0,
):
    from iol_cli.db import connect, init_db
    from iol_engines.simulation.event_bot_config import get_event_preset
    from iol_engines.simulation.event_runner import run_event_backtest, _create_run_row as _ev_create

    try:
        config = get_event_preset(bot)
    except ValueError as exc:
        return JSONResponse(status_code=400, content={"error": str(exc)})

    try:
        db_path = dbmod.resolve_db_path()
        conn = connect(db_path)
        init_db(conn)
        run_id = _ev_create(conn, config.name, date_from, date_to, initial_cash_ars)
        conn.close()
    except Exception as exc:
        return JSONResponse(status_code=500, content={"error": str(exc)})

    def _run() -> None:
        try:
            c = connect(dbmod.resolve_db_path())
            init_db(c)
            run_event_backtest(c, config, date_from, date_to, initial_cash_ars,
                               verbose=False, existing_run_id=run_id)
        except Exception:
            pass

    background_tasks.add_task(_run)
    return {"run_id": run_id, "status": "started", "bot": bot,
            "message": f"Event backtest started. Poll GET /api/simulation/event/runs/{run_id}"}


@router.get("/event/runs")
def event_runs(bot: Optional[str] = None, limit: int = 50):
    from iol_cli.db import connect, init_db
    try:
        db_path = dbmod.resolve_db_path()
        conn = connect(db_path)
        init_db(conn)
        where = "WHERE bot_name = ?" if bot else ""
        params = (bot, limit) if bot else (limit,)
        rows = conn.execute(
            f"SELECT id, bot_name, date_from, date_to, initial_cash, final_value, "
            f"total_return_pct, sharpe_ratio, max_drawdown_pct, win_rate_pct, "
            f"total_events_triggered, total_trades, mode, status, created_at "
            f"FROM event_simulation_runs {where} ORDER BY id DESC LIMIT ?",
            params,
        ).fetchall()
        cols = ["id", "bot_name", "date_from", "date_to", "initial_cash", "final_value",
                "total_return_pct", "sharpe_ratio", "max_drawdown_pct", "win_rate_pct",
                "total_events_triggered", "total_trades", "mode", "status", "created_at"]
        return {"runs": [dict(zip(cols, r)) for r in rows]}
    except Exception as exc:
        return JSONResponse(status_code=500, content={"error": str(exc)})


@router.get("/event/runs/{run_id}")
def event_run_detail(run_id: int):
    import json as _json
    from iol_cli.db import connect, init_db
    try:
        db_path = dbmod.resolve_db_path()
        conn = connect(db_path)
        init_db(conn)
        row = conn.execute(
            "SELECT id, bot_name, date_from, date_to, initial_cash, final_value, "
            "total_return_pct, sharpe_ratio, max_drawdown_pct, win_rate_pct, "
            "total_events_triggered, total_trades, mode, status, created_at, plan_json "
            "FROM event_simulation_runs WHERE id = ?", (run_id,)
        ).fetchone()
        if not row:
            return JSONResponse(status_code=404, content={"error": f"Run {run_id} not found"})
        cols = ["id", "bot_name", "date_from", "date_to", "initial_cash", "final_value",
                "total_return_pct", "sharpe_ratio", "max_drawdown_pct", "win_rate_pct",
                "total_events_triggered", "total_trades", "mode", "status", "created_at", "plan_json"]
        data = dict(zip(cols, row))
        if data.get("plan_json"):
            try:
                data["plan_json"] = _json.loads(data["plan_json"])
            except Exception:
                pass
        return data
    except Exception as exc:
        return JSONResponse(status_code=500, content={"error": str(exc)})


@router.get("/event/runs/{run_id}/trades")
def event_run_trades(run_id: int, limit: int = 200):
    from iol_cli.db import connect, init_db
    try:
        db_path = dbmod.resolve_db_path()
        conn = connect(db_path)
        init_db(conn)
        rows = conn.execute(
            "SELECT id, symbol, trade_date, action, quantity, price, amount_ars, "
            "pnl_ars, trigger_event_type, trigger_event_description, portfolio_value_after "
            "FROM event_simulation_trades WHERE run_id = ? ORDER BY rowid LIMIT ?",
            (run_id, limit),
        ).fetchall()
        cols = ["id", "symbol", "trade_date", "action", "quantity", "price", "amount_ars",
                "pnl_ars", "trigger_event_type", "trigger_event_description", "portfolio_value_after"]
        return {"run_id": run_id, "count": len(rows), "trades": [dict(zip(cols, r)) for r in rows]}
    except Exception as exc:
        return JSONResponse(status_code=500, content={"error": str(exc)})


@router.post("/swing/live-step")
def swing_live_step(as_of: Optional[str] = None, initial_cash_ars: float = 1_000_000.0):
    from datetime import date as _date
    from iol_cli.db import connect, init_db
    from iol_engines.simulation.swing_bot_config import list_swing_presets
    from iol_engines.simulation.swing_runner import run_swing_live_step
    target = as_of or _date.today().isoformat()
    try:
        db_path = dbmod.resolve_db_path()
        conn = connect(db_path)
        init_db(conn)
        bot_names = [c.name for c in list_swing_presets()]
        run_ids = run_swing_live_step(conn, bot_names, target, initial_cash_ars)
        return {"as_of": target, "run_ids": run_ids, "status": "stepped"}
    except Exception as exc:
        return JSONResponse(status_code=500, content={"error": str(exc)})


@router.post("/event/live-step")
def event_live_step(as_of: Optional[str] = None, initial_cash_ars: float = 1_000_000.0):
    from datetime import date as _date
    from iol_cli.db import connect, init_db
    from iol_engines.simulation.event_bot_config import list_event_presets
    from iol_engines.simulation.event_runner import run_event_live_step
    target = as_of or _date.today().isoformat()
    try:
        db_path = dbmod.resolve_db_path()
        conn = connect(db_path)
        init_db(conn)
        bot_names = [c.name for c in list_event_presets()]
        run_ids = run_event_live_step(conn, bot_names, target, initial_cash_ars)
        return {"as_of": target, "run_ids": run_ids, "status": "stepped"}
    except Exception as exc:
        return JSONResponse(status_code=500, content={"error": str(exc)})


@router.get("/live-summary")
def live_summary():
    """Return all running bots (swing + event) with their latest plan_json and engine signals."""
    import json as _json
    from iol_cli.db import connect, init_db

    try:
        db_path = dbmod.resolve_db_path()
        conn = connect(db_path)
        init_db(conn)

        swing_rows = conn.execute(
            "SELECT id, bot_name, initial_cash, final_value, total_return_pct, "
            "total_trades, mode, status, plan_json "
            "FROM swing_simulation_runs WHERE status = 'running' ORDER BY id DESC"
        ).fetchall()
        swing_cols = ["id", "bot_name", "initial_cash", "final_value", "total_return_pct",
                      "total_trades", "mode", "status", "plan_json"]

        event_rows = conn.execute(
            "SELECT id, bot_name, initial_cash, final_value, total_return_pct, "
            "total_trades, mode, status, plan_json "
            "FROM event_simulation_runs WHERE status = 'running' ORDER BY id DESC"
        ).fetchall()
        event_cols = ["id", "bot_name", "initial_cash", "final_value", "total_return_pct",
                      "total_trades", "mode", "status", "plan_json"]

        def _parse(row, cols):
            d = dict(zip(cols, row))
            if d.get("plan_json"):
                try:
                    d["plan_json"] = _json.loads(d["plan_json"])
                except Exception:
                    pass
            return d

        # Latest engine signals
        reg = conn.execute(
            "SELECT regime, regime_score, volatility_regime, as_of "
            "FROM engine_regime_snapshots ORDER BY as_of DESC LIMIT 1"
        ).fetchone()
        mac = conn.execute(
            "SELECT argentina_macro_stress, global_risk_on, as_of "
            "FROM engine_macro_snapshots ORDER BY as_of DESC LIMIT 1"
        ).fetchone()

        return {
            "swing_runs": [_parse(r, swing_cols) for r in swing_rows],
            "event_runs": [_parse(r, event_cols) for r in event_rows],
            "engine_signals": {
                "regime": reg[0] if reg else None,
                "regime_score": reg[1] if reg else None,
                "volatility_regime": reg[2] if reg else None,
                "regime_as_of": reg[3] if reg else None,
                "macro_stress": mac[0] if mac else None,
                "global_risk_on": mac[1] if mac else None,
                "macro_as_of": mac[2] if mac else None,
            },
        }
    except Exception as exc:
        return JSONResponse(status_code=500, content={"error": str(exc)})


@router.get("/event/detect")
def event_detect(as_of: Optional[str] = None):
    from datetime import date as _date
    from iol_cli.db import connect, init_db
    from iol_engines.simulation.event_detector import detect_all_events

    target = as_of or _date.today().isoformat()
    try:
        db_path = dbmod.resolve_db_path()
        conn = connect(db_path)
        init_db(conn)
        events = detect_all_events(conn, target)
        return {
            "as_of": target,
            "count": len(events),
            "events": [{"event_type": e.event_type, "severity": e.severity,
                        "symbol": e.symbol, "description": e.description,
                        "payload": e.payload} for e in events],
        }
    except Exception as exc:
        return JSONResponse(status_code=500, content={"error": str(exc)})
