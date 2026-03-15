"""Simulation comparison report — load and compare multiple backtest runs."""
from __future__ import annotations

import json
import sqlite3
from typing import Any, Dict, List, Optional


def load_run(conn: sqlite3.Connection, run_id: int) -> Optional[Dict[str, Any]]:
    """Load a single simulation_runs row with its bot config."""
    cur = conn.cursor()
    cur.execute(
        """
        SELECT r.id, r.date_from, r.date_to, r.status, r.initial_value_ars,
               r.final_value_ars, r.total_return_pct, r.sharpe_ratio,
               r.max_drawdown_pct, r.metrics_json, r.error_message,
               r.created_at_utc,
               c.name AS bot_name, c.description AS bot_description, c.config_json
        FROM simulation_runs r
        JOIN simulation_bot_configs c ON c.id = r.bot_config_id
        WHERE r.id = ?
        """,
        (run_id,),
    )
    row = cur.fetchone()
    if row is None:
        return None
    cols = [d[0] for d in cur.description]
    d = dict(zip(cols, row))
    d["metrics"] = json.loads(d.pop("metrics_json") or "{}")
    d["bot_config"] = json.loads(d.pop("config_json") or "{}")
    return d


def list_runs(
    conn: sqlite3.Connection,
    limit: int = 50,
    bot_name: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Return summary rows for recent simulation runs."""
    cur = conn.cursor()
    where = "WHERE c.name = ?" if bot_name else ""
    params = (bot_name, limit) if bot_name else (limit,)
    cur.execute(
        f"""
        SELECT r.id, r.date_from, r.date_to, r.status, r.initial_value_ars,
               r.final_value_ars, r.total_return_pct, r.sharpe_ratio,
               r.max_drawdown_pct, r.created_at_utc,
               c.name AS bot_name
        FROM simulation_runs r
        JOIN simulation_bot_configs c ON c.id = r.bot_config_id
        {where}
        ORDER BY r.id DESC
        LIMIT ?
        """,
        params,
    )
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def compare_runs(conn: sqlite3.Connection, run_ids: List[int]) -> Dict[str, Any]:
    """Build a side-by-side comparison of multiple runs."""
    runs = [load_run(conn, rid) for rid in run_ids]
    runs = [r for r in runs if r is not None]
    if not runs:
        return {"error": "No valid run IDs found", "runs": []}

    # Rank by total_return_pct descending
    ranked = sorted(runs, key=lambda r: r.get("total_return_pct") or -999, reverse=True)

    def _summary(r: Dict) -> Dict:
        return {
            "run_id": r["id"],
            "bot_name": r["bot_name"],
            "date_from": r["date_from"],
            "date_to": r["date_to"],
            "status": r["status"],
            "initial_value_ars": r["initial_value_ars"],
            "final_value_ars": r["final_value_ars"],
            "total_return_pct": r["total_return_pct"],
            "sharpe_ratio": r["sharpe_ratio"],
            "max_drawdown_pct": r["max_drawdown_pct"],
            "win_rate_pct": r["metrics"].get("win_rate_pct"),
            "turnover_pct": r["metrics"].get("turnover_pct"),
            "n_days": r["metrics"].get("n_days"),
            "equity_curve": r["metrics"].get("equity_curve", []),
        }

    return {
        "run_ids": run_ids,
        "count": len(ranked),
        "winner": ranked[0]["bot_name"] if ranked else None,
        "runs": [_summary(r) for r in ranked],
    }


def load_trades(
    conn: sqlite3.Connection, run_id: int, limit: int = 200
) -> List[Dict[str, Any]]:
    """Return all paper trades for a simulation run."""
    cur = conn.cursor()
    cur.execute(
        """
        SELECT trade_date, symbol, action, quantity, price, amount_ars,
               portfolio_value_after, reason
        FROM simulation_trades
        WHERE run_id = ?
        ORDER BY trade_date, id
        LIMIT ?
        """,
        (run_id, limit),
    )
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]
