"""Simulation comparison report — load and compare multiple backtest runs."""
from __future__ import annotations

import json
import sqlite3
from typing import Any, Dict, List, Optional


def _decode_cost_model_json(value: Any) -> Dict[str, Any]:
    try:
        return json.loads(value or "{}")
    except Exception:
        return {}


def execution_quality_summary(conn: sqlite3.Connection, trade_table: str, run_id: int) -> Dict[str, Any]:
    rows = conn.execute(
        f"""
        SELECT gross_amount_ars, net_amount_ars, total_cost_ars, cost_model_json
        FROM {trade_table}
        WHERE run_id = ?
        """,
        (run_id,),
    ).fetchall()
    total = len(rows)
    with_costs = sum(1 for r in rows if r["total_cost_ars"] is not None)
    gross = sum(float(r["gross_amount_ars"] or 0.0) for r in rows)
    net = sum(float(r["net_amount_ars"] or 0.0) for r in rows)
    costs = sum(float(r["total_cost_ars"] or 0.0) for r in rows)
    price_sources: Dict[str, int] = {}
    liquidity_warnings: Dict[str, int] = {}
    for row in rows:
        meta = _decode_cost_model_json(row["cost_model_json"])
        source = str(meta.get("price_source") or "missing_price_source")
        warning = str(meta.get("liquidity_warning") or "none")
        price_sources[source] = price_sources.get(source, 0) + 1
        liquidity_warnings[warning] = liquidity_warnings.get(warning, 0) + 1
    return {
        "trades": total,
        "trades_with_costs": with_costs,
        "cost_coverage_pct": round(with_costs * 100.0 / total, 2) if total else 0.0,
        "gross_traded_ars": round(gross, 2),
        "net_traded_ars": round(net, 2),
        "total_costs_ars": round(costs, 2),
        "avg_cost_per_trade_ars": round(costs / total, 2) if total else 0.0,
        "cost_drag_pct_gross": round(costs * 100.0 / gross, 4) if gross else 0.0,
        "price_source_counts": price_sources,
        "liquidity_warning_counts": liquidity_warnings,
    }


def add_estimated_gross_return_fields(
    row: Dict[str, Any],
    *,
    initial_key: str,
    return_key: str = "total_return_pct",
) -> None:
    initial = float(row.get(initial_key) or 0.0)
    net_return = row.get(return_key)
    costs = float(row.get("total_costs_ars") or 0.0)
    drag_points = costs * 100.0 / initial if initial else 0.0
    row["cost_return_drag_pct_points"] = round(drag_points, 4)
    row["estimated_gross_return_pct"] = (
        round(float(net_return) + drag_points, 4) if net_return is not None else None
    )


def load_run(conn: sqlite3.Connection, run_id: int) -> Optional[Dict[str, Any]]:
    """Load a single simulation_runs row with its bot config."""
    cur = conn.cursor()
    cur.execute(
        """
        SELECT r.id, r.date_from, r.date_to, r.status, r.initial_value_ars,
               r.final_value_ars, r.total_return_pct, r.sharpe_ratio,
               r.max_drawdown_pct, r.metrics_json, r.error_message,
               r.win_rate_pct, r.total_trades, r.mode, r.created_at_utc,
               r.cost_model_version,
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
    d.update(execution_quality_summary(conn, "simulation_trades", run_id))
    add_estimated_gross_return_fields(d, initial_key="initial_value_ars")
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
               r.max_drawdown_pct, r.win_rate_pct, r.total_trades, r.mode, r.created_at_utc,
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
               portfolio_value_after, reason, total_cost_ars, execution_price,
               instrument_type, cost_model_json
        FROM simulation_trades
        WHERE run_id = ?
        ORDER BY trade_date, id
        LIMIT ?
        """,
        (run_id, limit),
    )
    cols = [d[0] for d in cur.description]
    trades = []
    for row in cur.fetchall():
        item = dict(zip(cols, row))
        meta = _decode_cost_model_json(item.pop("cost_model_json", None))
        item["price_source"] = meta.get("price_source")
        item["liquidity_warning"] = meta.get("liquidity_warning")
        trades.append(item)
    return trades
