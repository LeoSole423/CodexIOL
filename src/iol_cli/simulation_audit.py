"""Weekly simulation audit diagnostics for CLI automation."""
from __future__ import annotations

import json
import sqlite3
from collections import defaultdict
from datetime import date, timedelta
from typing import Any, Dict, Iterable, List, Optional


def _pct(numerator: float, denominator: float) -> float:
    return 0.0 if not denominator else numerator * 100.0 / denominator


def _avg(values: Iterable[Optional[float]]) -> Optional[float]:
    vals = [float(v) for v in values if v is not None]
    return None if not vals else sum(vals) / len(vals)


def _loads(value: Any) -> Dict[str, Any]:
    try:
        decoded = json.loads(value or "{}")
        return decoded if isinstance(decoded, dict) else {}
    except Exception:
        return {}


def _rowdict(row: sqlite3.Row) -> Dict[str, Any]:
    return {key: row[key] for key in row.keys()}


def _iso_days_before(value: Optional[str], days: int) -> str:
    try:
        base = date.fromisoformat(str(value))
    except (TypeError, ValueError):
        base = date.today()
    return (base - timedelta(days=days)).isoformat()


def _t1_pending_rows(conn: sqlite3.Connection, family: str) -> List[Dict[str, Any]]:
    specs = {
        "daily": (
            "simulation_pending_orders",
            "simulation_runs",
            "symbol, side, action, amount_ars, quantity, signal_price",
        ),
        "swing": (
            "swing_pending_orders",
            "swing_simulation_runs",
            "symbol, side, NULL AS action, amount_ars, quantity, signal_price",
        ),
        "event": (
            "event_pending_orders",
            "event_simulation_runs",
            "symbol, side, action, amount_ars, quantity, signal_price",
        ),
    }
    pending_table, run_table, select_cols = specs[family]
    rows = conn.execute(
        f"""
        SELECT p.id, p.run_id, r.status AS run_status, p.status AS order_status,
               p.signal_date, {select_cols}
        FROM {pending_table} p
        JOIN {run_table} r ON r.id = p.run_id
        WHERE p.status='pending'
        ORDER BY r.status, p.signal_date, p.id
        """
    ).fetchall()
    return [
        {
            "family": family,
            "id": row["id"],
            "run_id": row["run_id"],
            "run_status": row["run_status"],
            "order_status": row["order_status"],
            "signal_date": row["signal_date"],
            "symbol": row["symbol"],
            "side": row["side"],
            "action": row["action"],
            "amount_ars": row["amount_ars"],
            "quantity": row["quantity"],
            "signal_price": row["signal_price"],
        }
        for row in rows
    ]


def t1_execution_health(conn: sqlite3.Connection, as_of: str) -> Dict[str, Any]:
    """Diagnose pending T+1 orders and open-price readiness."""
    from iol_cli.snapshot import _collect_simulation_ohlcv_watchlist

    pending_by_family: Dict[str, Dict[str, Any]] = {}
    active_symbols: Dict[str, set[str]] = {}
    stale_pending_total = 0
    active_pending_total = 0
    all_pending: List[Dict[str, Any]] = []
    for family in ("daily", "swing", "event"):
        rows = _t1_pending_rows(conn, family)
        all_pending.extend(rows)
        active = [r for r in rows if r["run_status"] == "running"]
        stale = [r for r in rows if r["run_status"] == "stale"]
        active_pending_total += len(active)
        stale_pending_total += len(stale)
        active_symbols[family] = {str(r["symbol"]).upper() for r in active if r.get("symbol")}
        pending_by_family[family] = {
            "active_pending": len(active),
            "oldest_active_signal_date": min((r["signal_date"] for r in active), default=None),
            "stale_pending": len(stale),
            "oldest_stale_signal_date": min((r["signal_date"] for r in stale), default=None),
        }

    watchlist = _collect_simulation_ohlcv_watchlist(conn)
    symbols: Dict[str, Dict[str, Any]] = {}
    for sym, market in watchlist:
        symbols.setdefault(sym, {"symbol": sym, "market": market, "sources": set()})
        symbols[sym]["sources"].add("dynamic_ohlcv_watchlist")
    for family, family_symbols in active_symbols.items():
        for sym in family_symbols:
            symbols.setdefault(sym, {"symbol": sym, "market": None, "sources": set()})
            symbols[sym]["sources"].add(f"{family}_pending")

    for sym, info in symbols.items():
        open_row = conn.execute(
            "SELECT open FROM symbol_daily_ohlcv WHERE symbol=? AND trade_date=? AND open IS NOT NULL",
            (sym, as_of),
        ).fetchone()
        latest_open = conn.execute(
            """
            SELECT trade_date, open FROM symbol_daily_ohlcv
            WHERE symbol=? AND open IS NOT NULL
            ORDER BY trade_date DESC LIMIT 1
            """,
            (sym,),
        ).fetchone()
        latest_snapshot = conn.execute(
            """
            SELECT snapshot_date, last_price FROM market_symbol_snapshots
            WHERE symbol=? AND last_price IS NOT NULL AND last_price > 0
            ORDER BY snapshot_date DESC LIMIT 1
            """,
            (sym,),
        ).fetchone()
        info["has_open_as_of"] = bool(open_row)
        info["open_as_of"] = float(open_row[0]) if open_row and open_row[0] is not None else None
        info["latest_open_date"] = latest_open[0] if latest_open else None
        info["latest_open"] = float(latest_open[1]) if latest_open and latest_open[1] is not None else None
        info["latest_snapshot_date"] = latest_snapshot[0] if latest_snapshot else None
        info["latest_last_price"] = (
            float(latest_snapshot[1]) if latest_snapshot and latest_snapshot[1] is not None else None
        )
        info["would_fallback_on_as_of"] = (not info["has_open_as_of"]) and bool(latest_snapshot)
        info["missing_price_for_as_of"] = (not info["has_open_as_of"]) and (not latest_snapshot)
        info["sources"] = sorted(info["sources"])

    symbol_rows = sorted(symbols.values(), key=lambda r: (not r["has_open_as_of"], r["symbol"]))
    total_symbols = len(symbol_rows)
    open_ready = sum(1 for r in symbol_rows if r["has_open_as_of"])
    fallback = sum(1 for r in symbol_rows if r["would_fallback_on_as_of"])
    missing_price = sum(1 for r in symbol_rows if r["missing_price_for_as_of"])
    flags: List[str] = []
    if active_pending_total:
        flags.append("pending_t1_orders")
    if stale_pending_total:
        flags.append("stale_pending_orders")
    if total_symbols and fallback / total_symbols > 0.25:
        flags.append("fallback_last_price_high")
    if missing_price:
        flags.append("missing_t1_prices")

    return {
        "as_of": as_of,
        "pending_by_family": pending_by_family,
        "active_pending_total": active_pending_total,
        "stale_pending_total": stale_pending_total,
        "symbols_needed": total_symbols,
        "open_ready": open_ready,
        "open_ready_pct": round(_pct(open_ready, total_symbols), 2),
        "fallback_needed": fallback,
        "missing_price": missing_price,
        "flags": flags,
        "symbols": symbol_rows,
        "pending_orders": all_pending,
    }


def _trade_cost_stats(conn: sqlite3.Connection, table: str, run_id: int, date_expr: str, week_ago: str) -> Dict[str, Any]:
    rows = conn.execute(
        f"""
        SELECT total_cost_ars, gross_amount_ars, net_amount_ars, cost_model_json
        FROM {table}
        WHERE run_id = ?
        """,
        (run_id,),
    ).fetchall()
    week = conn.execute(
        f"""
        SELECT COUNT(*) AS trades_week, SUM(COALESCE(total_cost_ars, 0)) AS costs_week
        FROM {table}
        WHERE run_id = ? AND {date_expr} >= ?
        """,
        (run_id, week_ago),
    ).fetchone()

    price_sources: Dict[str, int] = {}
    liquidity_warnings: Dict[str, int] = {}
    with_costs = 0
    total_costs = 0.0
    gross = 0.0
    net = 0.0
    for row in rows:
        if row["total_cost_ars"] is not None:
            with_costs += 1
        total_costs += float(row["total_cost_ars"] or 0.0)
        gross += float(row["gross_amount_ars"] or 0.0)
        net += float(row["net_amount_ars"] or 0.0)
        meta = _loads(row["cost_model_json"])
        source = str(meta.get("price_source") or "missing_price_source")
        warning = str(meta.get("liquidity_warning") or "none")
        price_sources[source] = price_sources.get(source, 0) + 1
        liquidity_warnings[warning] = liquidity_warnings.get(warning, 0) + 1

    total = len(rows)
    avg_cost = total_costs / total if total else None
    return {
        "trades": total,
        "trades_week": week["trades_week"] or 0,
        "with_costs": with_costs,
        "null_costs": total - with_costs,
        "cost_coverage_pct": round(_pct(with_costs, total), 2),
        "total_costs_ars": round(total_costs, 2),
        "costs_week_ars": round(float(week["costs_week"] or 0.0), 2),
        "avg_cost_per_trade_ars": round(avg_cost, 2) if avg_cost is not None else None,
        "gross_traded_ars": round(gross, 2),
        "net_traded_ars": round(net, 2),
        "cost_drag_pct_gross": round(_pct(total_costs, gross), 4),
        "price_source_counts": price_sources,
        "liquidity_warning_counts": liquidity_warnings,
    }


def _cost_anomalies(row: Dict[str, Any], costs: Dict[str, Any]) -> List[str]:
    flags: List[str] = []
    if not row.get("cost_model_version"):
        flags.append("missing_cost_model_version")
        if row.get("status") == "stale":
            flags.append("legacy_gross_run")
    if row.get("cost_model_version") and costs["null_costs"]:
        flags.append("trade_costs_null")
    if costs["trades"] and costs["cost_coverage_pct"] < 95.0:
        flags.append("cost_coverage_low")
    source_counts = costs.get("price_source_counts", {})
    unknown_sources = {
        "same_day_snapshot",
        "market_symbol_snapshots",
        "missing_price_source",
    }
    if sum(source_counts.get(src, 0) for src in unknown_sources):
        flags.append("same_day_execution_or_unknown")
    if costs["trades"] and source_counts.get("fallback_last_price", 0) / costs["trades"] > 0.25:
        flags.append("fallback_last_price_high")
    return flags


def _duplicate_running_daily(conn: sqlite3.Connection, bot_name: str, period: str) -> int:
    return int(
        conn.execute(
            """
            SELECT COUNT(*)
            FROM simulation_runs r
            JOIN simulation_bot_configs c ON c.id = r.bot_config_id
            WHERE c.name = ? AND r.mode = 'live' AND r.status = 'running'
              AND r.date_from LIKE ?
            """,
            (bot_name, f"{period}%"),
        ).fetchone()[0]
        or 0
    )


def _run_flags(
    conn: sqlite3.Connection,
    row: Dict[str, Any],
    costs: Dict[str, Any],
    *,
    family: str,
    week_count: int,
    pending_count: int,
) -> List[str]:
    flags: List[str] = []
    status = str(row.get("status") or "")
    if status == "stale":
        flags.append("stale")
    if status == "running" and row.get("max_drawdown_pct") is None:
        flags.append("null_drawdown")
    if family == "event" and status == "running" and row.get("total_events_triggered") is None:
        flags.append("null_event_count")
    if pending_count:
        flags.append("pending_t1_orders")
    if status == "running":
        period = str(row.get("date_from") or "")[:7]
        dupes = 0
        if family == "daily":
            dupes = _duplicate_running_daily(conn, str(row.get("bot_name") or ""), period)
        else:
            table = "swing_simulation_runs" if family == "swing" else "event_simulation_runs"
            dupes = int(
                conn.execute(
                    f"""
                    SELECT COUNT(*) FROM {table}
                    WHERE bot_name = ? AND mode = 'live' AND status = 'running'
                      AND date_from LIKE ?
                    """,
                    (row.get("bot_name"), f"{period}%"),
                ).fetchone()[0]
                or 0
            )
        if dupes > 1:
            flags.append("duplicate_running")
        if week_count == 0 and pending_count == 0:
            flags.append("no_week_steps" if family == "swing" else "no_week_trades")
    flags.extend(_cost_anomalies(row, costs))
    return flags


def _pending_stats(conn: sqlite3.Connection, table: str, run_id: int) -> Dict[str, Any]:
    row = conn.execute(
        f"SELECT COUNT(*) AS count, MIN(signal_date) AS oldest FROM {table} WHERE run_id=? AND status='pending'",
        (run_id,),
    ).fetchone()
    return {"pending_t1": row["count"] or 0, "oldest_pending": row["oldest"]}


def _daily_runs(conn: sqlite3.Connection, week_ago: str) -> List[Dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT r.id, c.name AS bot_name, r.status, 'live' AS mode, r.date_from, r.date_to,
               r.final_value_ars, r.total_return_pct, r.sharpe_ratio,
               r.max_drawdown_pct, r.win_rate_pct, r.total_trades, r.cost_model_version
        FROM simulation_runs r
        JOIN simulation_bot_configs c ON c.id = r.bot_config_id
        WHERE r.mode = 'live'
        ORDER BY r.id DESC
        """
    ).fetchall()
    out: List[Dict[str, Any]] = []
    for raw in rows:
        row = _rowdict(raw)
        costs = _trade_cost_stats(conn, "simulation_trades", row["id"], "trade_date", week_ago)
        pending = _pending_stats(conn, "simulation_pending_orders", row["id"])
        last_trade = conn.execute(
            "SELECT MAX(trade_date) FROM simulation_trades WHERE run_id=?", (row["id"],)
        ).fetchone()[0]
        item = {
            "family": "daily",
            "bot_name": row["bot_name"],
            "run_id": row["id"],
            "status": row["status"],
            "date_from": row["date_from"],
            "date_to": row["date_to"],
            "return_pct": row["total_return_pct"],
            "final_value": row["final_value_ars"],
            "trades_week": costs["trades_week"],
            "total_trades": row["total_trades"],
            "last_trade": last_trade,
            "drawdown_pct": row["max_drawdown_pct"],
            "win_rate_pct": row["win_rate_pct"],
            "cost_model_version": row["cost_model_version"],
            **pending,
            **costs,
        }
        item["anomalies"] = _run_flags(
            conn, row, costs, family="daily", week_count=costs["trades_week"], pending_count=pending["pending_t1"]
        )
        out.append(item)
    return out


def _swing_runs(conn: sqlite3.Connection, week_ago: str) -> List[Dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT id, bot_name, status, mode, date_from, date_to, final_value,
               total_return_pct, sharpe_ratio, max_drawdown_pct, win_rate_pct,
               avg_hold_days, total_trades, plan_json, cost_model_version
        FROM swing_simulation_runs
        WHERE mode = 'live'
        ORDER BY id DESC
        """
    ).fetchall()
    out: List[Dict[str, Any]] = []
    for raw in rows:
        row = _rowdict(raw)
        costs = _trade_cost_stats(conn, "swing_simulation_trades", row["id"], "COALESCE(exit_date, entry_date)", week_ago)
        pending = _pending_stats(conn, "swing_pending_orders", row["id"])
        steps = conn.execute(
            "SELECT COUNT(*) FROM swing_simulation_steps WHERE run_id=? AND step_date>=?",
            (row["id"], week_ago),
        ).fetchone()[0]
        open_trades = conn.execute(
            "SELECT COUNT(*) FROM swing_simulation_trades WHERE run_id=? AND exit_date IS NULL",
            (row["id"],),
        ).fetchone()[0]
        closed_week = conn.execute(
            "SELECT COUNT(*) FROM swing_simulation_trades WHERE run_id=? AND exit_date>=?",
            (row["id"], week_ago),
        ).fetchone()[0]
        mtm = _loads(row["plan_json"]).get("open_positions_mark_to_market", [])
        item = {
            "family": "swing",
            "bot_name": row["bot_name"],
            "run_id": row["id"],
            "status": row["status"],
            "date_from": row["date_from"],
            "date_to": row["date_to"],
            "return_pct": row["total_return_pct"],
            "final_value": row["final_value"],
            "steps_week": steps or 0,
            "open_positions": open_trades or 0,
            "closed_week": closed_week or 0,
            "total_trades": row["total_trades"],
            "drawdown_pct": row["max_drawdown_pct"],
            "win_rate_pct": row["win_rate_pct"],
            "avg_hold_days": row["avg_hold_days"],
            "open_mtm_count": len(mtm) if isinstance(mtm, list) else 0,
            "cost_model_version": row["cost_model_version"],
            **pending,
            **costs,
        }
        item["anomalies"] = _run_flags(
            conn, row, costs, family="swing", week_count=steps or 0, pending_count=pending["pending_t1"]
        )
        out.append(item)
    return out


def _event_runs(conn: sqlite3.Connection, week_ago: str) -> List[Dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT id, bot_name, status, mode, date_from, date_to, final_value,
               total_return_pct, sharpe_ratio, max_drawdown_pct, win_rate_pct,
               total_events_triggered, total_trades, plan_json, cost_model_version
        FROM event_simulation_runs
        WHERE mode = 'live'
        ORDER BY id DESC
        """
    ).fetchall()
    out: List[Dict[str, Any]] = []
    for raw in rows:
        row = _rowdict(raw)
        costs = _trade_cost_stats(conn, "event_simulation_trades", row["id"], "trade_date", week_ago)
        pending = _pending_stats(conn, "event_pending_orders", row["id"])
        by_event: Dict[str, int] = defaultdict(int)
        for trade in conn.execute(
            "SELECT trigger_event_type FROM event_simulation_trades WHERE run_id=? AND trade_date>=?",
            (row["id"], week_ago),
        ):
            by_event[str(trade["trigger_event_type"])] += 1
        item = {
            "family": "event",
            "bot_name": row["bot_name"],
            "run_id": row["id"],
            "status": row["status"],
            "date_from": row["date_from"],
            "date_to": row["date_to"],
            "return_pct": row["total_return_pct"],
            "final_value": row["final_value"],
            "events_total": row["total_events_triggered"],
            "trades_week": costs["trades_week"],
            "event_trades_by_type_week": dict(by_event),
            "primary_event_week": max(by_event, key=by_event.get) if by_event else None,
            "total_trades": row["total_trades"],
            "drawdown_pct": row["max_drawdown_pct"],
            "win_rate_pct": row["win_rate_pct"],
            "cost_model_version": row["cost_model_version"],
            **pending,
            **costs,
        }
        item["anomalies"] = _run_flags(
            conn, row, costs, family="event", week_count=costs["trades_week"], pending_count=pending["pending_t1"]
        )
        out.append(item)
    return out


def _aggregate_cost_coverage(family: str, runs: List[Dict[str, Any]]) -> Dict[str, Any]:
    trades = sum(int(r.get("trades") or 0) for r in runs)
    with_costs = sum(int(r.get("with_costs") or 0) for r in runs)
    total_costs = sum(float(r.get("total_costs_ars") or 0.0) for r in runs)
    return {
        "family": family,
        "runs": len(runs),
        "runs_with_cost_model": sum(1 for r in runs if r.get("cost_model_version")),
        "trades": trades,
        "trades_with_costs": with_costs,
        "cost_coverage_pct": round(_pct(with_costs, trades), 2),
        "total_costs_ars": round(total_costs, 2),
        "avg_cost_per_trade_ars": round(total_costs / trades, 2) if trades else None,
    }


def _event_detection(conn: sqlite3.Connection, as_of: str) -> Dict[str, Any]:
    try:
        from iol_engines.simulation.event_detector import detect_all_events

        events = detect_all_events(conn, as_of=as_of)
        by_type: Dict[str, int] = defaultdict(int)
        for event in events:
            by_type[str(getattr(event, "event_type", "unknown"))] += 1
        return {"ok": True, "total": len(events), "by_type": dict(by_type)}
    except Exception as exc:
        return {"ok": False, "error_type": type(exc).__name__, "error": str(exc)}


def build_simulation_audit(
    conn: sqlite3.Connection,
    as_of: Optional[str] = None,
    week_days: int = 7,
    outcome_days: int = 14,
    include_symbols: bool = False,
) -> Dict[str, Any]:
    """Build the weekly simulation accuracy and health audit payload."""
    target = as_of or date.today().isoformat()
    week_ago = _iso_days_before(target, week_days)
    outcomes_since = _iso_days_before(target, outcome_days)

    market_dates = [
        r[0]
        for r in conn.execute(
            """
            SELECT DISTINCT snapshot_date FROM market_symbol_snapshots
            WHERE snapshot_date >= ? ORDER BY snapshot_date
            """,
            (week_ago,),
        )
    ]
    portfolio_dates = [
        r[0]
        for r in conn.execute(
            """
            SELECT DISTINCT snapshot_date FROM portfolio_snapshots
            WHERE snapshot_date >= ? ORDER BY snapshot_date
            """,
            (week_ago,),
        )
    ]

    date_row = conn.execute(
        "SELECT COUNT(DISTINCT snapshot_date), MIN(snapshot_date), MAX(snapshot_date) FROM market_symbol_snapshots"
    ).fetchone()
    sym_counts = conn.execute(
        "SELECT symbol, COUNT(DISTINCT snapshot_date) AS dias FROM market_symbol_snapshots GROUP BY symbol"
    ).fetchall()
    total_symbols = len(sym_counts)
    rsi_ready = sum(1 for r in sym_counts if r["dias"] >= 15)
    ma20_ready = sum(1 for r in sym_counts if r["dias"] >= 20)
    macd_ready = sum(1 for r in sym_counts if r["dias"] >= 35)

    outcome_rows = conn.execute(
        """
        SELECT horizon, eval_status, forward_return_pct, excess_return_pct, hit, notes_json
        FROM advisor_signal_outcomes
        WHERE as_of >= ?
        """,
        (outcomes_since,),
    ).fetchall()
    missing_by_reason: Dict[str, int] = {}
    for row in outcome_rows:
        if row["eval_status"] != "ok":
            reason = str(_loads(row["notes_json"]).get("missing_reason", "unknown"))
            missing_by_reason[reason] = missing_by_reason.get(reason, 0) + 1

    horizons: Dict[str, Dict[str, Any]] = {}
    for horizon in (1, 5):
        ok_rows = [r for r in outcome_rows if r["horizon"] == horizon and r["eval_status"] == "ok"]
        missing = [r for r in outcome_rows if r["horizon"] == horizon and r["eval_status"] != "ok"]
        hits = sum(1 for r in ok_rows if r["hit"] == 1)
        horizons[str(horizon)] = {
            "evaluated": len(ok_rows),
            "missing": len(missing),
            "hits": hits,
            "hit_rate_pct": round(_pct(hits, len(ok_rows)), 2),
            "avg_forward_return_pct": _avg([r["forward_return_pct"] for r in ok_rows]),
            "avg_excess_return_pct": _avg([r["excess_return_pct"] for r in ok_rows]),
        }
    missing_prices_total = conn.execute(
        "SELECT COUNT(*) FROM advisor_signal_outcomes WHERE eval_status = 'missing_prices'"
    ).fetchone()[0]

    h5_segments = [
        {
            "signal_family": r["signal_family"],
            "n": r["n"],
            "hits": r["hits"] or 0,
            "hit_rate_pct": round(_pct(r["hits"] or 0, r["n"]), 2),
            "avg_forward_return_pct": r["avg_forward"],
            "avg_excess_return_pct": r["avg_excess"],
        }
        for r in conn.execute(
            """
            SELECT signal_family,
                   COUNT(*) AS n,
                   SUM(CASE WHEN hit = 1 THEN 1 ELSE 0 END) AS hits,
                   AVG(forward_return_pct) AS avg_forward,
                   AVG(excess_return_pct) AS avg_excess
            FROM advisor_signal_outcomes
            WHERE as_of >= ? AND horizon = 5 AND eval_status = 'ok'
            GROUP BY signal_family
            ORDER BY n DESC, signal_family
            """,
            (outcomes_since,),
        ).fetchall()
    ]

    portfolio_rows = conn.execute(
        """
        SELECT snapshot_date, total_value FROM portfolio_snapshots
        WHERE snapshot_date >= ? ORDER BY snapshot_date
        """,
        (week_ago,),
    ).fetchall()
    portfolio_summary: Dict[str, Any] = {"snapshots": [_rowdict(r) for r in portfolio_rows]}
    if portfolio_rows:
        first = float(portfolio_rows[0]["total_value"] or 0.0)
        last = float(portfolio_rows[-1]["total_value"] or 0.0)
        portfolio_summary.update(
            {
                "date_from": portfolio_rows[0]["snapshot_date"],
                "date_to": portfolio_rows[-1]["snapshot_date"],
                "first_value": first,
                "last_value": last,
                "return_pct": (last / first - 1.0) * 100.0 if first else None,
            }
        )

    daily = _daily_runs(conn, week_ago)
    swing = _swing_runs(conn, week_ago)
    event = _event_runs(conn, week_ago)
    t1 = t1_execution_health(conn, target)
    if not include_symbols:
        t1 = dict(t1)
        t1.pop("symbols", None)

    data_flags: List[str] = []
    if len(market_dates) < 5:
        data_flags.append(f"market_snapshots_week_lt_5:{len(market_dates)}")
    if len(portfolio_dates) < 5:
        data_flags.append(f"portfolio_snapshots_week_lt_5:{len(portfolio_dates)}")
    ok_outcomes = sum(1 for r in outcome_rows if r["eval_status"] == "ok")
    if missing_prices_total > max(100, ok_outcomes * 2):
        data_flags.append(f"missing_prices_high:{missing_prices_total}")
    if total_symbols and macd_ready / total_symbols < 0.9:
        data_flags.append(f"macd_warmup_below_90pct:{macd_ready}/{total_symbols}")

    return {
        "as_of": target,
        "window": {"week_from": week_ago, "outcomes_from": outcomes_since},
        "snapshot_coverage": {
            "market_dates": market_dates,
            "market_snapshot_count": len(market_dates),
            "portfolio_dates": portfolio_dates,
            "portfolio_snapshot_count": len(portfolio_dates),
        },
        "price_warmup": {
            "distinct_dates": date_row[0],
            "first_date": date_row[1],
            "last_date": date_row[2],
            "total_symbols": total_symbols,
            "rsi_ready": rsi_ready,
            "ma20_ready": ma20_ready,
            "macd_ready": macd_ready,
            "macd_missing": total_symbols - macd_ready,
            "worst_missing_business_days_approx": max(
                (35 - r["dias"] for r in sym_counts if r["dias"] < 35),
                default=0,
            ),
        },
        "signal_outcomes": {
            "total_rows": len(outcome_rows),
            "missing_by_reason": missing_by_reason,
            "horizons": horizons,
            "missing_prices_total_historical": missing_prices_total,
        },
        "h5_segments": h5_segments,
        "weekly_regime": [
            _rowdict(r)
            for r in conn.execute(
                """
                SELECT as_of, regime, regime_score, confidence
                FROM engine_regime_snapshots
                WHERE as_of >= ? ORDER BY as_of
                """,
                (week_ago,),
            ).fetchall()
        ],
        "real_portfolio": portfolio_summary,
        "simulations": {"daily": daily, "swing": swing, "event": event},
        "net_cost_coverage": {
            "daily": _aggregate_cost_coverage("daily", daily),
            "swing": _aggregate_cost_coverage("swing", swing),
            "event": _aggregate_cost_coverage("event", event),
        },
        "t1_health": t1,
        "event_detection_today": _event_detection(conn, target),
        "data_quality_flags": data_flags,
    }
