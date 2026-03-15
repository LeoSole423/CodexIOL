"""Event-driven trading bot backtest and live-step runner.

Iterates daily over historical data; each day it:
  1. Detects engine events (regime change, macro spike, smart money flip, etc.)
  2. Maps events to reaction rules from EventBotConfig
  3. Executes buy/sell/trim actions on the simulated portfolio
  4. Persists to event_simulation_runs / event_simulation_trades
"""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from .event_bot_config import EventBotConfig, EventReactionRule, get_event_preset, list_event_presets
from .event_detector import EngineEvent, detect_all_events
from .metrics import EquityCurve, build_metrics_dict
from .portfolio_sim import SimulatedPortfolio, load_prices_for_date, load_trading_dates


# ── DB helpers ────────────────────────────────────────────────────────────────

def _load_opportunity_scores(
    conn: sqlite3.Connection, as_of: str
) -> List[Tuple[str, float]]:
    """Return [(symbol, score)] sorted by score desc from latest opportunity run."""
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id FROM advisor_opportunity_runs
        WHERE as_of <= ? AND status IN ('done', 'ok')
        ORDER BY as_of DESC, id DESC LIMIT 1
        """,
        (as_of,),
    )
    row = cur.fetchone()
    if not row:
        return []
    run_id = row[0]
    cur.execute(
        """
        SELECT symbol, score_total FROM advisor_opportunity_candidates
        WHERE run_id = ? AND score_total IS NOT NULL
        ORDER BY score_total DESC
        """,
        (run_id,),
    )
    return [(r[0], float(r[1])) for r in cur.fetchall()]


def _create_run_row(
    conn: sqlite3.Connection,
    bot_name: str,
    date_from: str,
    date_to: str,
    initial_cash: float,
    mode: str = "backtest",
) -> int:
    now = datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO event_simulation_runs
            (bot_name, date_from, date_to, initial_cash, mode, status, created_at)
        VALUES (?, ?, ?, ?, ?, 'running', ?)
        """,
        (bot_name, date_from, date_to, initial_cash, mode, now),
    )
    conn.commit()
    return cur.lastrowid or 0


def _persist_trade(
    conn: sqlite3.Connection,
    run_id: int,
    symbol: str,
    trade_date: str,
    action: str,
    quantity: float,
    price: float,
    amount_ars: float,
    pnl_ars: Optional[float],
    event_type: str,
    event_description: str,
    portfolio_value: float,
) -> None:
    conn.execute(
        """
        INSERT INTO event_simulation_trades
            (run_id, symbol, trade_date, action, quantity, price, amount_ars,
             pnl_ars, trigger_event_type, trigger_event_description, portfolio_value_after)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (run_id, symbol, trade_date, action, quantity, price,
         round(amount_ars, 2),
         round(pnl_ars, 2) if pnl_ars is not None else None,
         event_type, event_description, round(portfolio_value, 2)),
    )


def _finalize_run(
    conn: sqlite3.Connection,
    run_id: int,
    curve: EquityCurve,
    trade_pnls: List[float],
    total_traded: float,
    total_trades: int,
    total_events: int,
) -> None:
    metrics = build_metrics_dict(curve, trade_pnls, total_traded)
    final_value = curve[-1][1] if curve else 0.0
    conn.execute(
        """
        UPDATE event_simulation_runs SET
            final_value = ?,
            total_return_pct = ?,
            sharpe_ratio = ?,
            max_drawdown_pct = ?,
            win_rate_pct = ?,
            total_events_triggered = ?,
            total_trades = ?,
            status = 'done'
        WHERE id = ?
        """,
        (
            round(final_value, 2),
            metrics["total_return_pct"],
            metrics["sharpe_ratio"],
            metrics["max_drawdown_pct"],
            metrics["win_rate_pct"],
            total_events,
            total_trades,
            run_id,
        ),
    )
    conn.commit()


# ── Event reaction execution ──────────────────────────────────────────────────

def _apply_reaction(
    portfolio: SimulatedPortfolio,
    rule: EventReactionRule,
    event: EngineEvent,
    prices: Dict[str, float],
    opp_scores: List[Tuple[str, float]],
    config: EventBotConfig,
    conn: sqlite3.Connection,
    run_id: int,
    date: str,
    cost_basis: Dict[str, float],
    trade_pnls: List[float],
    total_traded_ref: List[float],
    total_trades_ref: List[int],
) -> None:
    """Execute a single reaction rule triggered by an event."""
    total_value = portfolio.mark_to_market(prices)

    if rule.reaction == "buy_top_candidates":
        candidates = [
            (sym, score) for sym, score in opp_scores
            if sym in prices
            and score >= config.min_engine_score
            and portfolio.n_positions < config.max_positions
        ][:rule.top_n]
        cash_to_deploy = total_value * rule.magnitude_pct
        per_position = cash_to_deploy / max(len(candidates), 1)
        min_cash = total_value * config.cash_reserve_pct

        for sym, _ in candidates:
            if portfolio.n_positions >= config.max_positions:
                break
            price = prices.get(sym)
            if not price:
                continue
            amount = min(per_position, portfolio.cash_ars - min_cash)
            if amount < 500:
                continue
            qty = portfolio.buy(sym, amount, price)
            cost_basis[sym] = price
            total_traded_ref[0] += amount
            total_trades_ref[0] += 1
            pv = portfolio.mark_to_market(prices)
            _persist_trade(conn, run_id, sym, date, "buy", qty, price, amount, None,
                           event.event_type, event.description, pv)

    elif rule.reaction == "trim_all":
        for sym in list(portfolio.holdings.keys()):
            price = prices.get(sym, 0)
            if not price:
                continue
            pos_val = portfolio.position_value(sym, price)
            trim_amount = pos_val * rule.magnitude_pct
            if trim_amount < 100:
                continue
            cb = cost_basis.get(sym, price)
            pnl = (price - cb) * (trim_amount / price)
            qty = portfolio.sell(sym, trim_amount, price)
            trade_pnls.append(pnl)
            total_traded_ref[0] += trim_amount
            total_trades_ref[0] += 1
            pv = portfolio.mark_to_market(prices)
            _persist_trade(conn, run_id, sym, date, "trim", qty, price, trim_amount,
                           pnl, event.event_type, event.description, pv)

    elif rule.reaction == "exit_all":
        for sym in list(portfolio.holdings.keys()):
            price = prices.get(sym, 0)
            if not price:
                continue
            pos_val = portfolio.position_value(sym, price)
            cb = cost_basis.get(sym, price)
            pnl = (price - cb) * portfolio.holdings[sym].quantity
            qty = portfolio.sell(sym, pos_val, price)
            trade_pnls.append(pnl)
            total_traded_ref[0] += pos_val
            total_trades_ref[0] += 1
            pv = portfolio.mark_to_market(prices)
            _persist_trade(conn, run_id, sym, date, "exit", qty, price, pos_val,
                           pnl, event.event_type, event.description, pv)

    elif rule.reaction == "increase_cash":
        target_cash = total_value * rule.target_cash_pct
        deficit = target_cash - portfolio.cash_ars
        if deficit <= 0:
            return
        # Liquidate smallest positions first to reach target
        positions_by_value = sorted(
            portfolio.holdings.items(),
            key=lambda kv: portfolio.position_value(kv[0], prices.get(kv[0], 0)),
        )
        remaining_deficit = deficit
        for sym, _ in positions_by_value:
            if remaining_deficit <= 0:
                break
            price = prices.get(sym, 0)
            if not price:
                continue
            pos_val = portfolio.position_value(sym, price)
            liquidate = min(pos_val, remaining_deficit)
            cb = cost_basis.get(sym, price)
            pnl = (price - cb) * (liquidate / price)
            qty = portfolio.sell(sym, liquidate, price)
            trade_pnls.append(pnl)
            total_traded_ref[0] += liquidate
            total_trades_ref[0] += 1
            remaining_deficit -= liquidate
            pv = portfolio.mark_to_market(prices)
            action = "exit" if liquidate >= pos_val * 0.95 else "trim"
            _persist_trade(conn, run_id, sym, date, action, qty, price, liquidate,
                           pnl, event.event_type, event.description, pv)

    elif rule.reaction == "buy_symbol":
        sym = event.symbol or rule.symbol
        if not sym or sym not in prices:
            return
        price = prices[sym]
        min_cash = total_value * config.cash_reserve_pct
        amount = min(total_value * rule.magnitude_pct, portfolio.cash_ars - min_cash)
        if amount < 500 or portfolio.n_positions >= config.max_positions:
            return
        qty = portfolio.buy(sym, amount, price)
        cost_basis[sym] = price
        total_traded_ref[0] += amount
        total_trades_ref[0] += 1
        pv = portfolio.mark_to_market(prices)
        _persist_trade(conn, run_id, sym, date, "buy", qty, price, amount, None,
                       event.event_type, event.description, pv)

    elif rule.reaction == "sell_symbol":
        sym = event.symbol or rule.symbol
        if not sym or sym not in portfolio.holdings:
            return
        price = prices.get(sym, 0)
        if not price:
            return
        pos_val = portfolio.position_value(sym, price)
        sell_amount = pos_val * rule.magnitude_pct
        if sell_amount < 100:
            return
        cb = cost_basis.get(sym, price)
        pnl = (price - cb) * (sell_amount / price)
        qty = portfolio.sell(sym, sell_amount, price)
        trade_pnls.append(pnl)
        total_traded_ref[0] += sell_amount
        total_trades_ref[0] += 1
        action = "exit" if rule.magnitude_pct >= 0.95 else "trim"
        pv = portfolio.mark_to_market(prices)
        _persist_trade(conn, run_id, sym, date, action, qty, price, sell_amount,
                       pnl, event.event_type, event.description, pv)

    conn.commit()


# ── Main runner ───────────────────────────────────────────────────────────────

def run_event_backtest(
    conn: sqlite3.Connection,
    config: EventBotConfig,
    date_from: str,
    date_to: str,
    initial_cash_ars: float,
    *,
    verbose: bool = True,
    existing_run_id: Optional[int] = None,
) -> int:
    """Run a full event-driven backtest. Returns event_simulation_runs.id."""

    def log(msg: str) -> None:
        if verbose:
            from rich.console import Console
            Console().print(msg)

    run_id = existing_run_id or _create_run_row(
        conn, config.name, date_from, date_to, initial_cash_ars
    )
    log(
        f"[bold]Event backtest run #{run_id}[/bold] bot=[cyan]{config.name}[/cyan] "
        f"{date_from} to {date_to}"
    )

    trading_dates = load_trading_dates(conn, date_from, date_to)
    if not trading_dates:
        _finalize_run(conn, run_id, [], [], 0.0, 0, 0)
        log("[red]No market data found in date range.[/red]")
        return run_id

    portfolio = SimulatedPortfolio(cash_ars=initial_cash_ars)
    curve: EquityCurve = []
    trade_pnls: List[float] = []
    total_traded_ref = [0.0]
    total_trades_ref = [0]
    total_events = 0
    cost_basis: Dict[str, float] = {}

    # Track last event date for cooldown
    last_event_date: Optional[str] = None

    # Build rule lookup: {event_type -> [rule, ...]}
    rules_by_event: Dict[str, List[EventReactionRule]] = {}
    for rule in config.reaction_rules:
        rules_by_event.setdefault(rule.event_type, []).append(rule)

    for i, date in enumerate(trading_dates):
        prices = load_prices_for_date(conn, date)
        if not prices:
            curve.append((date, portfolio.mark_to_market({})))
            continue

        opp_scores = _load_opportunity_scores(conn, date)
        total_value = portfolio.mark_to_market(prices)
        curve.append((date, total_value))

        # Check cooldown
        in_cooldown = (
            last_event_date is not None
            and _days_between(last_event_date, date) < config.hold_after_event_days
        )

        if not in_cooldown:
            events = detect_all_events(conn, date)
            triggered = [e for e in events if e.event_type in rules_by_event]

            if triggered:
                last_event_date = date
                total_events += len(triggered)

                for event in triggered:
                    for rule in rules_by_event.get(event.event_type, []):
                        # For symbol-specific rules, only apply if event.symbol matches or rule has no symbol
                        if rule.reaction in ("buy_symbol", "sell_symbol"):
                            if event.symbol and rule.symbol and rule.symbol != event.symbol:
                                continue
                        _apply_reaction(
                            portfolio, rule, event, prices, opp_scores, config,
                            conn, run_id, date, cost_basis, trade_pnls,
                            total_traded_ref, total_trades_ref,
                        )

        if i % 20 == 0:
            log(
                f"  {date}  value=[yellow]ARS {total_value:,.0f}[/yellow]  "
                f"cash={portfolio.cash_ars:,.0f}  positions={portfolio.n_positions}  "
                f"events={total_events}"
            )

    _finalize_run(
        conn, run_id, curve, trade_pnls,
        total_traded_ref[0], total_trades_ref[0], total_events,
    )
    final_val = curve[-1][1] if curve else initial_cash_ars
    ret = (final_val - initial_cash_ars) / initial_cash_ars * 100 if initial_cash_ars else 0
    log(
        f"\n[bold green]Event backtest complete.[/bold green] "
        f"Return: [{'green' if ret >= 0 else 'red'}]{ret:+.1f}%[/]  "
        f"Final: ARS {final_val:,.0f}  Events: {total_events}  Trades: {total_trades_ref[0]}"
    )
    return run_id


# ── Live step ─────────────────────────────────────────────────────────────────

def run_event_live_step(
    conn: sqlite3.Connection,
    bot_names: List[str],
    as_of: str,
    initial_cash_ars: float = 1_000_000.0,
    *,
    verbose: bool = True,
) -> List[int]:
    """Execute one daily event-driven step for each bot."""

    def log(msg: str) -> None:
        if verbose:
            from rich.console import Console
            Console().print(msg)

    events = detect_all_events(conn, as_of)
    log(
        f"[bold]Event live step[/bold] {as_of}  "
        f"events=[yellow]{len(events)}[/yellow]  bots={', '.join(bot_names)}"
    )

    prices = load_prices_for_date(conn, as_of)
    if not prices:
        log(f"[yellow]No price data for {as_of} -skipping.[/yellow]")
        return []

    run_ids = []
    for bot_name in bot_names:
        try:
            config = get_event_preset(bot_name)
        except ValueError:
            log(f"[red]Unknown event bot: {bot_name}[/red]")
            continue

        period = as_of[:7]
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id FROM event_simulation_runs
            WHERE bot_name = ? AND mode = 'live' AND date_from LIKE ?
            ORDER BY id DESC LIMIT 1
            """,
            (bot_name, f"{period}%"),
        )
        row = cur.fetchone()
        run_id = row[0] if row else _create_run_row(
            conn, bot_name, f"{period}-01", as_of, initial_cash_ars, mode="live"
        )

        # Reconstruct portfolio from trade history
        portfolio = SimulatedPortfolio(cash_ars=initial_cash_ars)
        cost_basis: Dict[str, float] = {}
        trade_rows = conn.execute(
            """
            SELECT symbol, action, quantity, price, amount_ars
            FROM event_simulation_trades
            WHERE run_id = ?
            ORDER BY rowid ASC
            """,
            (run_id,),
        ).fetchall()
        for sym, action, qty, price, amount in trade_rows:
            if action == "buy" and price:
                portfolio.buy(sym, float(amount), float(price))
                cost_basis[sym] = float(price)
            elif action in ("trim", "exit") and price:
                portfolio.sell(sym, float(amount), float(price))

        # Check cooldown
        last_event_row = conn.execute(
            """
            SELECT MAX(trade_date) FROM event_simulation_trades
            WHERE run_id = ? AND trigger_event_type IS NOT NULL
            """,
            (run_id,),
        ).fetchone()
        last_event_date = last_event_row[0] if last_event_row else None
        in_cooldown = (
            last_event_date is not None
            and _days_between(last_event_date, as_of) < config.hold_after_event_days
        )

        total_events_triggered = 0
        total_trades_ref = [0]
        trade_pnls: List[float] = []
        total_traded_ref = [0.0]

        if not in_cooldown:
            rules_by_event: Dict[str, List[EventReactionRule]] = {}
            for rule in config.reaction_rules:
                rules_by_event.setdefault(rule.event_type, []).append(rule)

            opp_scores = _load_opportunity_scores(conn, as_of)
            triggered = [e for e in events if e.event_type in rules_by_event]
            total_events_triggered = len(triggered)

            for event in triggered:
                for rule in rules_by_event.get(event.event_type, []):
                    _apply_reaction(
                        portfolio, rule, event, prices, opp_scores, config,
                        conn, run_id, as_of, cost_basis, trade_pnls,
                        total_traded_ref, total_trades_ref,
                    )

        final_val = portfolio.mark_to_market(prices)

        # Build plan_json from today's trades
        today_trades = conn.execute(
            "SELECT symbol, action, amount_ars, trigger_event_type FROM event_simulation_trades "
            "WHERE run_id = ? AND trade_date = ? ORDER BY rowid",
            (run_id, as_of),
        ).fetchall()
        step_entries = [
            {"symbol": r[0], "amount_ars": round(r[2] or 0, 2), "trigger": r[3]}
            for r in today_trades if r[1] == "buy"
        ]
        step_exits = [
            {"symbol": r[0], "action": r[1], "amount_ars": round(r[2] or 0, 2), "trigger": r[3]}
            for r in today_trades if r[1] in ("exit", "trim")
        ]

        # Derive regime from last engine snapshot
        _reg = conn.execute(
            "SELECT regime, regime_score, volatility_regime FROM engine_regime_snapshots "
            "ORDER BY as_of DESC LIMIT 1"
        ).fetchone()
        _mac = conn.execute(
            "SELECT argentina_macro_stress FROM engine_macro_snapshots ORDER BY as_of DESC LIMIT 1"
        ).fetchone()

        plan = {
            "as_of": as_of,
            "regime": _reg[0] if _reg else "unknown",
            "regime_score": round(float(_reg[1]), 1) if _reg else 50.0,
            "macro_stress": round(float(_mac[0]), 1) if _mac else 50.0,
            "events_triggered": total_events_triggered,
            "entries": step_entries,
            "exits": step_exits,
            "portfolio_value_ars": round(final_val, 2),
            "open_positions": list(portfolio.holdings.keys()),
        }

        conn.execute(
            "UPDATE event_simulation_runs SET final_value=?, total_return_pct=?, plan_json=? WHERE id=?",
            (
                round(final_val, 2),
                round((final_val - initial_cash_ars) / initial_cash_ars * 100, 2),
                json.dumps(plan),
                run_id,
            ),
        )
        conn.commit()
        log(
            f"  [cyan]{bot_name}[/cyan] #{run_id}  "
            f"value=[yellow]ARS {final_val:,.0f}[/yellow]  "
            f"positions={portfolio.n_positions}  "
            f"events_reacted={total_events_triggered}"
        )
        run_ids.append(run_id)

    return run_ids


def _days_between(date_from: str, date_to: str) -> int:
    from datetime import date
    try:
        return max(0, (date.fromisoformat(date_to) - date.fromisoformat(date_from)).days)
    except (ValueError, TypeError):
        return 0
