"""Swing trading backtest and live-step runner.

Operates on daily price data from market_symbol_snapshots.
Holds positions for 3-10 days, using TA signals + engine signals for
entry/exit decisions.  Persists results to swing_simulation_runs/trades.
"""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from .bot_config import get_preset as get_daily_preset
from .metrics import EquityCurve, build_metrics_dict
from .portfolio_sim import SimulatedPortfolio, load_prices_for_date, load_trading_dates
from .swing_bot_config import SwingBotConfig, get_swing_preset, list_swing_presets
from .swing_indicators import PriceSeries, compute_swing_ta
from .swing_signals import OpenPosition, SwingSignal, classify_swing_signal


_TA_HISTORY_DAYS = 60   # Price bars needed for TA calculation


# ── DB helpers ────────────────────────────────────────────────────────────────

def _load_price_history(
    conn: sqlite3.Connection,
    symbol: str,
    as_of: str,
    days: int = _TA_HISTORY_DAYS,
) -> PriceSeries:
    """Load up to `days` most recent daily prices for symbol on or before as_of."""
    cur = conn.cursor()
    cur.execute(
        """
        SELECT snapshot_date, last_price
        FROM market_symbol_snapshots
        WHERE symbol = ? AND snapshot_date <= ? AND last_price > 0
        ORDER BY snapshot_date DESC LIMIT ?
        """,
        (symbol, as_of, days),
    )
    rows = cur.fetchall()
    return list(reversed([(r[0], float(r[1])) for r in rows]))


def _load_engine_signals(
    conn: sqlite3.Connection, as_of: str
) -> Tuple[Any, Any]:
    """Load the nearest cached regime and macro signals on or before as_of."""
    from iol_engines.macro.engine import MacroMomentumEngine
    from iol_engines.regime.engine import MarketRegimeEngine

    regime = MarketRegimeEngine().load_latest(conn, as_of)
    macro = MacroMomentumEngine().load_latest(conn, as_of)
    return regime, macro


def _load_opportunity_scores(
    conn: sqlite3.Connection, as_of: str
) -> Dict[str, float]:
    """Return {symbol: score_total} from the most recent opportunity run on/before as_of."""
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
        return {}
    run_id = row[0]
    cur.execute(
        """
        SELECT symbol, score_total
        FROM advisor_opportunity_candidates
        WHERE run_id = ? AND score_total IS NOT NULL
        ORDER BY score_total DESC
        """,
        (run_id,),
    )
    return {r[0]: float(r[1]) for r in cur.fetchall()}


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
        INSERT INTO swing_simulation_runs
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
    entry_date: str,
    exit_date: Optional[str],
    entry_price: float,
    exit_price: Optional[float],
    quantity: float,
    amount_ars: float,
    pnl_ars: Optional[float],
    hold_days: Optional[int],
    exit_reason: Optional[str],
    entry_signals: dict,
    exit_signals: Optional[dict],
) -> None:
    conn.execute(
        """
        INSERT INTO swing_simulation_trades
            (run_id, symbol, entry_date, exit_date, entry_price, exit_price,
             quantity, amount_ars, pnl_ars, return_pct, hold_days, exit_reason,
             entry_signals_json, exit_signals_json)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            run_id,
            symbol,
            entry_date,
            exit_date,
            entry_price,
            exit_price,
            quantity,
            amount_ars,
            round(pnl_ars, 2) if pnl_ars is not None else None,
            round(pnl_ars / amount_ars * 100, 2) if (pnl_ars is not None and amount_ars > 0) else None,
            hold_days,
            exit_reason,
            json.dumps(entry_signals),
            json.dumps(exit_signals) if exit_signals else None,
        ),
    )


def _finalize_run(
    conn: sqlite3.Connection,
    run_id: int,
    curve: EquityCurve,
    trade_pnls: List[float],
    trade_hold_days: List[int],
    total_traded: float,
    total_trades: int,
    *,
    error: Optional[str] = None,
) -> None:
    metrics = build_metrics_dict(curve, trade_pnls, total_traded)
    final_value = curve[-1][1] if curve else 0.0
    avg_hold = sum(trade_hold_days) / len(trade_hold_days) if trade_hold_days else None
    conn.execute(
        """
        UPDATE swing_simulation_runs SET
            final_value = ?,
            total_return_pct = ?,
            sharpe_ratio = ?,
            max_drawdown_pct = ?,
            win_rate_pct = ?,
            avg_hold_days = ?,
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
            round(avg_hold, 1) if avg_hold else None,
            total_trades,
            run_id,
        ),
    )
    conn.commit()


# ── Main runner ───────────────────────────────────────────────────────────────

def run_swing_backtest(
    conn: sqlite3.Connection,
    config: SwingBotConfig,
    date_from: str,
    date_to: str,
    initial_cash_ars: float,
    *,
    verbose: bool = True,
    existing_run_id: Optional[int] = None,
) -> int:
    """Run a full swing trading backtest. Returns swing_simulation_runs.id."""

    def log(msg: str) -> None:
        if verbose:
            from rich.console import Console
            Console().print(msg)

    run_id = existing_run_id or _create_run_row(
        conn, config.name, date_from, date_to, initial_cash_ars
    )
    log(
        f"[bold]Swing backtest run #{run_id}[/bold] bot=[cyan]{config.name}[/cyan] "
        f"{date_from} to {date_to}"
    )

    trading_dates = load_trading_dates(conn, date_from, date_to)
    if not trading_dates:
        _finalize_run(conn, run_id, [], [], [], 0.0, 0)
        log("[red]No market data found in date range.[/red]")
        return run_id

    portfolio = SimulatedPortfolio(cash_ars=initial_cash_ars)
    curve: EquityCurve = []
    trade_pnls: List[float] = []
    trade_hold_days: List[int] = []
    total_traded = 0.0
    total_trades = 0

    # Open positions state: {symbol -> OpenPosition}
    open_positions: Dict[str, OpenPosition] = {}
    # Track entry amounts for PnL calculation: {symbol -> amount_ars_invested}
    entry_amounts: Dict[str, float] = {}
    # Track entry quantity: {symbol -> quantity}
    entry_quantities: Dict[str, float] = {}
    # Track entry signals for trade record: {symbol -> dict}
    entry_signals_log: Dict[str, dict] = {}

    # Cached engine signals (refresh every 5 trading days)
    regime, macro = None, None
    _ENGINE_REFRESH = 5

    for i, date in enumerate(trading_dates):
        if i % _ENGINE_REFRESH == 0:
            regime, macro = _load_engine_signals(conn, date)

        regime_score = float(regime.regime_score) if regime else 50.0
        macro_stress = float(macro.argentina_macro_stress) if macro else 50.0

        prices = load_prices_for_date(conn, date)
        if not prices:
            curve.append((date, portfolio.mark_to_market({})))
            continue

        opp_scores = _load_opportunity_scores(conn, date)

        # ── Step 1: Update hold days and peak prices for open positions ────────
        for symbol, pos in list(open_positions.items()):
            current_price = prices.get(symbol)
            if current_price and current_price > pos.peak_price:
                open_positions[symbol].peak_price = current_price
            open_positions[symbol].days_held += 1

        # ── Step 2: Evaluate exits for open positions ──────────────────────────
        for symbol in list(open_positions.items()):
            sym = symbol[0]
            pos = symbol[1]
            current_price = prices.get(sym)
            if not current_price:
                continue

            price_history = _load_price_history(conn, sym, date)
            ta = compute_swing_ta(sym, price_history)
            engine_score = opp_scores.get(sym, 0.0)

            signal = classify_swing_signal(
                ta, engine_score, regime_score, macro_stress, pos, config
            )

            if signal.action == "exit":
                qty = entry_quantities.get(sym, 0.0)
                proceeds = qty * current_price
                cost = entry_amounts.get(sym, proceeds)
                pnl = proceeds - cost
                hold_days = pos.days_held

                portfolio.sell(sym, proceeds, current_price)
                total_traded += proceeds
                total_trades += 1
                trade_pnls.append(pnl)
                trade_hold_days.append(hold_days)

                # Determine canonical exit_reason from signal reason prefix
                reason_str = signal.reason
                exit_reason = "signal_exit"
                for prefix in ("stop_loss", "take_profit", "trailing_stop", "time_stop",
                               "rsi_overbought"):
                    if reason_str.startswith(prefix):
                        exit_reason = prefix
                        break

                _persist_trade(
                    conn, run_id, sym,
                    entry_date=pos.entry_date,
                    exit_date=date,
                    entry_price=pos.entry_price,
                    exit_price=current_price,
                    quantity=qty,
                    amount_ars=cost,
                    pnl_ars=pnl,
                    hold_days=hold_days,
                    exit_reason=exit_reason,
                    entry_signals=entry_signals_log.get(sym, {}),
                    exit_signals={"reason": reason_str, "conviction": signal.conviction},
                )
                conn.commit()

                del open_positions[sym]
                entry_amounts.pop(sym, None)
                entry_quantities.pop(sym, None)
                entry_signals_log.pop(sym, None)

        # ── Step 3: Evaluate entries for new positions ─────────────────────────
        total_value = portfolio.mark_to_market(prices)
        available_slots = config.max_positions - len(open_positions)
        min_cash = total_value * config.cash_reserve_pct

        if available_slots > 0 and portfolio.cash_ars > min_cash:
            # Sort candidates by opportunity score descending
            candidates = sorted(
                [(sym, score) for sym, score in opp_scores.items()
                 if sym not in open_positions and sym in prices],
                key=lambda x: x[1],
                reverse=True,
            )

            for sym, engine_score in candidates[:20]:
                if len(open_positions) >= config.max_positions:
                    break
                current_price = prices.get(sym)
                if not current_price:
                    continue

                price_history = _load_price_history(conn, sym, date)
                ta = compute_swing_ta(sym, price_history)

                signal = classify_swing_signal(
                    ta, engine_score, regime_score, macro_stress, None, config
                )

                if signal.action != "entry":
                    continue

                # Compute position size
                position_budget = total_value * config.position_size_pct
                max_cash_to_use = portfolio.cash_ars - min_cash
                amount = min(position_budget, max_cash_to_use)
                if amount < 500:
                    continue

                qty = portfolio.buy(sym, amount, current_price)
                total_traded += amount
                total_trades += 1

                open_positions[sym] = OpenPosition(
                    symbol=sym,
                    entry_price=current_price,
                    entry_date=date,
                    days_held=0,
                    peak_price=current_price,
                    engine_score=engine_score,
                )
                entry_amounts[sym] = amount
                entry_quantities[sym] = qty
                entry_signals_log[sym] = {
                    "reason": signal.reason,
                    "conviction": signal.conviction,
                    "engine_score": engine_score,
                    "regime_score": regime_score,
                    "macro_stress": macro_stress,
                }

                pv = portfolio.mark_to_market(prices)
                # Record entry as open trade (exit_date = None until closed)
                _persist_trade(
                    conn, run_id, sym,
                    entry_date=date,
                    exit_date=None,
                    entry_price=current_price,
                    exit_price=None,
                    quantity=qty,
                    amount_ars=amount,
                    pnl_ars=None,
                    hold_days=None,
                    exit_reason=None,
                    entry_signals=entry_signals_log[sym],
                    exit_signals=None,
                )
                conn.commit()

        # ── Step 4: Mark-to-market ─────────────────────────────────────────────
        total_value = portfolio.mark_to_market(prices)
        curve.append((date, total_value))

        if i % 20 == 0:
            log(
                f"  {date}  value=[yellow]ARS {total_value:,.0f}[/yellow]  "
                f"cash={portfolio.cash_ars:,.0f}  positions={len(open_positions)}"
            )

    # Force-close all remaining positions on last day
    if open_positions and trading_dates:
        last_date = trading_dates[-1]
        last_prices = load_prices_for_date(conn, last_date)
        for sym, pos in list(open_positions.items()):
            current_price = last_prices.get(sym, pos.entry_price)
            qty = entry_quantities.get(sym, 0.0)
            proceeds = qty * current_price
            cost = entry_amounts.get(sym, proceeds)
            pnl = proceeds - cost
            hold_days = pos.days_held

            portfolio.sell(sym, proceeds, current_price)
            total_traded += proceeds
            total_trades += 1
            trade_pnls.append(pnl)
            trade_hold_days.append(hold_days)

            # Update the open trade record with exit info
            conn.execute(
                """
                UPDATE swing_simulation_trades SET
                    exit_date = ?, exit_price = ?, pnl_ars = ?,
                    return_pct = ?, hold_days = ?, exit_reason = 'time_stop'
                WHERE run_id = ? AND symbol = ? AND exit_date IS NULL
                """,
                (
                    last_date,
                    current_price,
                    round(pnl, 2),
                    round(pnl / cost * 100, 2) if cost > 0 else 0,
                    hold_days,
                    run_id, sym,
                ),
            )
        conn.commit()

    _finalize_run(conn, run_id, curve, trade_pnls, trade_hold_days, total_traded, total_trades)

    final_val = curve[-1][1] if curve else initial_cash_ars
    ret = (final_val - initial_cash_ars) / initial_cash_ars * 100 if initial_cash_ars else 0
    log(
        f"\n[bold green]Swing backtest complete.[/bold green] "
        f"Return: [{'green' if ret >= 0 else 'red'}]{ret:+.1f}%[/]  "
        f"Final: ARS {final_val:,.0f}  Trades: {total_trades}  "
        f"({len(trading_dates)} trading days)"
    )
    return run_id


# ── Live step ─────────────────────────────────────────────────────────────────

def run_swing_live_step(
    conn: sqlite3.Connection,
    bot_names: List[str],
    as_of: str,
    initial_cash_ars: float = 1_000_000.0,
    *,
    verbose: bool = True,
) -> List[int]:
    """Execute one daily paper-trading step for each swing bot.

    Finds or creates a monthly live run, reconstructs open positions from
    existing trades, then evaluates exits and entries for today.

    Returns list of run_ids processed.
    """

    def log(msg: str) -> None:
        if verbose:
            from rich.console import Console
            Console().print(msg)

    regime, macro = _load_engine_signals(conn, as_of)
    regime_label = regime.regime if regime else "unknown"
    log(
        f"[bold]Swing live step[/bold] {as_of}  regime=[cyan]{regime_label}[/cyan]  "
        f"bots={', '.join(bot_names)}"
    )

    prices = load_prices_for_date(conn, as_of)
    if not prices:
        log(f"[yellow]No price data for {as_of} -skipping.[/yellow]")
        return []

    run_ids = []
    for bot_name in bot_names:
        try:
            config = get_swing_preset(bot_name)
        except ValueError:
            log(f"[red]Unknown swing bot: {bot_name}[/red]")
            continue

        # Find or create monthly live run
        period = as_of[:7]
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id FROM swing_simulation_runs
            WHERE bot_name = ? AND mode = 'live' AND date_from LIKE ?
            ORDER BY id DESC LIMIT 1
            """,
            (bot_name, f"{period}%"),
        )
        row = cur.fetchone()
        if row:
            run_id = row[0]
            conn.execute(
                "UPDATE swing_simulation_runs SET date_from = MIN(date_from, ?) WHERE id = ?",
                (f"{period}-01", run_id),
            )
        else:
            run_id = _create_run_row(
                conn, bot_name, f"{period}-01", as_of, initial_cash_ars, mode="live"
            )

        # Check if already executed today
        already = conn.execute(
            "SELECT 1 FROM swing_simulation_trades WHERE run_id=? AND entry_date=? LIMIT 1",
            (run_id, as_of),
        ).fetchone()
        if already:
            log(f"  [dim]{bot_name}[/dim] -step already executed for {as_of}, skipping.")
            run_ids.append(run_id)
            continue

        # Reconstruct portfolio and open positions from trade history
        portfolio = SimulatedPortfolio(cash_ars=initial_cash_ars)
        open_positions: Dict[str, OpenPosition] = {}
        entry_amounts: Dict[str, float] = {}
        entry_quantities: Dict[str, float] = {}

        trade_rows = conn.execute(
            """
            SELECT symbol, entry_date, entry_price, exit_date, quantity, amount_ars
            FROM swing_simulation_trades
            WHERE run_id = ?
            ORDER BY rowid ASC
            """,
            (run_id,),
        ).fetchall()

        for sym, entry_date, entry_price, exit_date, qty, amount in trade_rows:
            if exit_date is None:
                # Still open -buy it back into portfolio
                portfolio.buy(sym, float(amount), float(entry_price))
                entry_price_f = float(entry_price)
                current_p = prices.get(sym, entry_price_f)
                days_held = _days_between(entry_date, as_of)
                open_positions[sym] = OpenPosition(
                    symbol=sym,
                    entry_price=entry_price_f,
                    entry_date=entry_date,
                    days_held=days_held,
                    peak_price=max(entry_price_f, current_p),
                    engine_score=0.0,
                )
                entry_amounts[sym] = float(amount)
                entry_quantities[sym] = float(qty)
            else:
                # Closed -replay the sell to keep cash correct
                portfolio.sell(sym, float(amount), float(entry_price))

        regime_score = float(regime.regime_score) if regime else 50.0
        macro_stress = float(macro.argentina_macro_stress) if macro else 50.0
        opp_scores = _load_opportunity_scores(conn, as_of)

        step_entries: List[dict] = []
        step_exits: List[dict] = []

        # Process exits
        for sym, pos in list(open_positions.items()):
            current_price = prices.get(sym)
            if not current_price:
                continue
            price_history = _load_price_history(conn, sym, as_of)
            ta = compute_swing_ta(sym, price_history)
            engine_score = opp_scores.get(sym, 0.0)
            signal = classify_swing_signal(ta, engine_score, regime_score, macro_stress, pos, config)
            if signal.action == "exit":
                qty = entry_quantities.get(sym, 0.0)
                proceeds = qty * current_price
                cost = entry_amounts.get(sym, proceeds)
                pnl = proceeds - cost
                portfolio.sell(sym, proceeds, current_price)
                reason_str = signal.reason
                exit_reason = "signal_exit"
                for prefix in ("stop_loss", "take_profit", "trailing_stop", "time_stop", "rsi_overbought"):
                    if reason_str.startswith(prefix):
                        exit_reason = prefix
                        break
                conn.execute(
                    """
                    UPDATE swing_simulation_trades SET
                        exit_date=?, exit_price=?, pnl_ars=?, return_pct=?,
                        hold_days=?, exit_reason=?,
                        exit_signals_json=?
                    WHERE run_id=? AND symbol=? AND exit_date IS NULL
                    """,
                    (
                        as_of, current_price, round(pnl, 2),
                        round(pnl / cost * 100, 2) if cost > 0 else 0,
                        pos.days_held, exit_reason,
                        json.dumps({"reason": reason_str}),
                        run_id, sym,
                    ),
                )
                del open_positions[sym]
                step_exits.append({
                    "symbol": sym,
                    "reason": exit_reason,
                    "pnl_ars": round(pnl, 2),
                    "return_pct": round(pnl / cost * 100, 2) if cost > 0 else 0,
                })

        # Process entries
        total_value = portfolio.mark_to_market(prices)
        min_cash = total_value * config.cash_reserve_pct
        if len(open_positions) < config.max_positions and portfolio.cash_ars > min_cash:
            candidates = sorted(
                [(sym, score) for sym, score in opp_scores.items()
                 if sym not in open_positions and sym in prices],
                key=lambda x: x[1], reverse=True,
            )
            for sym, engine_score in candidates[:20]:
                if len(open_positions) >= config.max_positions:
                    break
                current_price = prices.get(sym)
                if not current_price:
                    continue
                price_history = _load_price_history(conn, sym, as_of)
                ta = compute_swing_ta(sym, price_history)
                signal = classify_swing_signal(ta, engine_score, regime_score, macro_stress, None, config)
                if signal.action != "entry":
                    continue
                position_budget = total_value * config.position_size_pct
                amount = min(position_budget, portfolio.cash_ars - min_cash)
                if amount < 500:
                    continue
                qty = portfolio.buy(sym, amount, current_price)
                open_positions[sym] = OpenPosition(
                    symbol=sym, entry_price=current_price, entry_date=as_of,
                    days_held=0, peak_price=current_price, engine_score=engine_score,
                )
                entry_amounts[sym] = amount
                entry_quantities[sym] = qty
                _persist_trade(
                    conn, run_id, sym,
                    entry_date=as_of, exit_date=None,
                    entry_price=current_price, exit_price=None,
                    quantity=qty, amount_ars=amount,
                    pnl_ars=None, hold_days=None, exit_reason=None,
                    entry_signals={"reason": signal.reason, "conviction": signal.conviction},
                    exit_signals=None,
                )
                step_entries.append({
                    "symbol": sym,
                    "amount_ars": round(amount, 2),
                    "price": round(current_price, 2),
                    "reason": signal.reason,
                    "score": round(engine_score, 1),
                })

        final_val = portfolio.mark_to_market(prices)
        plan = {
            "as_of": as_of,
            "regime": regime.regime if regime else "unknown",
            "regime_score": round(regime_score, 1),
            "macro_stress": round(macro_stress, 1),
            "entries": step_entries,
            "exits": step_exits,
            "portfolio_value_ars": round(final_val, 2),
            "open_positions": list(open_positions.keys()),
        }
        conn.execute(
            "UPDATE swing_simulation_runs SET final_value=?, total_return_pct=?, plan_json=? WHERE id=?",
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
            f"positions={len(open_positions)}"
        )
        run_ids.append(run_id)

    return run_ids


def _days_between(date_from: str, date_to: str) -> int:
    """Count calendar days between two ISO date strings."""
    from datetime import date
    try:
        d1 = date.fromisoformat(date_from)
        d2 = date.fromisoformat(date_to)
        return max(0, (d2 - d1).days)
    except (ValueError, TypeError):
        return 0
