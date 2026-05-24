"""Swing trading backtest and live-step runner.

Operates on daily price data from market_symbol_snapshots.
Holds positions for 3-10 days, using TA signals + engine signals for
entry/exit decisions.  Persists results to swing_simulation_runs/trades.

Execution model (realistic):
- Signals are generated using today's close price (T).
- Trades execute at the NEXT day's open price (T+1) from symbol_daily_ohlcv.
- Commission and slippage are applied via SimulatedPortfolio.
"""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from .bot_config import get_preset as get_daily_preset
from .metrics import EquityCurve, build_metrics_dict
from .portfolio_sim import Position, SimulatedPortfolio, load_execution_metadata_for_date, load_prices_for_date, load_trading_dates
from .cost_model import ExecutionCostModel, ExecutionFill
from .swing_bot_config import SwingBotConfig, get_swing_preset, list_swing_presets
from .swing_indicators import PriceSeries, compute_swing_ta
from .swing_signals import OpenPosition, SwingSignal, classify_swing_signal


_TA_HISTORY_DAYS = 60   # Price bars needed for TA calculation
_BENCHMARK_SYMBOL = "SPY"


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


def _load_open_price(
    conn: sqlite3.Connection, symbol: str, date: str
) -> Optional[float]:
    """Return open price from symbol_daily_ohlcv for the given date."""
    row = conn.execute(
        "SELECT open FROM symbol_daily_ohlcv WHERE symbol=? AND trade_date=?",
        (symbol, date),
    ).fetchone()
    return float(row[0]) if row and row[0] else None


def _get_benchmark_price(
    conn: sqlite3.Connection, symbol: str, date: str
) -> Optional[float]:
    """Return the most recent last_price for symbol on or before date."""
    row = conn.execute(
        """
        SELECT last_price FROM market_symbol_snapshots
        WHERE symbol=? AND snapshot_date<=? AND last_price>0
        ORDER BY snapshot_date DESC LIMIT 1
        """,
        (symbol, date),
    ).fetchone()
    return float(row[0]) if row else None


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
          AND candidate_status NOT IN ('suppressed', 'rejected')
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
    cost_model_version: Optional[str] = None,
) -> int:
    now = datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO swing_simulation_runs
            (bot_name, date_from, date_to, initial_cash, mode, status, created_at, cost_model_version)
        VALUES (?, ?, ?, ?, ?, 'running', ?, ?)
        """,
        (bot_name, date_from, date_to, initial_cash, mode, now, cost_model_version),
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
    fill: Optional[ExecutionFill] = None,
) -> None:
    fill_values = (
        fill.gross_amount_ars if fill else None,
        fill.net_amount_ars if fill else None,
        fill.commission_ars if fill else None,
        fill.market_fee_ars if fill else None,
        fill.iva_ars if fill else None,
        fill.slippage_ars if fill else None,
        fill.total_cost_ars if fill else None,
        fill.effective_price if fill else None,
        fill.instrument_type if fill else None,
        fill.to_json() if fill else None,
    )
    conn.execute(
        """
        INSERT INTO swing_simulation_trades
            (run_id, symbol, entry_date, exit_date, entry_price, exit_price,
             quantity, amount_ars, pnl_ars, return_pct, hold_days, exit_reason,
             entry_signals_json, exit_signals_json, gross_amount_ars, net_amount_ars,
             commission_ars, market_fee_ars, iva_ars, slippage_ars, total_cost_ars,
             execution_price, instrument_type, cost_model_json)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
            *fill_values,
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
    benchmark_return_pct: Optional[float] = None,
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
            benchmark_symbol = ?,
            benchmark_return_pct = ?,
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
            _BENCHMARK_SYMBOL,
            round(benchmark_return_pct, 2) if benchmark_return_pct is not None else None,
            run_id,
        ),
    )
    conn.commit()


def _mark_other_swing_live_runs_stale(conn: sqlite3.Connection, keep_run_id: int, bot_name: str) -> None:
    conn.execute(
        """
        UPDATE swing_simulation_runs
        SET status = 'stale'
        WHERE mode = 'live'
          AND status = 'running'
          AND bot_name = ?
          AND id <> ?
        """,
        (bot_name, int(keep_run_id)),
    )


def _swing_live_metrics(
    conn: sqlite3.Connection,
    run_id: int,
    date_from: str,
    initial_cash_ars: float,
    as_of: str,
    final_value: float,
) -> Dict[str, Any]:
    step_rows = conn.execute(
        """
        SELECT step_date, portfolio_value_ars
        FROM swing_simulation_steps
        WHERE run_id = ? AND portfolio_value_ars IS NOT NULL
        ORDER BY step_date, id
        """,
        (int(run_id),),
    ).fetchall()
    curve: EquityCurve = [(date_from, float(initial_cash_ars))]
    for row in step_rows:
        curve.append((str(row["step_date"]), float(row["portfolio_value_ars"])))
    if curve[-1][0] != as_of or abs(curve[-1][1] - final_value) > 0.01:
        curve.append((as_of, float(final_value)))
    trade_rows = conn.execute(
        """
        SELECT amount_ars, pnl_ars, hold_days
        FROM swing_simulation_trades
        WHERE run_id = ?
        """,
        (int(run_id),),
    ).fetchall()
    trade_pnls = [float(r["pnl_ars"]) for r in trade_rows if r["pnl_ars"] is not None]
    total_traded = sum(float(r["amount_ars"] or 0.0) for r in trade_rows)
    hold_days = [int(r["hold_days"]) for r in trade_rows if r["hold_days"] is not None]
    metrics = build_metrics_dict(curve, trade_pnls, total_traded)
    metrics["total_trades"] = len(trade_rows)
    metrics["avg_hold_days"] = round(sum(hold_days) / len(hold_days), 1) if hold_days else None
    return metrics


# ── Main runner ───────────────────────────────────────────────────────────────

def run_swing_backtest(
    conn: sqlite3.Connection,
    config: SwingBotConfig,
    date_from: str,
    date_to: str,
    initial_cash_ars: float,
    *,
    commission_rate: float = 0.0,
    commission_min: float = 0.0,
    verbose: bool = True,
    existing_run_id: Optional[int] = None,
    cost_model: Optional[ExecutionCostModel] = None,
) -> int:
    """Run a full swing trading backtest. Returns swing_simulation_runs.id.

    Signals are computed on day T using closing prices.
    Trades execute at day T+1's open price (look-ahead bias eliminated).
    Commission and slippage are applied via SimulatedPortfolio.
    """

    def log(msg: str) -> None:
        if verbose:
            from rich.console import Console
            Console().print(msg)

    enforce_cost_model_version = cost_model is not None
    cost_model = cost_model or ExecutionCostModel.from_config(
        commission_min=commission_min,
        legacy_commission_rate=commission_rate if commission_rate > 0 else None,
    )
    run_id = existing_run_id or _create_run_row(
        conn, config.name, date_from, date_to, initial_cash_ars,
        cost_model_version=cost_model.version,
    )
    log(
        f"[bold]Swing backtest run #{run_id}[/bold] bot=[cyan]{config.name}[/cyan] "
        f"{date_from} to {date_to} | commission={commission_rate*100:.2f}% slippage={config.slippage_pct*100:.2f}%"
    )

    trading_dates = load_trading_dates(conn, date_from, date_to)
    if not trading_dates:
        _finalize_run(conn, run_id, [], [], [], 0.0, 0)
        log("[red]No market data found in date range.[/red]")
        return run_id

    portfolio = SimulatedPortfolio(
        cash_ars=initial_cash_ars,
        commission_rate=commission_rate,
        commission_min=commission_min,
        slippage_pct=config.slippage_pct,
        cost_model=cost_model,
    )
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

    # Pending orders from previous day: list of dicts with keys:
    # {symbol, side, amount_ars, quantity, signal_price, reason, entry_date, entry_signals}
    pending_orders: List[dict] = []

    # Cached engine signals (refresh every 5 trading days)
    regime, macro = None, None
    _ENGINE_REFRESH = 5

    # Benchmark tracking
    bm_price_start = _get_benchmark_price(conn, _BENCHMARK_SYMBOL, date_from)

    for i, date in enumerate(trading_dates):
        if i % _ENGINE_REFRESH == 0:
            regime, macro = _load_engine_signals(conn, date)

        regime_score = float(regime.regime_score) if regime else 50.0
        macro_stress = float(macro.argentina_macro_stress) if macro else 50.0

        prices = load_prices_for_date(conn, date)
        metadata = load_execution_metadata_for_date(conn, date)
        if not prices:
            curve.append((date, portfolio.mark_to_market({})))
            continue

        # ── Step 0: Execute pending orders from previous day at today's open ──
        for order in pending_orders:
            sym = order["symbol"]
            open_px = _load_open_price(conn, sym, date)
            if not open_px:
                # No open price available — use last_price as fallback
                open_px = prices.get(sym)
            if not open_px:
                continue
            meta = metadata.get(sym, {})
            instrument_type = portfolio.cost_model.instrument_type_for(sym, meta.get("instrument_type"))
            volume_amount = meta.get("volume_amount") if isinstance(meta.get("volume_amount"), (int, float)) else None
            price_source = "symbol_daily_ohlcv.open" if _load_open_price(conn, sym, date) else "fallback_last_price"

            if order["side"] == "buy":
                fill = portfolio.buy(
                    sym,
                    order["amount_ars"],
                    open_px,
                    instrument_type=instrument_type,
                    price_source=price_source,
                    volume_amount=volume_amount,
                )
                if fill.quantity <= 0:
                    continue
                total_traded += fill.gross_amount_ars
                total_trades += 1
                open_positions[sym] = OpenPosition(
                    symbol=sym,
                    entry_price=open_px,
                    entry_date=date,
                    days_held=0,
                    peak_price=open_px,
                    engine_score=order.get("engine_score", 0.0),
                )
                entry_amounts[sym] = fill.net_cash_impact_ars
                entry_quantities[sym] = fill.quantity
                entry_signals_log[sym] = order.get("entry_signals", {})
                _persist_trade(
                    conn, run_id, sym,
                    entry_date=date, exit_date=None,
                    entry_price=fill.effective_price, exit_price=None,
                    quantity=fill.quantity, amount_ars=fill.net_cash_impact_ars,
                    pnl_ars=None, hold_days=None, exit_reason=None,
                    entry_signals=order.get("entry_signals", {}),
                    exit_signals=None,
                    fill=fill,
                )
                conn.commit()

            elif order["side"] == "sell" and sym in open_positions:
                pos = open_positions[sym]
                qty = entry_quantities.get(sym, 0.0)
                cost = entry_amounts.get(sym, 0.0)
                fill = portfolio.sell_quantity(
                    sym,
                    qty,
                    open_px,
                    instrument_type=instrument_type,
                    price_source=price_source,
                    volume_amount=volume_amount,
                )
                if fill.quantity <= 0:
                    continue
                pnl = float(fill.realized_pnl_ars or 0.0)
                hold_days = pos.days_held

                total_traded += fill.gross_amount_ars
                total_trades += 1
                trade_pnls.append(pnl)
                trade_hold_days.append(hold_days)

                _persist_trade(
                    conn, run_id, sym,
                    entry_date=pos.entry_date, exit_date=date,
                    entry_price=pos.entry_price, exit_price=fill.effective_price,
                    quantity=fill.quantity, amount_ars=cost,
                    pnl_ars=pnl, hold_days=hold_days,
                    exit_reason=order.get("reason", "signal_exit"),
                    entry_signals=entry_signals_log.get(sym, {}),
                    exit_signals={"reason": order.get("reason", "signal_exit")},
                    fill=fill,
                )
                conn.commit()
                del open_positions[sym]
                entry_amounts.pop(sym, None)
                entry_quantities.pop(sym, None)
                entry_signals_log.pop(sym, None)

        pending_orders = []

        opp_scores = _load_opportunity_scores(conn, date)

        # ── Step 1: Update hold days and peak prices for open positions ────────
        for symbol, pos in list(open_positions.items()):
            current_price = prices.get(symbol)
            if current_price and current_price > pos.peak_price:
                open_positions[symbol].peak_price = current_price
            open_positions[symbol].days_held += 1

        # ── Step 2: Evaluate exits for open positions → queue as pending ───────
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
                reason_str = signal.reason
                exit_reason = "signal_exit"
                for prefix in ("stop_loss", "take_profit", "trailing_stop", "time_stop",
                               "rsi_overbought"):
                    if reason_str.startswith(prefix):
                        exit_reason = prefix
                        break
                pending_orders.append({
                    "symbol": sym,
                    "side": "sell",
                    "quantity": entry_quantities.get(sym, 0.0),
                    "signal_price": current_price,
                    "reason": exit_reason,
                    "amount_ars": entry_amounts.get(sym, 0.0),
                })

        # ── Step 3: Evaluate entries for new positions → queue as pending ──────
        total_value = portfolio.mark_to_market(prices)
        already_pending_buys = {o["symbol"] for o in pending_orders if o["side"] == "buy"}
        available_slots = config.max_positions - len(open_positions) - len(already_pending_buys)
        min_cash = total_value * config.cash_reserve_pct

        if available_slots > 0 and portfolio.cash_ars > min_cash:
            candidates = sorted(
                [(sym, score) for sym, score in opp_scores.items()
                 if sym not in open_positions and sym not in already_pending_buys and sym in prices],
                key=lambda x: x[1],
                reverse=True,
            )

            for sym, engine_score in candidates[:20]:
                if len(open_positions) + len(already_pending_buys) >= config.max_positions:
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

                position_budget = total_value * config.position_size_pct
                max_cash_to_use = portfolio.cash_ars - min_cash
                amount = min(position_budget, max_cash_to_use)
                if amount < 500:
                    continue

                already_pending_buys.add(sym)
                pending_orders.append({
                    "symbol": sym,
                    "side": "buy",
                    "amount_ars": amount,
                    "signal_price": current_price,
                    "engine_score": engine_score,
                    "entry_signals": {
                        "reason": signal.reason,
                        "conviction": signal.conviction,
                        "engine_score": engine_score,
                        "regime_score": regime_score,
                        "macro_stress": macro_stress,
                    },
                })

        # ── Step 4: Mark-to-market ─────────────────────────────────────────────
        total_value = portfolio.mark_to_market(prices)
        curve.append((date, total_value))

        if i % 20 == 0:
            log(
                f"  {date}  value=[yellow]ARS {total_value:,.0f}[/yellow]  "
                f"cash={portfolio.cash_ars:,.0f}  positions={len(open_positions)}"
            )

    # Force-close all remaining positions on last day (at last day's price)
    if open_positions and trading_dates:
        last_date = trading_dates[-1]
        last_prices = load_prices_for_date(conn, last_date)
        last_metadata = load_execution_metadata_for_date(conn, last_date)
        for sym, pos in list(open_positions.items()):
            current_price = last_prices.get(sym, pos.entry_price)
            qty = entry_quantities.get(sym, 0.0)
            meta = last_metadata.get(sym, {})
            instrument_type = portfolio.cost_model.instrument_type_for(sym, meta.get("instrument_type"))
            volume_amount = meta.get("volume_amount") if isinstance(meta.get("volume_amount"), (int, float)) else None
            fill = portfolio.sell_quantity(
                sym,
                qty,
                current_price,
                instrument_type=instrument_type,
                volume_amount=volume_amount,
            )
            if fill.quantity <= 0:
                continue
            cost = entry_amounts.get(sym, 0.0)
            pnl = float(fill.realized_pnl_ars or 0.0)
            hold_days = pos.days_held

            total_traded += fill.gross_amount_ars
            total_trades += 1
            trade_pnls.append(pnl)
            trade_hold_days.append(hold_days)

            conn.execute(
                """
                UPDATE swing_simulation_trades SET
                    exit_date = ?, exit_price = ?, pnl_ars = ?,
                    return_pct = ?, hold_days = ?, exit_reason = 'time_stop',
                    gross_amount_ars = ?, net_amount_ars = ?, commission_ars = ?,
                    market_fee_ars = ?, iva_ars = ?, slippage_ars = ?,
                    total_cost_ars = ?, execution_price = ?, instrument_type = ?,
                    cost_model_json = ?
                WHERE run_id = ? AND symbol = ? AND exit_date IS NULL
                """,
                (
                    last_date,
                    fill.effective_price,
                    round(pnl, 2),
                    round(pnl / cost * 100, 2) if cost > 0 else 0,
                    hold_days,
                    round(fill.gross_amount_ars, 2),
                    round(fill.net_amount_ars, 2),
                    round(fill.commission_ars, 2),
                    round(fill.market_fee_ars, 2),
                    round(fill.iva_ars, 2),
                    round(fill.slippage_ars, 2),
                    round(fill.total_cost_ars, 2),
                    round(fill.effective_price, 6),
                    fill.instrument_type,
                    fill.to_json(),
                    run_id, sym,
                ),
            )
        conn.commit()

    # Benchmark return
    bm_price_end = _get_benchmark_price(conn, _BENCHMARK_SYMBOL, date_to)
    bm_return: Optional[float] = None
    if bm_price_start and bm_price_end:
        bm_return = (bm_price_end - bm_price_start) / bm_price_start * 100.0

    _finalize_run(conn, run_id, curve, trade_pnls, trade_hold_days, total_traded, total_trades, bm_return)

    final_val = curve[-1][1] if curve else initial_cash_ars
    ret = (final_val - initial_cash_ars) / initial_cash_ars * 100 if initial_cash_ars else 0
    bm_str = f"  benchmark({_BENCHMARK_SYMBOL})=[cyan]{bm_return:+.1f}%[/cyan]" if bm_return is not None else ""
    log(
        f"\n[bold green]Swing backtest complete.[/bold green] "
        f"Return: [{'green' if ret >= 0 else 'red'}]{ret:+.1f}%[/]{bm_str}  "
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
    commission_rate: float = 0.0,
    commission_min: float = 0.0,
    verbose: bool = True,
    cost_model: Optional[ExecutionCostModel] = None,
) -> List[int]:
    """Execute one daily paper-trading step for each swing bot.

    Finds or creates a monthly live run, reconstructs open positions from
    existing trades, executes any pending orders at today's open price,
    then queues new signals as pending orders for tomorrow.

    Returns list of run_ids processed.
    """

    def log(msg: str) -> None:
        if verbose:
            from rich.console import Console
            Console().print(msg)

    enforce_cost_model_version = cost_model is not None
    cost_model = cost_model or ExecutionCostModel.from_config(
        commission_min=commission_min,
        legacy_commission_rate=commission_rate if commission_rate > 0 else None,
    )
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
    metadata = load_execution_metadata_for_date(conn, as_of)

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
            version_row = conn.execute(
                "SELECT cost_model_version FROM swing_simulation_runs WHERE id = ?",
                (run_id,),
            ).fetchone()
            if enforce_cost_model_version and version_row and version_row[0] != cost_model.version:
                conn.execute("UPDATE swing_simulation_runs SET status='stale' WHERE id=?", (run_id,))
                run_id = _create_run_row(
                    conn, bot_name, f"{period}-01", as_of, initial_cash_ars,
                    mode="live", cost_model_version=cost_model.version,
                )
            else:
                conn.execute(
                    "UPDATE swing_simulation_runs SET date_from = MIN(date_from, ?) WHERE id = ?",
                    (f"{period}-01", run_id),
                )
        else:
            run_id = _create_run_row(
                conn, bot_name, f"{period}-01", as_of, initial_cash_ars, mode="live",
                cost_model_version=cost_model.version if enforce_cost_model_version else None,
            )
        _mark_other_swing_live_runs_stale(conn, int(run_id), bot_name)
        conn.commit()

        # Check if already executed today
        already = conn.execute(
            "SELECT 1 FROM swing_simulation_steps WHERE run_id=? AND step_date=? LIMIT 1",
            (run_id, as_of),
        ).fetchone()
        if already:
            log(f"  [dim]{bot_name}[/dim] -step already executed for {as_of}, skipping.")
            run_ids.append(run_id)
            continue

        # Reconstruct portfolio and open positions from trade history
        portfolio = SimulatedPortfolio(
            cash_ars=initial_cash_ars,
            commission_rate=commission_rate,
            commission_min=commission_min,
            slippage_pct=config.slippage_pct,
            cost_model=cost_model,
        )
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
                # Still open: rebuild exact stored net basis without charging costs again.
                qty_f = float(qty or 0.0)
                amount_f = float(amount or 0.0)
                if qty_f > 0:
                    portfolio.holdings[sym] = Position(sym, qty_f, amount_f / qty_f)
                    portfolio.cash_ars -= amount_f
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
                entry_amounts[sym] = amount_f
                entry_quantities[sym] = qty_f
            else:
                # Closed — replay the sell to keep cash correct
                portfolio.sell(sym, float(amount), float(entry_price))

        regime_score = float(regime.regime_score) if regime else 50.0
        macro_stress = float(macro.argentina_macro_stress) if macro else 50.0
        opp_scores = _load_opportunity_scores(conn, as_of)

        # ── Phase A: Execute pending orders from yesterday at today's open ─────
        pending_rows = conn.execute(
            """
            SELECT id, symbol, side, amount_ars, quantity, signal_price
            FROM swing_pending_orders
            WHERE run_id=? AND status='pending'
            ORDER BY id ASC
            """,
            (run_id,),
        ).fetchall()

        executed_pending: List[dict] = []
        for pid, sym, side, amt, qty, sig_px in pending_rows:
            open_px = _load_open_price(conn, sym, as_of)
            if not open_px:
                open_px = prices.get(sym)
            if not open_px:
                continue
            meta = metadata.get(sym, {})
            instrument_type = portfolio.cost_model.instrument_type_for(sym, meta.get("instrument_type"))
            volume_amount = meta.get("volume_amount") if isinstance(meta.get("volume_amount"), (int, float)) else None
            price_source = "symbol_daily_ohlcv.open" if _load_open_price(conn, sym, as_of) else "fallback_last_price"

            if side == "buy" and sym not in open_positions:
                amount = float(amt) if amt else 0.0
                fill = portfolio.buy(
                    sym,
                    amount,
                    open_px,
                    instrument_type=instrument_type,
                    price_source=price_source,
                    volume_amount=volume_amount,
                )
                if fill.quantity <= 0:
                    continue
                days_held = 0
                open_positions[sym] = OpenPosition(
                    symbol=sym,
                    entry_price=fill.effective_price,
                    entry_date=as_of,
                    days_held=days_held,
                    peak_price=fill.effective_price,
                    engine_score=0.0,
                )
                entry_amounts[sym] = fill.net_cash_impact_ars
                entry_quantities[sym] = fill.quantity
                _persist_trade(
                    conn, run_id, sym,
                    entry_date=as_of, exit_date=None,
                    entry_price=fill.effective_price, exit_price=None,
                    quantity=fill.quantity, amount_ars=fill.net_cash_impact_ars,
                    pnl_ars=None, hold_days=None, exit_reason=None,
                    entry_signals={"pending_id": pid, "signal_price": sig_px},
                    exit_signals=None,
                    fill=fill,
                )
                executed_pending.append({"symbol": sym, "side": "buy", "price": fill.effective_price, "amount_ars": round(fill.net_cash_impact_ars, 2)})

            elif side == "sell" and sym in open_positions:
                pos = open_positions[sym]
                sell_qty = float(qty) if qty else entry_quantities.get(sym, 0.0)
                cost = entry_amounts.get(sym, 0.0)
                fill = portfolio.sell_quantity(
                    sym,
                    sell_qty,
                    open_px,
                    instrument_type=instrument_type,
                    price_source=price_source,
                    volume_amount=volume_amount,
                )
                if fill.quantity <= 0:
                    continue
                pnl = float(fill.realized_pnl_ars or 0.0)
                hold_days = pos.days_held

                conn.execute(
                    """
                    UPDATE swing_simulation_trades SET
                        exit_date=?, exit_price=?, pnl_ars=?, return_pct=?,
                        hold_days=?, exit_reason=?,
                        exit_signals_json=?,
                        gross_amount_ars=?, net_amount_ars=?, commission_ars=?,
                        market_fee_ars=?, iva_ars=?, slippage_ars=?,
                        total_cost_ars=?, execution_price=?, instrument_type=?,
                        cost_model_json=?
                    WHERE run_id=? AND symbol=? AND exit_date IS NULL
                    """,
                    (
                        as_of, fill.effective_price, round(pnl, 2),
                        round(pnl / cost * 100, 2) if cost > 0 else 0,
                        hold_days, "signal_exit",
                        json.dumps({"pending_id": pid, "signal_price": sig_px}),
                        round(fill.gross_amount_ars, 2),
                        round(fill.net_amount_ars, 2),
                        round(fill.commission_ars, 2),
                        round(fill.market_fee_ars, 2),
                        round(fill.iva_ars, 2),
                        round(fill.slippage_ars, 2),
                        round(fill.total_cost_ars, 2),
                        round(fill.effective_price, 6),
                        fill.instrument_type,
                        fill.to_json(),
                        run_id, sym,
                    ),
                )
                del open_positions[sym]
                entry_amounts.pop(sym, None)
                entry_quantities.pop(sym, None)
                executed_pending.append({"symbol": sym, "side": "sell", "price": fill.effective_price, "pnl_ars": round(pnl, 2)})

            conn.execute(
                "UPDATE swing_pending_orders SET status='executed', execute_date=?, execute_price=? WHERE id=?",
                (as_of, open_px, pid),
            )

        conn.commit()

        # ── Phase B: Evaluate exits for open positions → queue as pending ──────
        step_pending: List[dict] = []

        for sym, pos in list(open_positions.items()):
            current_price = prices.get(sym)
            if not current_price:
                continue
            # Update peak
            if current_price > pos.peak_price:
                open_positions[sym].peak_price = current_price
            open_positions[sym].days_held += 1

            price_history = _load_price_history(conn, sym, as_of)
            ta = compute_swing_ta(sym, price_history)
            engine_score = opp_scores.get(sym, 0.0)
            signal = classify_swing_signal(ta, engine_score, regime_score, macro_stress, pos, config)

            if signal.action == "exit":
                reason_str = signal.reason
                exit_reason = "signal_exit"
                for prefix in ("stop_loss", "take_profit", "trailing_stop", "time_stop", "rsi_overbought"):
                    if reason_str.startswith(prefix):
                        exit_reason = prefix
                        break
                step_pending.append({
                    "symbol": sym,
                    "side": "sell",
                    "quantity": entry_quantities.get(sym, 0.0),
                    "signal_price": current_price,
                    "reason": exit_reason,
                })

        # ── Phase C: Evaluate entries → queue as pending ───────────────────────
        total_value = portfolio.mark_to_market(prices)
        pending_buy_symbols = {p["symbol"] for p in step_pending if p["side"] == "buy"}
        # Also exclude symbols queued for sell from new entries
        pending_sell_symbols = {p["symbol"] for p in step_pending if p["side"] == "sell"}
        min_cash = total_value * config.cash_reserve_pct

        available_slots = config.max_positions - len(open_positions) + len(pending_sell_symbols)
        if available_slots > 0 and portfolio.cash_ars > min_cash:
            candidates = sorted(
                [(sym, score) for sym, score in opp_scores.items()
                 if sym not in open_positions
                 and sym not in pending_buy_symbols
                 and sym not in pending_sell_symbols
                 and sym in prices],
                key=lambda x: x[1], reverse=True,
            )
            for sym, engine_score in candidates[:20]:
                if len(open_positions) - len(pending_sell_symbols) + len(pending_buy_symbols) >= config.max_positions:
                    break
                current_price = prices.get(sym)
                if not current_price:
                    continue
                price_history = _load_price_history(conn, sym, as_of)
                ta = compute_swing_ta(sym, price_history)

                if ta.rsi_14 is None and ta.macd_histogram is None:
                    continue  # Insufficient TA history — skip

                signal = classify_swing_signal(ta, engine_score, regime_score, macro_stress, None, config)
                if signal.action != "entry":
                    continue
                position_budget = total_value * config.position_size_pct
                amount = min(position_budget, portfolio.cash_ars - min_cash)
                if amount < 500:
                    continue
                pending_buy_symbols.add(sym)
                step_pending.append({
                    "symbol": sym,
                    "side": "buy",
                    "amount_ars": round(amount, 2),
                    "signal_price": current_price,
                    "reason": signal.reason,
                    "score": round(engine_score, 1),
                })

        # Persist pending orders to DB
        now_utc = datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
        for order in step_pending:
            conn.execute(
                """
                INSERT INTO swing_pending_orders
                    (run_id, signal_date, symbol, side, amount_ars, quantity,
                     signal_price, status, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, 'pending', ?)
                """,
                (
                    run_id, as_of, order["symbol"], order["side"],
                    order.get("amount_ars"), order.get("quantity"),
                    order["signal_price"], now_utc,
                ),
            )

        final_val = portfolio.mark_to_market(prices)
        open_marks = []
        for sym, pos in sorted(open_positions.items()):
            current_price = prices.get(sym)
            amount = entry_amounts.get(sym, 0.0)
            qty = entry_quantities.get(sym, 0.0)
            if current_price is None or amount <= 0:
                open_marks.append({
                    "symbol": sym,
                    "entry_date": pos.entry_date,
                    "entry_price": pos.entry_price,
                    "current_price": current_price,
                    "pnl_ars": None,
                    "return_pct": None,
                })
                continue
            market_value = float(qty) * float(current_price)
            pnl = market_value - float(amount)
            open_marks.append({
                "symbol": sym,
                "entry_date": pos.entry_date,
                "entry_price": round(float(pos.entry_price), 4),
                "current_price": round(float(current_price), 4),
                "pnl_ars": round(pnl, 2),
                "return_pct": round(pnl / float(amount) * 100.0, 2),
            })

        # Benchmark return (incremental — use price from run start)
        bm_start = _get_benchmark_price(conn, _BENCHMARK_SYMBOL, f"{period}-01")
        bm_end = _get_benchmark_price(conn, _BENCHMARK_SYMBOL, as_of)
        bm_return: Optional[float] = None
        if bm_start and bm_end:
            bm_return = (bm_end - bm_start) / bm_start * 100.0

        plan = {
            "as_of": as_of,
            "regime": regime.regime if regime else "unknown",
            "regime_score": round(regime_score, 1),
            "macro_stress": round(macro_stress, 1),
            "executed_today": executed_pending,
            "pending_tomorrow": [
                {"symbol": p["symbol"], "side": p["side"],
                 "signal_price": p["signal_price"], "score": p.get("score")}
                for p in step_pending
            ],
            "open_positions": list(open_positions.keys()),
            "open_positions_mark_to_market": open_marks,
            "portfolio_value_ars": round(final_val, 2),
            "benchmark_symbol": _BENCHMARK_SYMBOL,
            "benchmark_return_pct": round(bm_return, 2) if bm_return is not None else None,
        }
        run_row = conn.execute(
            "SELECT date_from, initial_cash FROM swing_simulation_runs WHERE id = ?",
            (run_id,),
        ).fetchone()
        run_date_from = str(run_row["date_from"] if run_row else f"{period}-01")
        run_initial = float(run_row["initial_cash"] if run_row and run_row["initial_cash"] else initial_cash_ars)
        metrics = _swing_live_metrics(conn, run_id, run_date_from, run_initial, as_of, final_val)
        conn.execute(
            """
            UPDATE swing_simulation_runs SET
                final_value=?,
                total_return_pct=?,
                sharpe_ratio=?,
                max_drawdown_pct=?,
                win_rate_pct=?,
                avg_hold_days=?,
                total_trades=?,
                status='running',
                benchmark_symbol=?,
                benchmark_return_pct=?,
                plan_json=?,
                cost_model_version=?
            WHERE id=?
            """,
            (
                round(final_val, 2),
                metrics["total_return_pct"],
                metrics["sharpe_ratio"],
                metrics["max_drawdown_pct"],
                metrics["win_rate_pct"],
                metrics["avg_hold_days"],
                metrics["total_trades"],
                _BENCHMARK_SYMBOL,
                round(bm_return, 2) if bm_return is not None else None,
                json.dumps(plan),
                cost_model.version,
                run_id,
            ),
        )
        conn.execute(
            """
            INSERT INTO swing_simulation_steps
                (run_id, step_date, regime, regime_score, macro_stress,
                 entries_count, exits_count, open_positions_count,
                 portfolio_value_ars, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_id, as_of,
                regime.regime if regime else None,
                round(regime_score, 2),
                round(macro_stress, 2),
                len([p for p in step_pending if p["side"] == "buy"]),
                len([p for p in step_pending if p["side"] == "sell"]),
                len(open_positions),
                round(final_val, 2),
                now_utc,
            ),
        )
        conn.commit()

        bm_str = f"  bm={bm_return:+.1f}%" if bm_return is not None else ""
        log(
            f"  [cyan]{bot_name}[/cyan] #{run_id}  "
            f"value=[yellow]ARS {final_val:,.0f}[/yellow]  "
            f"positions={len(open_positions)}  "
            f"pending_tomorrow={len(step_pending)}{bm_str}"
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
