"""BacktestRunner — paper-trading simulation using historical DB data.

Uses market_symbol_snapshots for prices and advisor_opportunity_candidates
for trade signals. Zero IOL API calls — purely DB-driven.

Flow per trading day:
  1. Load prices for the date
  2. Load cached engine signals (regime, macro, smart_money) for that date
  3. Load the latest opportunity candidates available on that date
  4. Rescore candidates using engine-adjusted weights blended by regime_influence
  5. Filter by adjusted min_score_threshold
  6. Execute paper trades (buy/sell) on SimulatedPortfolio
  7. Mark-to-market and record equity point
  8. Persist trades to simulation_trades

At the end, compute metrics and update simulation_runs row.
"""
from __future__ import annotations

import json
import sqlite3
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from .bot_config import BotConfig, get_preset
from .metrics import EquityCurve, build_metrics_dict
from .portfolio_sim import (
    SimulatedPortfolio,
    load_prices_for_date,
    load_trading_dates,
)


# Only re-run engine signal lookup every N trading days (they are date-keyed
# and rarely change day-to-day, so this avoids repeated DB queries)
_ENGINE_REFRESH_EVERY_N_DAYS = 5


# ── DB helpers ────────────────────────────────────────────────────────────────

def _load_opportunity_candidates(
    conn: sqlite3.Connection, as_of: str, top_n: int = 30
) -> List[Dict[str, Any]]:
    """Load the best opportunity candidates from the most recent run on/before as_of."""
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
        SELECT symbol, signal_side, signal_family, score_total,
               score_risk, score_value, score_momentum, score_catalyst,
               suggested_weight_pct, suggested_amount_ars, reason_summary, sector_bucket
        FROM advisor_opportunity_candidates
        WHERE run_id = ?
        ORDER BY score_total DESC LIMIT ?
        """,
        (run_id, top_n),
    )
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


def _load_engine_signals(
    conn: sqlite3.Connection, as_of: str
) -> Tuple[Any, Any, List[Any]]:
    """Load the nearest cached engine signals on or before as_of.

    Returns (regime_signal, macro_signal, smart_money_signals[]).
    Any may be None if not cached yet.
    """
    from iol_engines.macro.engine import MacroMomentumEngine
    from iol_engines.regime.engine import MarketRegimeEngine
    from iol_engines.smart_money.engine import SmartMoneyEngine

    regime = MarketRegimeEngine().load_latest(conn, as_of)
    macro = MacroMomentumEngine().load_latest(conn, as_of)
    smart_money = SmartMoneyEngine().load_latest(conn, as_of) or []
    return regime, macro, smart_money


def _rescore_with_engines(
    candidates: List[Dict[str, Any]],
    regime: Any,
    macro: Any,
    smart_money: List[Any],
    bot_config: BotConfig,
) -> List[Dict[str, Any]]:
    """Re-score candidates using engine-adjusted weights.

    Blends the new engine-aware score with the original score using
    bot_config.regime_influence as the blend factor:
      final = regime_influence * engine_score + (1 - regime_influence) * original_score
    """
    from iol_engines.opportunity.adapter import build_adjusted_params

    params = build_adjusted_params(regime, macro, smart_money)
    adj_weights = params["weights"]
    catalyst_overrides = params["catalyst_overrides"]

    rescored = []
    for c in candidates:
        s_risk = float(c.get("score_risk") or 0)
        s_value = float(c.get("score_value") or 0)
        s_momentum = float(c.get("score_momentum") or 0)
        s_catalyst = float(c.get("score_catalyst") or 0)

        engine_score = (
            adj_weights.get("risk", 0.35) * s_risk
            + adj_weights.get("value", 0.20) * s_value
            + adj_weights.get("momentum", 0.35) * s_momentum
            + adj_weights.get("catalyst", 0.10) * s_catalyst
        )

        # Catalyst delta from smart money
        symbol = c.get("symbol", "")
        engine_score += catalyst_overrides.get(symbol, 0.0)
        engine_score = max(0.0, min(100.0, engine_score))

        # Blend with original using regime_influence
        original = float(c.get("score_total") or 0)
        ri = bot_config.regime_influence
        final_score = ri * engine_score + (1.0 - ri) * original

        rescored.append({**c, "score_total": final_score})

    return sorted(rescored, key=lambda x: float(x.get("score_total") or 0), reverse=True)


def _create_run_row(
    conn: sqlite3.Connection,
    config: BotConfig,
    date_from: str,
    date_to: str,
    initial_cash: float,
    mode: str = "backtest",
) -> int:
    """Insert a simulation_runs row with status='running' and return its id."""
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO simulation_bot_configs (name, created_at_utc, description, config_json)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(name) DO UPDATE SET
            description=excluded.description,
            config_json=excluded.config_json
        """,
        (
            config.name,
            datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
            config.description,
            config.to_json(),
        ),
    )
    cur.execute("SELECT id FROM simulation_bot_configs WHERE name = ?", (config.name,))
    bot_id = cur.fetchone()[0]

    cur.execute(
        """
        INSERT INTO simulation_runs
            (created_at_utc, bot_config_id, date_from, date_to, status,
             initial_value_ars, mode)
        VALUES (?, ?, ?, ?, 'running', ?, ?)
        """,
        (
            datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
            bot_id,
            date_from,
            date_to,
            initial_cash,
            mode,
        ),
    )
    conn.commit()
    return cur.lastrowid or 0


def _persist_trade(
    conn: sqlite3.Connection,
    run_id: int,
    date: str,
    symbol: str,
    action: str,
    quantity: float,
    price: float,
    amount_ars: float,
    portfolio_value: float,
    reason: str,
    engine_source: str = "simulation",
) -> None:
    conn.execute(
        """
        INSERT INTO simulation_trades
            (run_id, trade_date, symbol, action, quantity, price,
             amount_ars, portfolio_value_after, reason, engine_source)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (run_id, date, symbol, action, quantity, price, amount_ars, portfolio_value,
         reason, engine_source),
    )


def _finalize_run(
    conn: sqlite3.Connection,
    run_id: int,
    portfolio: SimulatedPortfolio,
    curve: EquityCurve,
    trade_pnls: List[float],
    total_traded: float,
    *,
    error: Optional[str] = None,
    engine_driven: bool = False,
    avg_regime_score: Optional[float] = None,
    regime_context: Optional[Dict[str, Any]] = None,
) -> None:
    metrics = build_metrics_dict(curve, trade_pnls, total_traded)
    final_value = curve[-1][1] if curve else 0.0
    conn.execute(
        """
        UPDATE simulation_runs SET
            status = ?,
            final_value_ars = ?,
            total_return_pct = ?,
            sharpe_ratio = ?,
            max_drawdown_pct = ?,
            metrics_json = ?,
            error_message = ?,
            engine_driven = ?,
            avg_regime_score = ?,
            regime_context_json = ?
        WHERE id = ?
        """,
        (
            "error" if error else "done",
            round(final_value, 2),
            metrics["total_return_pct"],
            metrics["sharpe_ratio"],
            metrics["max_drawdown_pct"],
            json.dumps(metrics),
            error,
            1 if engine_driven else 0,
            avg_regime_score,
            json.dumps(regime_context) if regime_context else None,
            run_id,
        ),
    )
    conn.commit()


# ── Main trading-day execution (shared by backtest + live-step) ──────────────

def _execute_trading_day(
    conn: sqlite3.Connection,
    run_id: int,
    date: str,
    config: BotConfig,
    portfolio: SimulatedPortfolio,
    curve: EquityCurve,
    trade_pnls: List[float],
    cost_basis: Dict[str, float],
    total_traded_ref: List[float],  # mutable [total] for pass-by-reference
    *,
    regime: Any = None,
    macro: Any = None,
    smart_money: List[Any] = None,
    use_engines: bool = True,
    engine_source: str = "simulation",
) -> float:
    """Execute one trading day. Returns portfolio value after mark-to-market."""
    prices = load_prices_for_date(conn, date)
    total_value = portfolio.mark_to_market(prices)
    curve.append((date, total_value))

    candidates = _load_opportunity_candidates(conn, date)
    if not candidates:
        return total_value

    # Apply engine rescoring
    if use_engines and (regime is not None or macro is not None or smart_money):
        candidates = _rescore_with_engines(candidates, regime, macro, smart_money or [], config)

    # Adjust threshold: tighten in stressed macro environment
    threshold = config.min_score_threshold
    if use_engines and macro is not None and macro.argentina_macro_stress > 70:
        threshold = min(threshold * 1.10, 95.0)

    candidates = [
        c for c in candidates
        if float(c.get("score_total") or 0) >= threshold
    ]

    spendable = total_value * (1 - config.cash_reserve_pct)
    equity_budget = min(spendable - (total_value - portfolio.cash_ars),
                        portfolio.cash_ars * (1 - config.cash_reserve_pct))
    deployed = 0.0

    for c in candidates:
        symbol = c["symbol"]
        side = (c.get("signal_side") or "buy").lower()
        score = float(c.get("score_total") or 0)
        reason = c.get("reason_summary") or ""
        price = prices.get(symbol)

        if price is None or price <= 0:
            continue

        if side == "sell":
            action_type = (c.get("signal_family") or "trim").lower()
            pos_value = portfolio.position_value(symbol, price)
            if pos_value <= 0:
                continue
            amount = pos_value if action_type == "exit" else pos_value * 0.33

            cb = cost_basis.get(symbol, price)
            trade_pnls.append((price - cb) * (amount / price))

            qty = portfolio.sell(symbol, amount, price)
            pv = portfolio.mark_to_market(prices)
            _persist_trade(conn, run_id, date, symbol, action_type, qty, price, amount, pv,
                           reason, engine_source)
            total_traded_ref[0] += amount
            conn.commit()

        else:
            max_by_weight = total_value * config.max_position_pct
            existing = portfolio.position_value(symbol, price)
            room = max_by_weight - existing
            if room <= 0:
                continue
            remaining_budget = equity_budget - deployed
            if remaining_budget <= 0:
                break

            suggested = float(c.get("suggested_amount_ars") or 0)
            if not suggested:
                # Fallback: use suggested_weight_pct or bot's max_position_pct
                weight = float(c.get("suggested_weight_pct") or config.max_position_pct * 100)
                suggested = total_value * (weight / 100)
            amount = min(suggested, room, remaining_budget)
            if amount < 100:
                continue

            qty = portfolio.buy(symbol, amount, price)
            cost_basis[symbol] = price
            deployed += amount
            pv = portfolio.mark_to_market(prices)
            _persist_trade(conn, run_id, date, symbol, "buy", qty, price, amount, pv,
                           reason, engine_source)
            total_traded_ref[0] += amount
            conn.commit()

            if portfolio.n_positions >= config.max_positions:
                break

    return portfolio.mark_to_market(prices)


# ── Backtest ─────────────────────────────────────────────────────────────────

def run_backtest(
    conn: sqlite3.Connection,
    config: BotConfig,
    date_from: str,
    date_to: str,
    initial_cash_ars: float,
    *,
    verbose: bool = True,
    existing_run_id: Optional[int] = None,
    use_engines: bool = True,
) -> int:
    """Run a full backtest. Returns the simulation_runs.id.

    Pass existing_run_id to reuse a pre-created row (used by the web API).
    Set use_engines=False to use raw pre-computed scores without engine rescoring.
    """

    def log(msg: str) -> None:
        if verbose:
            from rich.console import Console
            Console().print(msg)

    run_id = existing_run_id or _create_run_row(
        conn, config, date_from, date_to, initial_cash_ars, mode="backtest"
    )
    log(
        f"[bold]Backtest run #{run_id}[/bold] bot=[cyan]{config.name}[/cyan] "
        f"{date_from} → {date_to}  engines={'on' if use_engines else 'off'}"
    )

    trading_dates = load_trading_dates(conn, date_from, date_to)
    if not trading_dates:
        _finalize_run(conn, run_id, SimulatedPortfolio(initial_cash_ars), [], [], 0.0,
                      error="No market data found in date range")
        log("[red]No market data found.[/red]")
        return run_id

    portfolio = SimulatedPortfolio(cash_ars=initial_cash_ars)
    curve: EquityCurve = []
    trade_pnls: List[float] = []
    total_traded_ref = [0.0]
    cost_basis: Dict[str, float] = {}

    # For regime context summary
    regime_scores: List[float] = []
    regime_counts: Dict[str, int] = defaultdict(int)

    # Cached engine signals (refreshed every N trading days)
    regime, macro, smart_money = None, None, []

    try:
        for i, date in enumerate(trading_dates):
            # Refresh engine signals every N days
            if use_engines and i % _ENGINE_REFRESH_EVERY_N_DAYS == 0:
                regime, macro, smart_money = _load_engine_signals(conn, date)
                if regime:
                    regime_scores.append(regime.regime_score)
                    regime_counts[regime.regime] += 1

            _execute_trading_day(
                conn, run_id, date, config, portfolio, curve,
                trade_pnls, cost_basis, total_traded_ref,
                regime=regime, macro=macro, smart_money=smart_money,
                use_engines=use_engines,
            )

            if i % 20 == 0:
                total_val = curve[-1][1] if curve else initial_cash_ars
                log(
                    f"  {date}  value=[yellow]ARS {total_val:,.0f}[/yellow]  "
                    f"cash={portfolio.cash_ars:,.0f}  positions={portfolio.n_positions}"
                )

    except Exception as exc:
        _finalize_run(conn, run_id, portfolio, curve, trade_pnls, total_traded_ref[0],
                      error=str(exc), engine_driven=use_engines)
        log(f"[red]Backtest error:[/red] {exc}")
        return run_id

    avg_rs = sum(regime_scores) / len(regime_scores) if regime_scores else None
    regime_ctx = dict(regime_counts) if regime_counts else None

    _finalize_run(
        conn, run_id, portfolio, curve, trade_pnls, total_traded_ref[0],
        engine_driven=use_engines,
        avg_regime_score=avg_rs,
        regime_context=regime_ctx,
    )
    final_val = curve[-1][1] if curve else initial_cash_ars
    ret = (final_val - initial_cash_ars) / initial_cash_ars * 100 if initial_cash_ars else 0
    log(
        f"\n[bold green]✓ Backtest complete.[/bold green] "
        f"Return: [{'green' if ret >= 0 else 'red'}]{ret:+.1f}%[/]  "
        f"Final: ARS {final_val:,.0f}  ({len(trading_dates)} trading days)"
    )
    return run_id


# ── Live paper trading ────────────────────────────────────────────────────────

def _find_or_create_live_run(
    conn: sqlite3.Connection,
    config: BotConfig,
    as_of: str,
    initial_cash_ars: float,
) -> int:
    """Find an active live run for this bot in the current month, or create one."""
    period = as_of[:7]  # YYYY-MM
    cur = conn.cursor()
    cur.execute(
        """
        SELECT r.id FROM simulation_runs r
        JOIN simulation_bot_configs c ON r.bot_config_id = c.id
        WHERE c.name = ? AND r.mode = 'live' AND r.date_from LIKE ?
          AND r.status IN ('running', 'done')
        ORDER BY r.id DESC LIMIT 1
        """,
        (config.name, f"{period}%"),
    )
    row = cur.fetchone()
    if row:
        # Re-open as running if it was marked done
        conn.execute("UPDATE simulation_runs SET status='running' WHERE id=?", (row[0],))
        conn.commit()
        return row[0]

    return _create_run_row(conn, config, f"{period}-01", as_of, initial_cash_ars, mode="live")


def _reconstruct_portfolio(
    conn: sqlite3.Connection,
    run_id: int,
    initial_cash_ars: float,
) -> Tuple[SimulatedPortfolio, Dict[str, float]]:
    """Replay existing trades for run_id to rebuild in-memory portfolio state."""
    trades = conn.execute(
        """
        SELECT symbol, action, quantity, price, amount_ars
        FROM simulation_trades
        WHERE run_id = ?
        ORDER BY id ASC
        """,
        (run_id,),
    ).fetchall()

    portfolio = SimulatedPortfolio(cash_ars=initial_cash_ars)
    cost_basis: Dict[str, float] = {}

    for symbol, action, quantity, price, amount_ars in trades:
        if price and price > 0:
            if action == "buy":
                portfolio.buy(symbol, amount_ars, price)
                cost_basis[symbol] = price
            elif action in ("trim", "exit"):
                portfolio.sell(symbol, amount_ars, price)

    return portfolio, cost_basis


def run_live_step(
    conn: sqlite3.Connection,
    bot_names: List[str],
    as_of: str,
    initial_cash_ars: float = 1_000_000.0,
    *,
    verbose: bool = True,
) -> List[int]:
    """Execute one paper-trading step for each bot. Called daily by the scheduler.

    Finds or creates a live simulation_run for the current month, reconstructs
    the portfolio from existing trades, then executes today's decisions using
    current engine signals.

    Returns list of run_ids processed.
    """

    def log(msg: str) -> None:
        if verbose:
            from rich.console import Console
            Console().print(msg)

    run_ids = []

    # Load today's engine signals once (shared across all bots)
    regime, macro, smart_money = _load_engine_signals(conn, as_of)
    regime_label = regime.regime if regime else "unknown"
    log(
        f"[bold]Live step[/bold] {as_of}  regime=[cyan]{regime_label}[/cyan]  "
        f"bots={', '.join(bot_names)}"
    )

    prices = load_prices_for_date(conn, as_of)
    if not prices:
        log(f"[yellow]No price data for {as_of} — skipping live step.[/yellow]")
        return []

    candidates = _load_opportunity_candidates(conn, as_of)
    if not candidates:
        log(f"[yellow]No opportunity candidates for {as_of} — skipping live step.[/yellow]")
        return []

    for bot_name in bot_names:
        try:
            config = get_preset(bot_name)
        except ValueError:
            log(f"[red]Unknown bot preset: {bot_name}[/red]")
            continue

        run_id = _find_or_create_live_run(conn, config, as_of, initial_cash_ars)
        portfolio, cost_basis = _reconstruct_portfolio(conn, run_id, initial_cash_ars)

        # Check if we already executed a step today for this run
        already_today = conn.execute(
            "SELECT 1 FROM simulation_trades WHERE run_id=? AND trade_date=? LIMIT 1",
            (run_id, as_of),
        ).fetchone()
        if already_today:
            log(f"  [dim]{bot_name}[/dim] — step already executed for {as_of}, skipping.")
            run_ids.append(run_id)
            continue

        curve_so_far: EquityCurve = []
        trade_pnls: List[float] = []
        total_traded_ref = [0.0]

        _execute_trading_day(
            conn, run_id, as_of, config, portfolio, curve_so_far,
            trade_pnls, cost_basis, total_traded_ref,
            regime=regime, macro=macro, smart_money=smart_money,
            use_engines=True,
            engine_source="live_engine",
        )

        # Update the run's date_to and metrics
        final_val = portfolio.mark_to_market(prices)
        regime_score = regime.regime_score if regime else None
        conn.execute(
            """
            UPDATE simulation_runs SET
                date_to = ?,
                status = 'running',
                final_value_ars = ?,
                total_return_pct = ?,
                engine_driven = 1,
                avg_regime_score = ?
            WHERE id = ?
            """,
            (
                as_of,
                round(final_val, 2),
                (final_val - initial_cash_ars) / initial_cash_ars * 100,
                regime_score,
                run_id,
            ),
        )
        conn.commit()
        log(
            f"  [cyan]{bot_name}[/cyan] #{run_id}  "
            f"value=[yellow]ARS {final_val:,.0f}[/yellow]  "
            f"positions={portfolio.n_positions}"
        )
        run_ids.append(run_id)

    return run_ids
