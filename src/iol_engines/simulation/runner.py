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
    load_execution_metadata_for_date,
    load_prices_for_date,
    load_trading_dates,
)
from .cost_model import COST_MODEL_VERSION, ExecutionCostModel, ExecutionFill


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
          AND candidate_status NOT IN ('suppressed', 'rejected')
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
    cost_model_version: Optional[str] = None,
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
             initial_value_ars, mode, cost_model_version)
        VALUES (?, ?, ?, ?, 'running', ?, ?, ?)
        """,
        (
            datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
            bot_id,
            date_from,
            date_to,
            initial_cash,
            mode,
            cost_model_version,
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
        INSERT INTO simulation_trades
            (run_id, trade_date, symbol, action, quantity, price,
             amount_ars, portfolio_value_after, reason, engine_source,
             gross_amount_ars, net_amount_ars, commission_ars, market_fee_ars,
             iva_ars, slippage_ars, total_cost_ars, execution_price,
             instrument_type, cost_model_json)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (run_id, date, symbol, action, quantity, price, amount_ars, portfolio_value,
         reason, engine_source, *fill_values),
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
            win_rate_pct = ?,
            total_trades = ?,
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
            metrics["win_rate_pct"],
            len(trade_pnls),
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
    metadata = load_execution_metadata_for_date(conn, date)
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
        meta = metadata.get(symbol, {})
        instrument_type = portfolio.cost_model.instrument_type_for(symbol, meta.get("instrument_type"))
        volume_amount = meta.get("volume_amount")

        if price is None or price <= 0:
            continue

        if side == "sell":
            action_type = (c.get("signal_family") or "trim").lower()
            pos_value = portfolio.position_value(symbol, price)
            if pos_value <= 0:
                continue
            amount = pos_value if action_type == "exit" else pos_value * 0.33

            fill = portfolio.sell(
                symbol,
                amount,
                price,
                instrument_type=instrument_type,
                volume_amount=volume_amount if isinstance(volume_amount, (int, float)) else None,
            )
            if fill.quantity <= 0:
                continue
            trade_pnls.append(float(fill.realized_pnl_ars or 0.0))
            pv = portfolio.mark_to_market(prices)
            _persist_trade(conn, run_id, date, symbol, action_type, fill.quantity, price,
                           fill.gross_amount_ars, pv, reason, engine_source, fill=fill)
            total_traded_ref[0] += fill.gross_amount_ars
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

            fill = portfolio.buy(
                symbol,
                amount,
                price,
                instrument_type=instrument_type,
                volume_amount=volume_amount if isinstance(volume_amount, (int, float)) else None,
            )
            if fill.quantity <= 0:
                continue
            cost_basis[symbol] = portfolio.holdings[symbol].avg_price
            deployed += fill.net_cash_impact_ars
            pv = portfolio.mark_to_market(prices)
            _persist_trade(conn, run_id, date, symbol, "buy", fill.quantity, price,
                           fill.gross_amount_ars, pv, reason, engine_source, fill=fill)
            total_traded_ref[0] += fill.gross_amount_ars
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
    cost_model: Optional[ExecutionCostModel] = None,
) -> int:
    """Run a full backtest. Returns the simulation_runs.id.

    Pass existing_run_id to reuse a pre-created row (used by the web API).
    Set use_engines=False to use raw pre-computed scores without engine rescoring.
    """

    def log(msg: str) -> None:
        if verbose:
            from rich.console import Console
            Console().print(msg)

    cost_model = cost_model or ExecutionCostModel()
    run_id = existing_run_id or _create_run_row(
        conn, config, date_from, date_to, initial_cash_ars, mode="backtest",
        cost_model_version=cost_model.version,
    )
    log(
        f"[bold]Backtest run #{run_id}[/bold] bot=[cyan]{config.name}[/cyan] "
        f"{date_from} → {date_to}  engines={'on' if use_engines else 'off'}"
    )

    trading_dates = load_trading_dates(conn, date_from, date_to)
    if not trading_dates:
        _finalize_run(conn, run_id, SimulatedPortfolio(initial_cash_ars, cost_model=cost_model), [], [], 0.0,
                      error="No market data found in date range")
        log("[red]No market data found.[/red]")
        return run_id

    portfolio = SimulatedPortfolio(cash_ars=initial_cash_ars, cost_model=cost_model)
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
    cost_model_version: Optional[str] = None,
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
        existing_version = conn.execute(
            "SELECT cost_model_version FROM simulation_runs WHERE id = ?",
            (row[0],),
        ).fetchone()
        if cost_model_version and existing_version and existing_version[0] != cost_model_version:
            conn.execute("UPDATE simulation_runs SET status='stale' WHERE id=?", (row[0],))
            conn.commit()
        else:
        # Re-open as running if it was marked done
            conn.execute("UPDATE simulation_runs SET status='running' WHERE id=?", (row[0],))
            _mark_other_daily_live_runs_stale(conn, int(row[0]), config.name)
            conn.commit()
            return row[0]

    run_id = _create_run_row(
        conn, config, f"{period}-01", as_of, initial_cash_ars, mode="live",
        cost_model_version=cost_model_version,
    )
    _mark_other_daily_live_runs_stale(conn, run_id, config.name)
    conn.commit()
    return run_id


def _mark_other_daily_live_runs_stale(conn: sqlite3.Connection, keep_run_id: int, bot_name: str) -> None:
    conn.execute(
        """
        UPDATE simulation_runs
        SET status = 'stale'
        WHERE mode = 'live'
          AND status = 'running'
          AND id <> ?
          AND bot_config_id IN (
              SELECT id FROM simulation_bot_configs WHERE name = ?
          )
        """,
        (int(keep_run_id), bot_name),
    )


def _daily_live_metrics(
    conn: sqlite3.Connection,
    run_id: int,
    date_from: str,
    as_of: str,
    initial_cash_ars: float,
    final_value: float,
) -> Dict[str, Any]:
    rows = conn.execute(
        """
        SELECT trade_date, amount_ars, portfolio_value_after
        FROM simulation_trades
        WHERE run_id = ?
        ORDER BY trade_date, id
        """,
        (int(run_id),),
    ).fetchall()
    curve: EquityCurve = [(date_from, float(initial_cash_ars))]
    total_traded = 0.0
    for row in rows:
        total_traded += float(row["amount_ars"] or 0.0)
        if row["portfolio_value_after"] is not None:
            curve.append((str(row["trade_date"]), float(row["portfolio_value_after"])))
    if not curve or curve[-1][0] != as_of or abs(curve[-1][1] - final_value) > 0.01:
        curve.append((as_of, float(final_value)))
    metrics = build_metrics_dict(curve, [], total_traded)
    metrics["total_trades"] = len(rows)
    return metrics


def _reconstruct_portfolio(
    conn: sqlite3.Connection,
    run_id: int,
    initial_cash_ars: float,
    cost_model: Optional[ExecutionCostModel] = None,
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

    portfolio = SimulatedPortfolio(cash_ars=initial_cash_ars, cost_model=cost_model or ExecutionCostModel())
    cost_basis: Dict[str, float] = {}

    for symbol, action, quantity, price, amount_ars in trades:
        if price and price > 0:
            if action == "buy":
                portfolio.buy(symbol, amount_ars, price)
                if symbol in portfolio.holdings:
                    cost_basis[symbol] = portfolio.holdings[symbol].avg_price
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
    cost_model: Optional[ExecutionCostModel] = None,
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
    cost_model = cost_model or ExecutionCostModel()

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

        run_id = _find_or_create_live_run(
            conn, config, as_of, initial_cash_ars, cost_model_version=cost_model.version
        )
        portfolio, cost_basis = _reconstruct_portfolio(conn, run_id, initial_cash_ars, cost_model)

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
        run_row = conn.execute(
            "SELECT date_from, initial_value_ars FROM simulation_runs WHERE id = ?",
            (run_id,),
        ).fetchone()
        run_date_from = str(run_row["date_from"] if run_row else as_of)
        run_initial = float(run_row["initial_value_ars"] if run_row and run_row["initial_value_ars"] else initial_cash_ars)
        metrics = _daily_live_metrics(conn, run_id, run_date_from, as_of, run_initial, final_val)
        conn.execute(
            """
            UPDATE simulation_runs SET
                date_to = ?,
                status = 'running',
                final_value_ars = ?,
                total_return_pct = ?,
                sharpe_ratio = ?,
                max_drawdown_pct = ?,
                win_rate_pct = ?,
                total_trades = ?,
                metrics_json = ?,
                engine_driven = 1,
                avg_regime_score = ?,
                cost_model_version = ?
            WHERE id = ?
            """,
            (
                as_of,
                round(final_val, 2),
                metrics["total_return_pct"],
                metrics["sharpe_ratio"],
                metrics["max_drawdown_pct"],
                metrics["win_rate_pct"],
                metrics["total_trades"],
                json.dumps(metrics),
                regime_score,
                cost_model.version,
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
