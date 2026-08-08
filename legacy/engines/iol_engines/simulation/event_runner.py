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
from .portfolio_sim import Position, SimulatedPortfolio, load_execution_metadata_for_date, load_prices_for_date, load_trading_dates
from .cost_model import ExecutionCostModel, ExecutionFill
from .pending_orders import UNFILLABLE_MIN_LOT, buy_is_unfillable_min_lot, cancel_pending_order


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
          AND candidate_status NOT IN ('suppressed', 'rejected')
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
    cost_model_version: Optional[str] = None,
) -> int:
    now = datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO event_simulation_runs
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
    trade_date: str,
    action: str,
    quantity: float,
    price: float,
    amount_ars: float,
    pnl_ars: Optional[float],
    event_type: str,
    event_description: str,
    portfolio_value: float,
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
        INSERT INTO event_simulation_trades
            (run_id, symbol, trade_date, action, quantity, price, amount_ars,
             pnl_ars, trigger_event_type, trigger_event_description, portfolio_value_after,
             gross_amount_ars, net_amount_ars, commission_ars, market_fee_ars,
             iva_ars, slippage_ars, total_cost_ars, execution_price,
             instrument_type, cost_model_json)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (run_id, symbol, trade_date, action, quantity, price,
         round(amount_ars, 2),
         round(pnl_ars, 2) if pnl_ars is not None else None,
         event_type, event_description, round(portfolio_value, 2), *fill_values),
    )


def _persist_event_pending_order(
    conn: sqlite3.Connection,
    run_id: int,
    signal_date: str,
    order: Dict[str, Any],
) -> None:
    conn.execute(
        """
        INSERT INTO event_pending_orders
            (run_id, signal_date, symbol, side, action, amount_ars, quantity,
             signal_price, trigger_event_type, trigger_event_description,
             status, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?)
        """,
        (
            run_id,
            signal_date,
            order["symbol"],
            order["side"],
            order["action"],
            order.get("amount_ars"),
            order.get("quantity"),
            order["signal_price"],
            order["event_type"],
            order.get("event_description"),
            datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
        ),
    )


_BENCHMARK_SYMBOL = "SPY"


def _get_benchmark_price(conn: sqlite3.Connection, symbol: str, date: str) -> Optional[float]:
    row = conn.execute(
        """
        SELECT last_price FROM market_symbol_snapshots
        WHERE symbol=? AND snapshot_date<=? AND last_price>0
        ORDER BY snapshot_date DESC LIMIT 1
        """,
        (symbol, date),
    ).fetchone()
    return float(row[0]) if row else None


def _load_open_price(conn: sqlite3.Connection, symbol: str, date: str) -> Optional[float]:
    row = conn.execute(
        "SELECT open FROM symbol_daily_ohlcv WHERE symbol=? AND trade_date=?",
        (symbol, date),
    ).fetchone()
    return float(row[0]) if row and row[0] else None


def _execute_event_pending_order(
    conn: sqlite3.Connection,
    run_id: int,
    date: str,
    order: Dict[str, Any],
    portfolio: SimulatedPortfolio,
    prices: Dict[str, float],
    metadata: Dict[str, Dict[str, object]],
    trade_pnls: List[float],
    total_traded_ref: List[float],
    total_trades_ref: List[int],
) -> bool:
    symbol = str(order.get("symbol") or "")
    if not symbol:
        return False
    open_price = _load_open_price(conn, symbol, date)
    price_source = "symbol_daily_ohlcv.open" if open_price else "fallback_last_price"
    exec_price = open_price or prices.get(symbol)
    if not exec_price:
        return False

    meta = metadata.get(symbol, {})
    instrument_type = portfolio.cost_model.instrument_type_for(symbol, meta.get("instrument_type"))
    volume_amount = meta.get("volume_amount") if isinstance(meta.get("volume_amount"), (int, float)) else None
    side = str(order.get("side") or "buy")
    action = str(order.get("action") or ("buy" if side == "buy" else "trim"))

    if side == "buy":
        amount_ars = float(order.get("amount_ars") or 0.0)
        if buy_is_unfillable_min_lot(
            portfolio,
            symbol=symbol,
            amount_ars=amount_ars,
            price=float(exec_price),
            instrument_type=instrument_type,
            price_source=price_source,
            volume_amount=volume_amount,
        ):
            cancel_pending_order(
                conn,
                "event_pending_orders",
                order.get("id"),
                as_of=date,
                price=float(exec_price),
                reason=UNFILLABLE_MIN_LOT,
            )
            conn.commit()
            return True
        fill = portfolio.buy(
            symbol,
            amount_ars,
            float(exec_price),
            instrument_type=instrument_type,
            price_source=price_source,
            volume_amount=volume_amount,
        )
    else:
        quantity = float(order.get("quantity") or 0.0)
        if quantity > 0:
            fill = portfolio.sell_quantity(
                symbol,
                quantity,
                float(exec_price),
                instrument_type=instrument_type,
                price_source=price_source,
                volume_amount=volume_amount,
            )
        else:
            fill = portfolio.sell(
                symbol,
                float(order.get("amount_ars") or 0.0),
                float(exec_price),
                instrument_type=instrument_type,
                price_source=price_source,
                volume_amount=volume_amount,
            )

    if fill.quantity <= 0:
        return False
    pnl = float(fill.realized_pnl_ars or 0.0) if side == "sell" else None
    if pnl is not None:
        trade_pnls.append(pnl)
    total_traded_ref[0] += fill.gross_amount_ars
    total_trades_ref[0] += 1
    pv = portfolio.mark_to_market(prices)
    _persist_trade(
        conn,
        run_id,
        symbol,
        date,
        action,
        fill.quantity,
        float(exec_price),
        fill.net_cash_impact_ars if side == "buy" else fill.gross_amount_ars,
        pnl,
        str(order.get("event_type") or ""),
        str(order.get("event_description") or ""),
        pv,
        fill=fill,
    )
    if order.get("id") is not None:
        conn.execute(
            "UPDATE event_pending_orders SET status='executed', execute_date=?, execute_price=? WHERE id=?",
            (date, float(exec_price), int(order["id"])),
        )
    conn.commit()
    return True


def _finalize_run(
    conn: sqlite3.Connection,
    run_id: int,
    curve: EquityCurve,
    trade_pnls: List[float],
    total_traded: float,
    total_trades: int,
    total_events: int,
    benchmark_return_pct: Optional[float] = None,
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
            total_events,
            total_trades,
            _BENCHMARK_SYMBOL,
            round(benchmark_return_pct, 2) if benchmark_return_pct is not None else None,
            run_id,
        ),
    )
    conn.commit()


def _mark_other_event_live_runs_stale(conn: sqlite3.Connection, keep_run_id: int, bot_name: str) -> None:
    stale_ids = [
        int(r[0])
        for r in conn.execute(
            """
            SELECT id FROM event_simulation_runs
            WHERE mode = 'live'
              AND status = 'running'
              AND bot_name = ?
              AND id <> ?
            """,
            (bot_name, int(keep_run_id)),
        ).fetchall()
    ]
    conn.execute(
        """
        UPDATE event_simulation_runs
        SET status = 'stale'
        WHERE mode = 'live'
          AND status = 'running'
          AND bot_name = ?
          AND id <> ?
        """,
        (bot_name, int(keep_run_id)),
    )
    if stale_ids:
        placeholders = ",".join("?" for _ in stale_ids)
        conn.execute(
            f"UPDATE event_pending_orders SET status='cancelled' WHERE status='pending' AND run_id IN ({placeholders})",
            stale_ids,
        )


def _event_live_metrics(
    conn: sqlite3.Connection,
    run_id: int,
    date_from: str,
    initial_cash_ars: float,
    as_of: str,
    final_value: float,
) -> Dict[str, Any]:
    rows = conn.execute(
        """
        SELECT trade_date, amount_ars, pnl_ars, portfolio_value_after
        FROM event_simulation_trades
        WHERE run_id = ?
        ORDER BY trade_date, id
        """,
        (int(run_id),),
    ).fetchall()
    curve: EquityCurve = [(date_from, float(initial_cash_ars))]
    for row in rows:
        if row["portfolio_value_after"] is not None:
            curve.append((str(row["trade_date"]), float(row["portfolio_value_after"])))
    if curve[-1][0] != as_of or abs(curve[-1][1] - final_value) > 0.01:
        curve.append((as_of, float(final_value)))
    trade_pnls = [float(r["pnl_ars"]) for r in rows if r["pnl_ars"] is not None]
    total_traded = sum(float(r["amount_ars"] or 0.0) for r in rows)
    metrics = build_metrics_dict(curve, trade_pnls, total_traded)
    metrics["total_trades"] = len(rows)
    return metrics


# ── Event reaction execution ──────────────────────────────────────────────────

def _execution_context(
    portfolio: SimulatedPortfolio,
    metadata: Dict[str, Dict[str, object]],
    symbol: str,
    *,
    price_source: str = "same_day_snapshot",
) -> Dict[str, Any]:
    meta = metadata.get(symbol, {})
    volume = meta.get("volume_amount")
    return {
        "instrument_type": portfolio.cost_model.instrument_type_for(symbol, meta.get("instrument_type")),
        "volume_amount": volume if isinstance(volume, (int, float)) else None,
        "price_source": price_source,
    }


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
    pending_orders: Optional[List[Dict[str, Any]]] = None,
) -> None:
    """Execute a single reaction rule triggered by an event."""
    total_value = portfolio.mark_to_market(prices)
    metadata = load_execution_metadata_for_date(conn, date)

    def pending_buy_amount() -> float:
        if pending_orders is None:
            return 0.0
        return sum(float(o.get("amount_ars") or 0.0) for o in pending_orders if o.get("side") == "buy")

    def pending_buy_symbols() -> set[str]:
        if pending_orders is None:
            return set()
        return {str(o.get("symbol")) for o in pending_orders if o.get("side") == "buy"}

    def queue_order(
        *,
        symbol: str,
        side: str,
        action: str,
        price: float,
        amount_ars: Optional[float] = None,
        quantity: Optional[float] = None,
    ) -> bool:
        if pending_orders is None:
            return False
        if side == "buy":
            exec_meta = _execution_context(portfolio, metadata, symbol)
            if buy_is_unfillable_min_lot(
                portfolio,
                symbol=symbol,
                amount_ars=float(amount_ars or 0.0),
                price=float(price),
                instrument_type=exec_meta["instrument_type"],
                price_source="same_day_snapshot",
                volume_amount=exec_meta["volume_amount"],
            ):
                return False
        pending_orders.append({
            "symbol": symbol,
            "side": side,
            "action": action,
            "amount_ars": amount_ars,
            "quantity": quantity,
            "signal_price": price,
            "event_type": event.event_type,
            "event_description": event.description,
        })
        return True

    if rule.reaction == "buy_top_candidates":
        candidates = [
            (sym, score) for sym, score in opp_scores
            if sym in prices
            and score >= config.min_engine_score
            and sym not in pending_buy_symbols()
            and portfolio.n_positions + len(pending_buy_symbols()) < config.max_positions
        ][:rule.top_n]
        cash_to_deploy = total_value * rule.magnitude_pct
        per_position = cash_to_deploy / max(len(candidates), 1)
        min_cash = total_value * config.cash_reserve_pct

        for sym, _ in candidates:
            if portfolio.n_positions + len(pending_buy_symbols()) >= config.max_positions:
                break
            price = prices.get(sym)
            if not price:
                continue
            amount = min(per_position, portfolio.cash_ars - min_cash - pending_buy_amount())
            if amount < 500:
                continue
            if queue_order(symbol=sym, side="buy", action="buy", price=price, amount_ars=amount):
                continue
            fill = portfolio.buy(sym, amount, price, **_execution_context(portfolio, metadata, sym))
            if fill.quantity <= 0:
                continue
            cost_basis[sym] = portfolio.holdings[sym].avg_price
            total_traded_ref[0] += fill.gross_amount_ars
            total_trades_ref[0] += 1
            pv = portfolio.mark_to_market(prices)
            _persist_trade(conn, run_id, sym, date, "buy", fill.quantity, fill.effective_price,
                           fill.net_cash_impact_ars, None, event.event_type, event.description, pv, fill=fill)

    elif rule.reaction == "trim_all":
        for sym in list(portfolio.holdings.keys()):
            price = prices.get(sym, 0)
            if not price:
                continue
            pos_val = portfolio.position_value(sym, price)
            trim_amount = pos_val * rule.magnitude_pct
            if trim_amount < 100:
                continue
            pos = portfolio.holdings.get(sym)
            if not pos:
                continue
            quantity = min(pos.quantity, trim_amount / price)
            if queue_order(symbol=sym, side="sell", action="trim", price=price, quantity=quantity):
                continue
            fill = portfolio.sell(sym, trim_amount, price, **_execution_context(portfolio, metadata, sym))
            if fill.quantity <= 0:
                continue
            pnl = float(fill.realized_pnl_ars or 0.0)
            trade_pnls.append(pnl)
            total_traded_ref[0] += fill.gross_amount_ars
            total_trades_ref[0] += 1
            pv = portfolio.mark_to_market(prices)
            _persist_trade(conn, run_id, sym, date, "trim", fill.quantity, fill.effective_price,
                           fill.gross_amount_ars, pnl, event.event_type, event.description, pv, fill=fill)

    elif rule.reaction == "exit_all":
        for sym in list(portfolio.holdings.keys()):
            price = prices.get(sym, 0)
            if not price:
                continue
            pos_val = portfolio.position_value(sym, price)
            pos = portfolio.holdings.get(sym)
            if pos and queue_order(symbol=sym, side="sell", action="exit", price=price, quantity=pos.quantity):
                continue
            fill = portfolio.sell(sym, pos_val, price, **_execution_context(portfolio, metadata, sym))
            if fill.quantity <= 0:
                continue
            pnl = float(fill.realized_pnl_ars or 0.0)
            trade_pnls.append(pnl)
            total_traded_ref[0] += fill.gross_amount_ars
            total_trades_ref[0] += 1
            pv = portfolio.mark_to_market(prices)
            _persist_trade(conn, run_id, sym, date, "exit", fill.quantity, fill.effective_price,
                           fill.gross_amount_ars, pnl, event.event_type, event.description, pv, fill=fill)

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
            pos = portfolio.holdings.get(sym)
            action = "exit" if liquidate >= pos_val * 0.95 else "trim"
            if pos and queue_order(
                symbol=sym,
                side="sell",
                action=action,
                price=price,
                quantity=min(pos.quantity, liquidate / price),
            ):
                remaining_deficit -= liquidate
                continue
            fill = portfolio.sell(sym, liquidate, price, **_execution_context(portfolio, metadata, sym))
            if fill.quantity <= 0:
                continue
            pnl = float(fill.realized_pnl_ars or 0.0)
            trade_pnls.append(pnl)
            total_traded_ref[0] += fill.gross_amount_ars
            total_trades_ref[0] += 1
            remaining_deficit -= liquidate
            pv = portfolio.mark_to_market(prices)
            _persist_trade(conn, run_id, sym, date, action, fill.quantity, fill.effective_price,
                           fill.gross_amount_ars, pnl, event.event_type, event.description, pv, fill=fill)

    elif rule.reaction == "buy_symbol":
        sym = event.symbol or rule.symbol
        if not sym or sym not in prices:
            return
        price = prices[sym]
        min_cash = total_value * config.cash_reserve_pct
        amount = min(total_value * rule.magnitude_pct, portfolio.cash_ars - min_cash - pending_buy_amount())
        if amount < 500 or portfolio.n_positions + len(pending_buy_symbols()) >= config.max_positions:
            return
        if queue_order(symbol=sym, side="buy", action="buy", price=price, amount_ars=amount):
            return
        fill = portfolio.buy(sym, amount, price, **_execution_context(portfolio, metadata, sym))
        if fill.quantity <= 0:
            return
        cost_basis[sym] = portfolio.holdings[sym].avg_price
        total_traded_ref[0] += fill.gross_amount_ars
        total_trades_ref[0] += 1
        pv = portfolio.mark_to_market(prices)
        _persist_trade(conn, run_id, sym, date, "buy", fill.quantity, fill.effective_price,
                       fill.net_cash_impact_ars, None, event.event_type, event.description, pv, fill=fill)

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
        pos = portfolio.holdings.get(sym)
        action = "exit" if rule.magnitude_pct >= 0.95 else "trim"
        if pos and queue_order(
            symbol=sym,
            side="sell",
            action=action,
            price=price,
            quantity=min(pos.quantity, sell_amount / price),
        ):
            return
        fill = portfolio.sell(sym, sell_amount, price, **_execution_context(portfolio, metadata, sym))
        if fill.quantity <= 0:
            return
        pnl = float(fill.realized_pnl_ars or 0.0)
        trade_pnls.append(pnl)
        total_traded_ref[0] += fill.gross_amount_ars
        total_trades_ref[0] += 1
        pv = portfolio.mark_to_market(prices)
        _persist_trade(conn, run_id, sym, date, action, fill.quantity, fill.effective_price,
                       fill.gross_amount_ars, pnl, event.event_type, event.description, pv, fill=fill)

    conn.commit()


# ── Main runner ───────────────────────────────────────────────────────────────

def run_event_backtest(
    conn: sqlite3.Connection,
    config: EventBotConfig,
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
    """Run a full event-driven backtest. Returns event_simulation_runs.id."""

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
        f"[bold]Event backtest run #{run_id}[/bold] bot=[cyan]{config.name}[/cyan] "
        f"{date_from} to {date_to} | commission={commission_rate*100:.2f}% slippage={config.slippage_pct*100:.2f}%"
    )

    trading_dates = load_trading_dates(conn, date_from, date_to)
    if not trading_dates:
        _finalize_run(conn, run_id, [], [], 0.0, 0, 0)
        log("[red]No market data found in date range.[/red]")
        return run_id

    portfolio = SimulatedPortfolio(
        cash_ars=initial_cash_ars,
        commission_rate=commission_rate,
        commission_min=commission_min,
        slippage_pct=config.slippage_pct,
        cost_model=cost_model,
    )
    bm_price_start = _get_benchmark_price(conn, _BENCHMARK_SYMBOL, date_from)
    curve: EquityCurve = []
    trade_pnls: List[float] = []
    total_traded_ref = [0.0]
    total_trades_ref = [0]
    total_events = 0
    cost_basis: Dict[str, float] = {}
    pending_orders: List[Dict[str, Any]] = []

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

        metadata = load_execution_metadata_for_date(conn, date)
        carry_forward: List[Dict[str, Any]] = []
        for order in list(pending_orders):
            executed = _execute_event_pending_order(
                conn, run_id, date, order, portfolio, prices, metadata,
                trade_pnls, total_traded_ref, total_trades_ref,
            )
            if not executed:
                carry_forward.append(order)
        pending_orders[:] = carry_forward

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
                            pending_orders=pending_orders,
                        )

        if i % 20 == 0:
            log(
                f"  {date}  value=[yellow]ARS {total_value:,.0f}[/yellow]  "
                f"cash={portfolio.cash_ars:,.0f}  positions={portfolio.n_positions}  "
                f"events={total_events}"
            )

    bm_price_end = _get_benchmark_price(conn, _BENCHMARK_SYMBOL, date_to)
    bm_return: Optional[float] = None
    if bm_price_start and bm_price_end:
        bm_return = (bm_price_end - bm_price_start) / bm_price_start * 100.0

    _finalize_run(
        conn, run_id, curve, trade_pnls,
        total_traded_ref[0], total_trades_ref[0], total_events, bm_return,
    )
    final_val = curve[-1][1] if curve else initial_cash_ars
    ret = (final_val - initial_cash_ars) / initial_cash_ars * 100 if initial_cash_ars else 0
    bm_str = f"  benchmark({_BENCHMARK_SYMBOL})=[cyan]{bm_return:+.1f}%[/cyan]" if bm_return is not None else ""
    log(
        f"\n[bold green]Event backtest complete.[/bold green] "
        f"Return: [{'green' if ret >= 0 else 'red'}]{ret:+.1f}%[/]{bm_str}  "
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
    commission_rate: float = 0.0,
    commission_min: float = 0.0,
    verbose: bool = True,
    cost_model: Optional[ExecutionCostModel] = None,
) -> List[int]:
    """Execute one daily event-driven step for each bot."""

    def log(msg: str) -> None:
        if verbose:
            from rich.console import Console
            Console().print(msg)

    enforce_cost_model_version = cost_model is not None
    cost_model = cost_model or ExecutionCostModel.from_config(
        commission_min=commission_min,
        legacy_commission_rate=commission_rate if commission_rate > 0 else None,
    )
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
        if row:
            run_id = row[0]
            version_row = conn.execute(
                "SELECT cost_model_version FROM event_simulation_runs WHERE id = ?",
                (run_id,),
            ).fetchone()
            if enforce_cost_model_version and version_row and version_row[0] != cost_model.version:
                conn.execute("UPDATE event_simulation_runs SET status='stale' WHERE id=?", (run_id,))
                conn.execute("UPDATE event_pending_orders SET status='cancelled' WHERE run_id=? AND status='pending'", (run_id,))
                run_id = _create_run_row(
                    conn, bot_name, f"{period}-01", as_of, initial_cash_ars,
                    mode="live", cost_model_version=cost_model.version,
                )
        else:
            run_id = _create_run_row(
                conn, bot_name, f"{period}-01", as_of, initial_cash_ars, mode="live",
                cost_model_version=cost_model.version if enforce_cost_model_version else None,
            )
        _mark_other_event_live_runs_stale(conn, int(run_id), bot_name)
        conn.commit()

        # Reconstruct portfolio from trade history
        portfolio = SimulatedPortfolio(
            cash_ars=initial_cash_ars,
            commission_rate=commission_rate,
            commission_min=commission_min,
            slippage_pct=config.slippage_pct,
            cost_model=cost_model,
        )
        cost_basis: Dict[str, float] = {}
        trade_rows = conn.execute(
            """
            SELECT symbol, action, quantity, price, amount_ars, net_amount_ars,
                   total_cost_ars, cost_model_json
            FROM event_simulation_trades
            WHERE run_id = ?
            ORDER BY rowid ASC
            """,
            (run_id,),
        ).fetchall()
        for sym, action, qty, price, amount, net_amount, total_cost, cost_json in trade_rows:
            if action == "buy" and price:
                qty_f = float(qty or 0.0)
                amount_f = float(amount or 0.0)
                if qty_f > 0:
                    existing = portfolio.holdings.get(sym)
                    if existing:
                        total_qty = existing.quantity + qty_f
                        total_basis = existing.cost_basis + amount_f
                        portfolio.holdings[sym] = Position(sym, total_qty, total_basis / total_qty)
                    else:
                        portfolio.holdings[sym] = Position(sym, qty_f, amount_f / qty_f)
                    portfolio.cash_ars -= amount_f
                    cost_basis[sym] = portfolio.holdings[sym].avg_price
            elif action in ("trim", "exit") and price:
                qty_f = float(qty or 0.0)
                if qty_f <= 0 or sym not in portfolio.holdings:
                    continue
                try:
                    fill_data = json.loads(cost_json or "{}")
                except Exception:
                    fill_data = {}
                cash_in = (
                    float(fill_data.get("net_cash_impact_ars"))
                    if fill_data.get("net_cash_impact_ars") is not None
                    else float(net_amount or 0.0) - float(total_cost or 0.0)
                )
                if cash_in <= 0:
                    cash_in = float(amount or 0.0) - float(total_cost or 0.0)
                pos = portfolio.holdings[sym]
                remaining = pos.quantity - qty_f
                if remaining < 0.0001:
                    del portfolio.holdings[sym]
                    cost_basis.pop(sym, None)
                else:
                    portfolio.holdings[sym] = Position(sym, remaining, pos.avg_price)
                    cost_basis[sym] = pos.avg_price
                portfolio.cash_ars += cash_in

        already_today = conn.execute(
            """
            SELECT 1 FROM event_simulation_trades WHERE run_id=? AND trade_date=?
            UNION ALL
            SELECT 1 FROM event_pending_orders WHERE run_id=? AND signal_date=?
            LIMIT 1
            """,
            (run_id, as_of, run_id, as_of),
        ).fetchone()
        if already_today:
            log(f"  [dim]{bot_name}[/dim] - event step already processed for {as_of}, skipping.")
            run_ids.append(run_id)
            continue

        total_events_triggered = 0
        total_trades_ref = [0]
        trade_pnls: List[float] = []
        total_traded_ref = [0.0]
        pending_rows = conn.execute(
            """
            SELECT id, symbol, side, action, amount_ars, quantity, signal_price,
                   trigger_event_type, trigger_event_description
            FROM event_pending_orders
            WHERE run_id=? AND status='pending'
            ORDER BY id ASC
            """,
            (run_id,),
        ).fetchall()
        pending_orders: List[Dict[str, Any]] = [
            {
                "id": row["id"],
                "symbol": row["symbol"],
                "side": row["side"],
                "action": row["action"],
                "amount_ars": row["amount_ars"],
                "quantity": row["quantity"],
                "signal_price": row["signal_price"],
                "event_type": row["trigger_event_type"],
                "event_description": row["trigger_event_description"],
            }
            for row in pending_rows
        ]
        metadata = load_execution_metadata_for_date(conn, as_of)
        carry_forward: List[Dict[str, Any]] = []
        for order in list(pending_orders):
            executed = _execute_event_pending_order(
                conn, run_id, as_of, order, portfolio, prices, metadata,
                trade_pnls, total_traded_ref, total_trades_ref,
            )
            if not executed:
                carry_forward.append(order)
        pending_orders[:] = carry_forward

        # Check cooldown after pending executions so a T+1 fill prevents
        # immediately re-queuing the same still-latest engine event.
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
                        pending_orders=pending_orders,
                    )

        queued_today = 0
        for order in pending_orders:
            if order.get("id") is None:
                _persist_event_pending_order(conn, run_id, as_of, order)
                queued_today += 1
        conn.commit()

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

        # Benchmark return
        period = as_of[:7]
        bm_start = _get_benchmark_price(conn, _BENCHMARK_SYMBOL, f"{period}-01")
        bm_end = _get_benchmark_price(conn, _BENCHMARK_SYMBOL, as_of)
        bm_return: Optional[float] = None
        if bm_start and bm_end:
            bm_return = (bm_end - bm_start) / bm_start * 100.0

        plan = {
            "as_of": as_of,
            "regime": _reg[0] if _reg else "unknown",
            "regime_score": round(float(_reg[1]), 1) if _reg else 50.0,
            "macro_stress": round(float(_mac[0]), 1) if _mac else 50.0,
            "events_triggered": total_events_triggered,
            "pending_tomorrow": queued_today,
            "entries": step_entries,
            "exits": step_exits,
            "portfolio_value_ars": round(final_val, 2),
            "open_positions": list(portfolio.holdings.keys()),
            "benchmark_symbol": _BENCHMARK_SYMBOL,
            "benchmark_return_pct": round(bm_return, 2) if bm_return is not None else None,
        }
        run_row = conn.execute(
            "SELECT date_from, initial_cash FROM event_simulation_runs WHERE id = ?",
            (run_id,),
        ).fetchone()
        run_date_from = str(run_row["date_from"] if run_row else f"{period}-01")
        run_initial = float(run_row["initial_cash"] if run_row and run_row["initial_cash"] else initial_cash_ars)
        metrics = _event_live_metrics(conn, run_id, run_date_from, run_initial, as_of, final_val)

        conn.execute(
            """
            UPDATE event_simulation_runs SET
                final_value=?,
                total_return_pct=?,
                sharpe_ratio=?,
                max_drawdown_pct=?,
                win_rate_pct=?,
                total_events_triggered=?,
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
                int(total_events_triggered),
                metrics["total_trades"],
                _BENCHMARK_SYMBOL,
                round(bm_return, 2) if bm_return is not None else None,
                json.dumps(plan),
                cost_model.version,
                run_id,
            ),
        )
        conn.commit()
        bm_str = f"  bm={bm_return:+.1f}%" if bm_return is not None else ""
        log(
            f"  [cyan]{bot_name}[/cyan] #{run_id}  "
            f"value=[yellow]ARS {final_val:,.0f}[/yellow]  "
            f"positions={portfolio.n_positions}  "
            f"events_reacted={total_events_triggered}{bm_str}"
        )
        run_ids.append(run_id)

    return run_ids


def _days_between(date_from: str, date_to: str) -> int:
    from datetime import date
    try:
        return max(0, (date.fromisoformat(date_to) - date.fromisoformat(date_from)).days)
    except (ValueError, TypeError):
        return 0
