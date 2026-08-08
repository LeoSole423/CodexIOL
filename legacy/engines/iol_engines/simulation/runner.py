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
from dataclasses import dataclass
from datetime import date as date_type, datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from .bot_config import BotConfig, get_preset
from .metrics import EquityCurve, build_metrics_dict
from .portfolio_sim import (
    Position,
    SimulatedPortfolio,
    load_execution_metadata_for_date,
    load_prices_for_date,
    load_trading_dates,
)
from .cost_model import COST_MODEL_VERSION, ExecutionCostModel, ExecutionFill
from .pending_orders import UNFILLABLE_MIN_LOT, buy_is_unfillable_min_lot, cancel_pending_order


# Only re-run engine signal lookup every N trading days (they are date-keyed
# and rarely change day-to-day, so this avoids repeated DB queries)
_ENGINE_REFRESH_EVERY_N_DAYS = 5


@dataclass(frozen=True)
class DailyPositionState:
    symbol: str
    quantity: float
    avg_price: float
    entry_date: str
    days_held: int
    peak_price: float


# ── DB helpers ────────────────────────────────────────────────────────────────

def _load_opportunity_candidates(
    conn: sqlite3.Connection,
    as_of: str,
    top_n: Optional[int] = 30,
    *,
    operable_only: bool = True,
) -> List[Dict[str, Any]]:
    """Load opportunity candidates from the most recent run on/before as_of."""
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
    status_clause = "AND candidate_status = 'operable'" if operable_only else ""
    limit_clause = "LIMIT ?" if top_n is not None else ""
    params: Tuple[Any, ...] = (run_id, int(top_n)) if top_n is not None else (run_id,)
    cur.execute(
        f"""
        SELECT symbol, signal_side, signal_family, score_total,
               score_risk, score_value, score_momentum, score_catalyst,
               suggested_weight_pct, suggested_amount_ars, reason_summary, sector_bucket,
               candidate_status
        FROM advisor_opportunity_candidates
        WHERE run_id = ?
          {status_clause}
        ORDER BY score_total DESC {limit_clause}
        """,
        params,
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


def _load_open_price(conn: sqlite3.Connection, symbol: str, date: str) -> Optional[float]:
    row = conn.execute(
        "SELECT open FROM symbol_daily_ohlcv WHERE symbol=? AND trade_date=?",
        (symbol, date),
    ).fetchone()
    return float(row[0]) if row and row[0] else None


def _parse_iso_date(value: str) -> date_type:
    return date_type.fromisoformat(str(value)[:10])


def _trading_days_held(conn: sqlite3.Connection, symbol: str, entry_date: str, as_of: str) -> int:
    row = conn.execute(
        """
        SELECT COUNT(DISTINCT snapshot_date)
        FROM market_symbol_snapshots
        WHERE symbol=? AND snapshot_date>? AND snapshot_date<=?
        """,
        (symbol, entry_date, as_of),
    ).fetchone()
    count = int(row[0] or 0) if row else 0
    if count:
        return count
    try:
        return max(0, (_parse_iso_date(as_of) - _parse_iso_date(entry_date)).days)
    except ValueError:
        return 0


def _peak_price_since_entry(
    conn: sqlite3.Connection,
    symbol: str,
    entry_date: str,
    as_of: str,
    fallback: float,
) -> float:
    row = conn.execute(
        """
        SELECT MAX(last_price)
        FROM market_symbol_snapshots
        WHERE symbol=? AND snapshot_date>=? AND snapshot_date<=? AND last_price>0
        """,
        (symbol, entry_date, as_of),
    ).fetchone()
    return max(float(row[0] or 0.0), float(fallback or 0.0))


def _daily_position_states(
    conn: sqlite3.Connection,
    run_id: int,
    as_of: str,
    portfolio: SimulatedPortfolio,
    prices: Dict[str, float],
) -> Dict[str, DailyPositionState]:
    rows = conn.execute(
        """
        SELECT trade_date, symbol, action, quantity
        FROM simulation_trades
        WHERE run_id = ? AND trade_date <= ?
        ORDER BY trade_date, id
        """,
        (run_id, as_of),
    ).fetchall()
    entry_dates: Dict[str, str] = {}
    quantities: Dict[str, float] = {}
    for row in rows:
        symbol = str(row["symbol"])
        action = str(row["action"] or "").lower()
        qty = float(row["quantity"] or 0.0)
        if qty <= 0:
            continue
        current_qty = quantities.get(symbol, 0.0)
        if action == "buy":
            if current_qty <= 0:
                entry_dates[symbol] = str(row["trade_date"])
            quantities[symbol] = current_qty + qty
        elif action in ("sell", "trim", "exit"):
            remaining = current_qty - qty
            if remaining <= 0.0001:
                quantities.pop(symbol, None)
                entry_dates.pop(symbol, None)
            else:
                quantities[symbol] = remaining

    states: Dict[str, DailyPositionState] = {}
    for symbol, pos in portfolio.holdings.items():
        current_price = float(prices.get(symbol) or pos.avg_price)
        entry_date = entry_dates.get(symbol, as_of)
        states[symbol] = DailyPositionState(
            symbol=symbol,
            quantity=pos.quantity,
            avg_price=pos.avg_price,
            entry_date=entry_date,
            days_held=_trading_days_held(conn, symbol, entry_date, as_of),
            peak_price=_peak_price_since_entry(conn, symbol, entry_date, as_of, current_price),
        )
    return states


def _candidates_by_symbol(candidates: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    by_symbol: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for candidate in candidates:
        symbol = str(candidate.get("symbol") or "")
        if symbol:
            by_symbol[symbol].append(candidate)
    return by_symbol


def _daily_exit_order_for_position(
    state: DailyPositionState,
    price: float,
    config: BotConfig,
    candidates: List[Dict[str, Any]],
    pending_sell_symbols: set[str],
) -> Optional[Dict[str, Any]]:
    if state.symbol in pending_sell_symbols or price <= 0:
        return None

    advisor_sell = next(
        (
            c for c in candidates
            if str(c.get("signal_side") or "").lower() == "sell"
            and str(c.get("candidate_status") or "").lower() == "operable"
        ),
        None,
    )
    action = "exit"
    reason = ""
    quantity = state.quantity
    amount = state.quantity * price

    if advisor_sell:
        family = str(advisor_sell.get("signal_family") or "trim").lower()
        action = "exit" if family == "exit" else "trim"
        if action == "trim":
            quantity = state.quantity * 0.33
            amount = quantity * price
        reason = f"daily_exit:advisor_sell:{family}"
    elif price <= state.avg_price * (1.0 - config.stop_loss_pct):
        reason = "daily_exit:stop_loss"
    elif price >= state.avg_price * (1.0 + config.take_profit_pct):
        reason = "daily_exit:take_profit"
    elif (
        state.days_held >= config.min_hold_days
        and state.peak_price > 0
        and price <= state.peak_price * (1.0 - config.trailing_stop_pct)
    ):
        reason = "daily_exit:trailing_stop"
    elif state.days_held >= config.max_hold_days:
        reason = "daily_exit:time_stop"
    else:
        latest = candidates[0] if candidates else None
        if latest is not None:
            status = str(latest.get("candidate_status") or "").lower()
            score = float(latest.get("score_total") or 0.0)
            if status in ("suppressed", "rejected") or score < config.exit_score_threshold:
                reason = "daily_exit:score_deterioration"

    if not reason or quantity <= 0 or amount <= 0:
        return None
    return {
        "symbol": state.symbol,
        "side": "sell",
        "action": action,
        "amount_ars": amount,
        "quantity": quantity,
        "signal_price": price,
        "reason": reason,
        "engine_source": "daily_exit_policy",
    }


def _queue_daily_exit_orders(
    conn: sqlite3.Connection,
    run_id: int,
    as_of: str,
    config: BotConfig,
    portfolio: SimulatedPortfolio,
    prices: Dict[str, float],
    candidates: List[Dict[str, Any]],
    pending_orders: Optional[List[Dict[str, Any]]],
) -> set[str]:
    if pending_orders is None or not portfolio.holdings:
        return set()
    by_symbol = _candidates_by_symbol(candidates)
    pending_sell_symbols = {
        str(order.get("symbol"))
        for order in pending_orders
        if str(order.get("side") or "").lower() == "sell"
    }
    queued: set[str] = set()
    states = _daily_position_states(conn, run_id, as_of, portfolio, prices)
    for symbol, state in states.items():
        price = float(prices.get(symbol) or 0.0)
        order = _daily_exit_order_for_position(
            state,
            price,
            config,
            by_symbol.get(symbol, []),
            pending_sell_symbols | queued,
        )
        if order is None:
            continue
        pending_orders.append(order)
        queued.add(symbol)
    return queued


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


def _persist_pending_order(
    conn: sqlite3.Connection,
    run_id: int,
    signal_date: str,
    order: Dict[str, Any],
) -> None:
    conn.execute(
        """
        INSERT INTO simulation_pending_orders
            (run_id, signal_date, symbol, side, action, amount_ars, quantity,
             signal_price, reason, engine_source, status, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?)
        """,
        (
            run_id,
            signal_date,
            order["symbol"],
            order["side"],
            order.get("action"),
            order.get("amount_ars"),
            order.get("quantity"),
            order["signal_price"],
            order.get("reason"),
            order.get("engine_source"),
            datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
        ),
    )


def _execute_pending_order(
    conn: sqlite3.Connection,
    run_id: int,
    date: str,
    order: Dict[str, Any],
    portfolio: SimulatedPortfolio,
    prices: Dict[str, float],
    metadata: Dict[str, Dict[str, object]],
    trade_pnls: List[float],
    total_traded_ref: List[float],
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
                "simulation_pending_orders",
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
    if side == "sell":
        trade_pnls.append(float(fill.realized_pnl_ars or 0.0))
    pv = portfolio.mark_to_market(prices)
    _persist_trade(
        conn,
        run_id,
        date,
        symbol,
        action,
        fill.quantity,
        float(exec_price),
        fill.gross_amount_ars,
        pv,
        str(order.get("reason") or ""),
        str(order.get("engine_source") or "simulation_t1"),
        fill=fill,
    )
    total_traded_ref[0] += fill.gross_amount_ars
    if order.get("id") is not None:
        conn.execute(
            "UPDATE simulation_pending_orders SET status='executed', execute_date=?, execute_price=? WHERE id=?",
            (date, float(exec_price), int(order["id"])),
        )
    conn.commit()
    return True


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
    total_trades = conn.execute(
        "SELECT COUNT(*) FROM simulation_trades WHERE run_id = ?",
        (run_id,),
    ).fetchone()[0]
    metrics["total_trades"] = int(total_trades or 0)
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
            metrics["total_trades"],
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
    pending_orders: Optional[List[Dict[str, Any]]] = None,
) -> float:
    """Execute one trading day. Returns portfolio value after mark-to-market."""
    prices = load_prices_for_date(conn, date)
    metadata = load_execution_metadata_for_date(conn, date)

    if pending_orders is not None:
        carry_forward: List[Dict[str, Any]] = []
        for order in list(pending_orders):
            executed = _execute_pending_order(
                conn, run_id, date, order, portfolio, prices, metadata,
                trade_pnls, total_traded_ref,
            )
            if not executed:
                carry_forward.append(order)
        pending_orders[:] = carry_forward

    total_value = portfolio.mark_to_market(prices)
    curve.append((date, total_value))

    all_candidates = _load_opportunity_candidates(conn, date, top_n=500, operable_only=False)

    # Apply engine rescoring
    if use_engines and (regime is not None or macro is not None or smart_money):
        all_candidates = _rescore_with_engines(all_candidates, regime, macro, smart_money or [], config)

    exiting_symbols = _queue_daily_exit_orders(
        conn, run_id, date, config, portfolio, prices, all_candidates, pending_orders
    )

    # Adjust threshold: tighten in stressed macro environment
    threshold = config.min_score_threshold
    if use_engines and macro is not None and macro.argentina_macro_stress > 70:
        threshold = min(threshold * 1.10, 95.0)

    candidates = [
        c for c in all_candidates
        if str(c.get("candidate_status") or "").lower() == "operable"
        and str(c.get("signal_side") or "buy").lower() == "buy"
        and float(c.get("score_total") or 0) >= threshold
    ]

    spendable = total_value * (1 - config.cash_reserve_pct)
    equity_budget = min(spendable - (total_value - portfolio.cash_ars),
                        portfolio.cash_ars * (1 - config.cash_reserve_pct))
    deployed = 0.0
    pending_sell_symbols_for_entries = {
        str(order.get("symbol"))
        for order in pending_orders or []
        if str(order.get("side") or "").lower() == "sell"
    }

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

        if symbol in exiting_symbols or symbol in pending_sell_symbols_for_entries:
            continue

        if side == "sell":
            action_type = (c.get("signal_family") or "trim").lower()
            pos_value = portfolio.position_value(symbol, price)
            if pos_value <= 0:
                continue
            amount = pos_value if action_type == "exit" else pos_value * 0.33
            quantity = portfolio.holdings[symbol].quantity if action_type == "exit" else amount / price

            if pending_orders is not None:
                pending_orders.append({
                    "symbol": symbol,
                    "side": "sell",
                    "action": action_type,
                    "amount_ars": amount,
                    "quantity": quantity,
                    "signal_price": price,
                    "reason": reason,
                    "engine_source": engine_source,
                })
                continue

            fill = portfolio.sell(
                symbol,
                amount,
                price,
                instrument_type=instrument_type,
                price_source="same_day_snapshot",
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
            pending_buy_count = (
                len({o["symbol"] for o in pending_orders if o.get("side") == "buy"})
                if pending_orders is not None else 0
            )
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
            if buy_is_unfillable_min_lot(
                portfolio,
                symbol=symbol,
                amount_ars=amount,
                price=float(price),
                instrument_type=instrument_type,
                price_source="same_day_snapshot",
                volume_amount=volume_amount if isinstance(volume_amount, (int, float)) else None,
            ):
                continue

            if pending_orders is not None:
                pending_orders.append({
                    "symbol": symbol,
                    "side": "buy",
                    "action": "buy",
                    "amount_ars": amount,
                    "quantity": None,
                    "signal_price": price,
                    "reason": reason,
                    "engine_source": engine_source,
                })
                deployed += amount
                if portfolio.n_positions + pending_buy_count + 1 >= config.max_positions:
                    break
                continue

            fill = portfolio.buy(
                symbol,
                amount,
                price,
                instrument_type=instrument_type,
                price_source="same_day_snapshot",
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
    pending_orders: List[Dict[str, Any]] = []

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
                pending_orders=pending_orders,
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
            conn.execute("UPDATE simulation_pending_orders SET status='cancelled' WHERE run_id=? AND status='pending'", (row[0],))
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
    stale_ids = [
        int(r[0])
        for r in conn.execute(
            """
            SELECT id FROM simulation_runs
            WHERE mode = 'live'
              AND status = 'running'
              AND id <> ?
              AND bot_config_id IN (
                  SELECT id FROM simulation_bot_configs WHERE name = ?
              )
            """,
            (int(keep_run_id), bot_name),
        ).fetchall()
    ]
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
    if stale_ids:
        placeholders = ",".join("?" for _ in stale_ids)
        conn.execute(
            f"UPDATE simulation_pending_orders SET status='cancelled' WHERE status='pending' AND run_id IN ({placeholders})",
            stale_ids,
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
        SELECT trade_date, action, amount_ars, portfolio_value_after, cost_model_json
        FROM simulation_trades
        WHERE run_id = ?
        ORDER BY trade_date, id
        """,
        (int(run_id),),
    ).fetchall()
    curve: EquityCurve = [(date_from, float(initial_cash_ars))]
    total_traded = 0.0
    trade_pnls: List[float] = []
    for row in rows:
        total_traded += float(row["amount_ars"] or 0.0)
        if str(row["action"] or "").lower() in ("sell", "trim", "exit"):
            try:
                meta = json.loads(row["cost_model_json"] or "{}")
            except Exception:
                meta = {}
            if meta.get("realized_pnl_ars") is not None:
                trade_pnls.append(float(meta["realized_pnl_ars"]))
        if row["portfolio_value_after"] is not None:
            curve.append((str(row["trade_date"]), float(row["portfolio_value_after"])))
    if not curve or curve[-1][0] != as_of or abs(curve[-1][1] - final_value) > 0.01:
        curve.append((as_of, float(final_value)))
    metrics = build_metrics_dict(curve, trade_pnls, total_traded)
    metrics["total_trades"] = len(rows)
    metrics["closed_trades"] = len(trade_pnls)
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
        SELECT symbol, action, quantity, price, amount_ars, total_cost_ars, cost_model_json
        FROM simulation_trades
        WHERE run_id = ?
        ORDER BY id ASC
        """,
        (run_id,),
    ).fetchall()

    portfolio = SimulatedPortfolio(cash_ars=initial_cash_ars, cost_model=cost_model or ExecutionCostModel())
    cost_basis: Dict[str, float] = {}

    for symbol, action, quantity, price, amount_ars, total_cost_ars, cost_json in trades:
        qty = float(quantity or 0.0)
        if qty <= 0:
            continue
        meta: Dict[str, Any] = {}
        try:
            meta = json.loads(cost_json or "{}")
        except Exception:
            meta = {}
        if action == "buy":
            cash_impact = meta.get("net_cash_impact_ars")
            if cash_impact is None:
                cash_impact = float(amount_ars or 0.0) + float(total_cost_ars or 0.0)
            existing = portfolio.holdings.get(symbol)
            if existing:
                total_qty = existing.quantity + qty
                total_basis = existing.cost_basis + float(cash_impact)
                portfolio.holdings[symbol] = Position(symbol, total_qty, total_basis / total_qty)
            else:
                portfolio.holdings[symbol] = Position(symbol, qty, float(cash_impact) / qty)
            portfolio.cash_ars -= float(cash_impact)
            cost_basis[symbol] = portfolio.holdings[symbol].avg_price
        elif action in ("trim", "exit"):
            cash_impact = meta.get("net_cash_impact_ars")
            if cash_impact is None:
                cash_impact = max(0.0, float(amount_ars or 0.0) - float(total_cost_ars or 0.0))
            pos = portfolio.holdings.get(symbol)
            if pos:
                remaining = pos.quantity - qty
                if remaining <= 0.0001:
                    del portfolio.holdings[symbol]
                    cost_basis.pop(symbol, None)
                else:
                    portfolio.holdings[symbol] = Position(symbol, remaining, pos.avg_price)
                    cost_basis[symbol] = pos.avg_price
            portfolio.cash_ars += float(cash_impact)

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
        log(f"[yellow]No opportunity candidates for {as_of} — pending executions only.[/yellow]")

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

        # Check if we already executed or queued a step today for this run.
        already_today = conn.execute(
            """
            SELECT 1 FROM simulation_trades WHERE run_id=? AND trade_date=?
            UNION ALL
            SELECT 1 FROM simulation_pending_orders WHERE run_id=? AND signal_date=?
            LIMIT 1
            """,
            (run_id, as_of, run_id, as_of),
        ).fetchone()
        if already_today:
            log(f"  [dim]{bot_name}[/dim] - step already processed for {as_of}, skipping.")
            run_ids.append(run_id)
            continue

        pending_rows = conn.execute(
            """
            SELECT id, symbol, side, action, amount_ars, quantity, signal_price,
                   reason, engine_source
            FROM simulation_pending_orders
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
                "reason": row["reason"],
                "engine_source": row["engine_source"],
            }
            for row in pending_rows
        ]

        curve_so_far: EquityCurve = []
        trade_pnls: List[float] = []
        total_traded_ref = [0.0]

        _execute_trading_day(
            conn, run_id, as_of, config, portfolio, curve_so_far,
            trade_pnls, cost_basis, total_traded_ref,
            regime=regime, macro=macro, smart_money=smart_money,
            use_engines=True,
            engine_source="live_engine",
            pending_orders=pending_orders,
        )

        queued_today = 0
        for order in pending_orders:
            if order.get("id") is None:
                _persist_pending_order(conn, run_id, as_of, order)
                queued_today += 1
        conn.commit()

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
            f"positions={portfolio.n_positions}  pending_tomorrow={queued_today}"
        )
        run_ids.append(run_id)

    return run_ids
