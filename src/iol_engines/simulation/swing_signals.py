"""Swing trading signal classification.

Combines engine signals (regime, macro, opportunity score) with TA indicators
to produce entry/hold/exit decisions for each symbol.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, Optional

from .swing_bot_config import SwingBotConfig
from .swing_indicators import SwingTA


@dataclass
class SwingSignal:
    action: Literal["entry", "hold", "exit", "no_signal"]
    reason: str
    conviction: float           # 0-100; higher = stronger signal
    suggested_stop_pct: float   # Fraction of entry price for stop-loss
    suggested_target_pct: float # Fraction of entry price for take-profit


@dataclass
class OpenPosition:
    """State tracked for an open swing position."""
    symbol: str
    entry_price: float
    entry_date: str
    days_held: int
    peak_price: float   # Highest price seen since entry (for trailing stop)
    engine_score: float


def _entry_conviction(
    engine_score: float,
    regime_score: float,
    macro_stress: float,
    ta: SwingTA,
    config: SwingBotConfig,
) -> float:
    """Compute a 0-100 conviction score for an entry signal."""
    score = 0.0
    # Engine opportunity score contributes 40%
    score += (engine_score / 100.0) * 40.0
    # Regime score contributes 30%
    score += (regime_score / 100.0) * 30.0
    # Inverse macro stress contributes 20% (lower stress = better)
    score += ((100.0 - macro_stress) / 100.0) * 20.0
    # TA momentum contributes 10%
    if ta.macd_bullish:
        score += 5.0
    if ta.price_above_ma20:
        score += 3.0
    if ta.rsi_14 is not None and 40.0 <= ta.rsi_14 <= 60.0:
        score += 2.0  # RSI in sweet spot = bonus
    return min(score, 100.0)


def classify_swing_signal(
    ta: SwingTA,
    engine_score: float,
    regime_score: float,
    macro_stress: float,
    position: Optional[OpenPosition],
    config: SwingBotConfig,
) -> SwingSignal:
    """Classify whether to enter, hold, or exit a position for a symbol.

    Args:
        ta: Technical analysis indicators for the symbol.
        engine_score: Opportunity score from advisor engines (0-100).
        regime_score: Current regime score (0-100; higher = bullish).
        macro_stress: Argentina macro stress level (0-100; higher = risky).
        position: Open position state, or None if not holding this symbol.
        config: Bot configuration with thresholds.

    Returns:
        SwingSignal with action and reason.
    """
    atr_pct = (ta.atr_14 / ta.last_price) if (ta.atr_14 and ta.last_price > 0) else config.stop_loss_pct

    # ── EXIT logic (checked first when holding) ───────────────────────────────
    if position is not None:
        days = position.days_held

        # 1. Stop-loss
        if ta.last_price <= position.entry_price * (1.0 - config.stop_loss_pct):
            return SwingSignal(
                action="exit",
                reason=f"stop_loss (price {ta.last_price:.2f} ≤ stop {position.entry_price * (1 - config.stop_loss_pct):.2f})",
                conviction=0.0,
                suggested_stop_pct=config.stop_loss_pct,
                suggested_target_pct=config.take_profit_pct,
            )

        # 2. Take-profit
        if ta.last_price >= position.entry_price * (1.0 + config.take_profit_pct):
            return SwingSignal(
                action="exit",
                reason=f"take_profit (price {ta.last_price:.2f} ≥ target {position.entry_price * (1 + config.take_profit_pct):.2f})",
                conviction=100.0,
                suggested_stop_pct=config.stop_loss_pct,
                suggested_target_pct=config.take_profit_pct,
            )

        # 3. Trailing stop (only after min hold period)
        if days >= config.min_hold_days and ta.atr_14 is not None:
            trailing_stop = position.peak_price - (ta.atr_14 * config.trailing_atr_mult)
            if ta.last_price <= trailing_stop:
                return SwingSignal(
                    action="exit",
                    reason=f"trailing_stop (price {ta.last_price:.2f} ≤ trailing {trailing_stop:.2f}, peak {position.peak_price:.2f})",
                    conviction=50.0,
                    suggested_stop_pct=config.stop_loss_pct,
                    suggested_target_pct=config.take_profit_pct,
                )

        # 4. Time stop
        if days >= config.max_hold_days:
            return SwingSignal(
                action="exit",
                reason=f"time_stop ({days} days ≥ max {config.max_hold_days})",
                conviction=50.0,
                suggested_stop_pct=config.stop_loss_pct,
                suggested_target_pct=config.take_profit_pct,
            )

        # 5. Signal deterioration
        if engine_score < config.exit_score_threshold:
            return SwingSignal(
                action="exit",
                reason=f"signal_exit (score {engine_score:.1f} < threshold {config.exit_score_threshold})",
                conviction=30.0,
                suggested_stop_pct=config.stop_loss_pct,
                suggested_target_pct=config.take_profit_pct,
            )

        # 6. RSI overbought (only after min hold)
        if days >= config.min_hold_days and ta.rsi_14 is not None and ta.rsi_14 > config.rsi_overbought:
            return SwingSignal(
                action="exit",
                reason=f"rsi_overbought (RSI {ta.rsi_14:.1f} > {config.rsi_overbought})",
                conviction=60.0,
                suggested_stop_pct=config.stop_loss_pct,
                suggested_target_pct=config.take_profit_pct,
            )

        # Still holding — no exit trigger
        return SwingSignal(
            action="hold",
            reason=f"holding day {days}/{config.max_hold_days}",
            conviction=50.0,
            suggested_stop_pct=config.stop_loss_pct,
            suggested_target_pct=config.take_profit_pct,
        )

    # ── ENTRY logic (only when not holding this symbol) ───────────────────────

    # Hard filters — all must pass
    if engine_score < config.min_engine_score:
        return SwingSignal(
            action="no_signal",
            reason=f"engine_score too low ({engine_score:.1f} < {config.min_engine_score})",
            conviction=0.0,
            suggested_stop_pct=atr_pct,
            suggested_target_pct=config.take_profit_pct,
        )

    if regime_score < config.min_regime_score:
        return SwingSignal(
            action="no_signal",
            reason=f"regime_score too low ({regime_score:.1f} < {config.min_regime_score})",
            conviction=0.0,
            suggested_stop_pct=atr_pct,
            suggested_target_pct=config.take_profit_pct,
        )

    if macro_stress > config.max_macro_stress:
        return SwingSignal(
            action="no_signal",
            reason=f"macro_stress too high ({macro_stress:.1f} > {config.max_macro_stress})",
            conviction=0.0,
            suggested_stop_pct=atr_pct,
            suggested_target_pct=config.take_profit_pct,
        )

    if ta.rsi_14 is None or ta.rsi_14 < 30.0 or ta.rsi_14 > 65.0:
        rsi_str = f"{ta.rsi_14:.1f}" if ta.rsi_14 is not None else "N/A"
        return SwingSignal(
            action="no_signal",
            reason=f"RSI out of entry range ({rsi_str}; want 30-65)",
            conviction=0.0,
            suggested_stop_pct=atr_pct,
            suggested_target_pct=config.take_profit_pct,
        )

    if ta.price_above_ma20 is False:
        return SwingSignal(
            action="no_signal",
            reason="price below MA20 (no uptrend)",
            conviction=0.0,
            suggested_stop_pct=atr_pct,
            suggested_target_pct=config.take_profit_pct,
        )

    if ta.macd_bullish is False:
        return SwingSignal(
            action="no_signal",
            reason="MACD histogram negative (no momentum)",
            conviction=0.0,
            suggested_stop_pct=atr_pct,
            suggested_target_pct=config.take_profit_pct,
        )

    conviction = _entry_conviction(engine_score, regime_score, macro_stress, ta, config)

    return SwingSignal(
        action="entry",
        reason=(
            f"score={engine_score:.0f} regime={regime_score:.0f} "
            f"rsi={ta.rsi_14:.1f} macd={'bull' if ta.macd_bullish else 'bear'}"
        ),
        conviction=conviction,
        suggested_stop_pct=atr_pct,
        suggested_target_pct=config.take_profit_pct,
    )
