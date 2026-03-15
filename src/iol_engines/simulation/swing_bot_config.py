"""Swing trading bot configuration presets.

Each config defines entry/exit rules and risk parameters for a multi-day
hold strategy (3-10 days).
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List


@dataclass
class SwingBotConfig:
    name: str
    description: str

    # Hold period
    min_hold_days: int    # Don't exit before this many days (unless stop hit)
    max_hold_days: int    # Time stop — force exit after this many days

    # Exit thresholds (as fractions of entry price)
    stop_loss_pct: float        # e.g. 0.03 = exit if down 3%
    take_profit_pct: float      # e.g. 0.08 = exit if up 8%
    trailing_atr_mult: float    # trailing stop = peak_price - ATR × mult

    # Portfolio constraints
    max_positions: int
    cash_reserve_pct: float     # Minimum cash to keep (e.g. 0.05 = keep 5%)
    position_size_pct: float    # Max % of portfolio per position

    # Entry signal thresholds
    min_engine_score: float     # Opportunity score (0-100)
    min_regime_score: float     # Regime score (0-100); higher = more bullish
    max_macro_stress: float     # Argentina macro stress (0-100); lower = safer

    # Exit signal
    exit_score_threshold: float   # If engine score drops below this, exit
    rsi_overbought: float = 78.0  # Exit if RSI exceeds this


# ── Presets ───────────────────────────────────────────────────────────────────

SWING_BOT_PRESETS: Dict[str, SwingBotConfig] = {
    "swing-conservative": SwingBotConfig(
        name="swing-conservative",
        description="Patient swing trader: 5-10 day holds, tight stops, high conviction only",
        min_hold_days=5,
        max_hold_days=10,
        stop_loss_pct=0.03,
        take_profit_pct=0.08,
        trailing_atr_mult=1.5,
        max_positions=4,
        cash_reserve_pct=0.10,
        position_size_pct=0.20,
        min_engine_score=60.0,
        min_regime_score=55.0,
        max_macro_stress=60.0,
        exit_score_threshold=35.0,
        rsi_overbought=78.0,
    ),
    "swing-balanced": SwingBotConfig(
        name="swing-balanced",
        description="Balanced swing trader: 3-7 day holds, moderate risk/reward",
        min_hold_days=3,
        max_hold_days=7,
        stop_loss_pct=0.04,
        take_profit_pct=0.10,
        trailing_atr_mult=2.0,
        max_positions=6,
        cash_reserve_pct=0.05,
        position_size_pct=0.15,
        min_engine_score=50.0,
        min_regime_score=45.0,
        max_macro_stress=70.0,
        exit_score_threshold=28.0,
        rsi_overbought=80.0,
    ),
    "swing-aggressive": SwingBotConfig(
        name="swing-aggressive",
        description="Aggressive swing trader: 3-5 day holds, wide stops, high upside targets",
        min_hold_days=3,
        max_hold_days=5,
        stop_loss_pct=0.05,
        take_profit_pct=0.15,
        trailing_atr_mult=2.5,
        max_positions=8,
        cash_reserve_pct=0.03,
        position_size_pct=0.12,
        min_engine_score=40.0,
        min_regime_score=35.0,
        max_macro_stress=80.0,
        exit_score_threshold=20.0,
        rsi_overbought=82.0,
    ),
}


def get_swing_preset(name: str) -> SwingBotConfig:
    if name not in SWING_BOT_PRESETS:
        raise ValueError(
            f"Unknown swing bot: '{name}'. Available: {list(SWING_BOT_PRESETS)}"
        )
    return SWING_BOT_PRESETS[name]


def list_swing_presets() -> List[SwingBotConfig]:
    return list(SWING_BOT_PRESETS.values())
