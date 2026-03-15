"""Event-driven trading bot configuration.

These bots react to discrete market events (regime changes, macro spikes,
institutional activity) by adjusting portfolio exposure.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Literal, Optional


Reaction = Literal[
    "buy_top_candidates",  # Buy top N opportunity-scored symbols
    "trim_all",            # Trim all positions by magnitude_pct
    "exit_all",            # Fully exit all positions
    "buy_symbol",          # Buy a specific symbol (for smart money events)
    "sell_symbol",         # Sell a specific symbol
    "increase_cash",       # Liquidate positions to reach target cash %
]


@dataclass
class EventReactionRule:
    event_type: str
    reaction: Reaction
    magnitude_pct: float      # % of portfolio to move (for buy/trim)
    top_n: int = 3            # For buy_top_candidates: number of symbols to buy
    symbol: Optional[str] = None  # For buy_symbol / sell_symbol (smart money events)
    target_cash_pct: float = 0.0  # For increase_cash reaction


@dataclass
class EventBotConfig:
    name: str
    description: str
    reaction_rules: List[EventReactionRule]
    max_positions: int
    cash_reserve_pct: float
    position_size_pct: float
    min_engine_score: float
    hold_after_event_days: int  # Cooldown: don't react to new events for N days after last event


# ── Presets ───────────────────────────────────────────────────────────────────

_EVENT_BOTS: Dict[str, EventBotConfig] = {
    "event-defensive": EventBotConfig(
        name="event-defensive",
        description=(
            "Defensive event bot: reduces exposure on negative signals, "
            "cautiously buys on positive signals. 5-day event cooldown."
        ),
        reaction_rules=[
            # Negative events → reduce exposure
            EventReactionRule("regime_change", "trim_all", magnitude_pct=0.30, top_n=0),
            EventReactionRule("volatility_spike", "increase_cash", magnitude_pct=0.0, target_cash_pct=0.50),
            EventReactionRule("macro_stress_high", "increase_cash", magnitude_pct=0.0, target_cash_pct=0.40),
            EventReactionRule("risk_off", "trim_all", magnitude_pct=0.25),
            EventReactionRule("smart_money_distribute", "sell_symbol", magnitude_pct=1.0),
            # Positive events → small buys
            EventReactionRule("macro_stress_low", "buy_top_candidates", magnitude_pct=0.10, top_n=2),
            EventReactionRule("risk_on", "buy_top_candidates", magnitude_pct=0.10, top_n=2),
            EventReactionRule("smart_money_accumulate", "buy_symbol", magnitude_pct=0.08),
            EventReactionRule("volatility_calm", "buy_top_candidates", magnitude_pct=0.08, top_n=2),
        ],
        max_positions=4,
        cash_reserve_pct=0.20,
        position_size_pct=0.15,
        min_engine_score=55.0,
        hold_after_event_days=5,
    ),
    "event-opportunistic": EventBotConfig(
        name="event-opportunistic",
        description=(
            "Opportunistic event bot: aggressively buys on positive signals, "
            "ignores moderate negative signals. 3-day event cooldown."
        ),
        reaction_rules=[
            # Aggressive positive reactions
            EventReactionRule("risk_on", "buy_top_candidates", magnitude_pct=0.25, top_n=4),
            EventReactionRule("macro_stress_low", "buy_top_candidates", magnitude_pct=0.20, top_n=3),
            EventReactionRule("smart_money_accumulate", "buy_symbol", magnitude_pct=0.15),
            EventReactionRule("volatility_calm", "buy_top_candidates", magnitude_pct=0.15, top_n=3),
            # Only react to extreme negative events
            EventReactionRule("volatility_spike", "trim_all", magnitude_pct=0.20),
            EventReactionRule("smart_money_distribute", "sell_symbol", magnitude_pct=0.80),
        ],
        max_positions=8,
        cash_reserve_pct=0.05,
        position_size_pct=0.12,
        min_engine_score=40.0,
        hold_after_event_days=3,
    ),
    "event-adaptive": EventBotConfig(
        name="event-adaptive",
        description=(
            "Adaptive event bot: balanced response to all events, "
            "gradual adjustments. 2-day event cooldown."
        ),
        reaction_rules=[
            # Balanced reactions to all events
            EventReactionRule("regime_change", "trim_all", magnitude_pct=0.15),
            EventReactionRule("volatility_spike", "increase_cash", magnitude_pct=0.0, target_cash_pct=0.35),
            EventReactionRule("volatility_calm", "buy_top_candidates", magnitude_pct=0.12, top_n=2),
            EventReactionRule("macro_stress_high", "trim_all", magnitude_pct=0.20),
            EventReactionRule("macro_stress_low", "buy_top_candidates", magnitude_pct=0.15, top_n=3),
            EventReactionRule("risk_off", "trim_all", magnitude_pct=0.15),
            EventReactionRule("risk_on", "buy_top_candidates", magnitude_pct=0.15, top_n=3),
            EventReactionRule("smart_money_accumulate", "buy_symbol", magnitude_pct=0.10),
            EventReactionRule("smart_money_distribute", "sell_symbol", magnitude_pct=0.90),
        ],
        max_positions=6,
        cash_reserve_pct=0.10,
        position_size_pct=0.14,
        min_engine_score=48.0,
        hold_after_event_days=2,
    ),
}


def get_event_preset(name: str) -> EventBotConfig:
    if name not in _EVENT_BOTS:
        raise ValueError(f"Unknown event bot: '{name}'. Available: {list(_EVENT_BOTS)}")
    return _EVENT_BOTS[name]


def list_event_presets() -> List[EventBotConfig]:
    return list(_EVENT_BOTS.values())
