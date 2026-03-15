"""Shared signal dataclasses produced by each engine.

All signals are plain frozen dataclasses so they can be easily
serialised to JSON and stored in SQLite.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Dict, List, Optional


# ── Motor 1 ─────────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class RegimeSignal:
    as_of: str
    regime: str                          # "bull" | "bear" | "sideways" | "crisis"
    confidence: float                    # 0-1
    regime_score: float                  # 0-100 (higher = more bullish)
    favored_asset_classes: List[str]     # e.g. ["equity"] or ["gold","bonds","cash"]
    defensive_weight_adjustment: float   # delta applied to equity allocation (negative = reduce)
    breadth_score: float                 # % symbols above MA50 (0-100)
    volatility_regime: str               # "low" | "normal" | "high" | "extreme"
    notes: str = ""

    def to_dict(self) -> Dict:
        return {
            "as_of": self.as_of,
            "regime": self.regime,
            "confidence": self.confidence,
            "regime_score": self.regime_score,
            "favored_asset_classes": self.favored_asset_classes,
            "defensive_weight_adjustment": self.defensive_weight_adjustment,
            "breadth_score": self.breadth_score,
            "volatility_regime": self.volatility_regime,
            "notes": self.notes,
        }


# ── Motor 2 ─────────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class MacroSignal:
    as_of: str
    argentina_macro_stress: float        # 0-100 (higher = worse for local equities)
    global_risk_on: float                # 0-100 (higher = risk appetite)
    # Argentina
    inflation_mom_pct: Optional[float] = None
    bcra_rate_pct: Optional[float] = None
    usd_ars_official: Optional[float] = None
    usd_ars_blue: Optional[float] = None
    cedear_fx_premium_pct: Optional[float] = None
    # Global
    fed_rate_pct: Optional[float] = None
    us_cpi_yoy_pct: Optional[float] = None
    # Composite
    sentiment_score: Optional[float] = None   # -100 to +100
    upcoming_events: List[Dict] = field(default_factory=list)

    def to_dict(self) -> Dict:
        return {
            "as_of": self.as_of,
            "argentina_macro_stress": self.argentina_macro_stress,
            "global_risk_on": self.global_risk_on,
            "inflation_mom_pct": self.inflation_mom_pct,
            "bcra_rate_pct": self.bcra_rate_pct,
            "usd_ars_official": self.usd_ars_official,
            "usd_ars_blue": self.usd_ars_blue,
            "cedear_fx_premium_pct": self.cedear_fx_premium_pct,
            "fed_rate_pct": self.fed_rate_pct,
            "us_cpi_yoy_pct": self.us_cpi_yoy_pct,
            "sentiment_score": self.sentiment_score,
            "upcoming_events": self.upcoming_events,
        }


# ── Motor 3 ─────────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class SmartMoneySignal:
    as_of: str
    symbol: str
    net_institutional_direction: str     # "accumulate" | "distribute" | "neutral"
    conviction_score: float              # 0-100
    top_holders_added: List[str] = field(default_factory=list)
    top_holders_trimmed: List[str] = field(default_factory=list)
    latest_13f_date: Optional[str] = None
    notes: str = ""

    def to_dict(self) -> Dict:
        return {
            "as_of": self.as_of,
            "symbol": self.symbol,
            "net_institutional_direction": self.net_institutional_direction,
            "conviction_score": self.conviction_score,
            "top_holders_added": self.top_holders_added,
            "top_holders_trimmed": self.top_holders_trimmed,
            "latest_13f_date": self.latest_13f_date,
            "notes": self.notes,
        }


# ── Motor 5 ─────────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class PositionAction:
    symbol: str
    action: str                  # "buy" | "sell" | "trim" | "hold" | "watch"
    amount_ars: float
    weight_pct: float
    reason: str
    engine_source: str
    candidate_score: float

    def to_dict(self) -> Dict:
        return {
            "symbol": self.symbol,
            "action": self.action,
            "amount_ars": self.amount_ars,
            "weight_pct": self.weight_pct,
            "reason": self.reason,
            "engine_source": self.engine_source,
            "candidate_score": self.candidate_score,
        }


@dataclass(frozen=True)
class StrategyActionPlan:
    as_of: str
    portfolio_cash_ars: float
    portfolio_cash_usd: float
    actions: List[PositionAction]
    total_deployed_ars: float
    defensive_overlay_applied: bool
    regime: str
    notes: str = ""

    def to_dict(self) -> Dict:
        return {
            "as_of": self.as_of,
            "portfolio_cash_ars": self.portfolio_cash_ars,
            "portfolio_cash_usd": self.portfolio_cash_usd,
            "actions": [a.to_dict() for a in self.actions],
            "total_deployed_ars": self.total_deployed_ars,
            "defensive_overlay_applied": self.defensive_overlay_applied,
            "regime": self.regime,
            "notes": self.notes,
        }
