"""BotConfig — named trading bot configurations for backtesting.

Three built-in presets:
  conservative  — high risk-weight, full regime influence, tight position caps
  balanced      — default weights, moderate regime influence
  growth        — high momentum-weight, low regime influence, larger positions
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Dict, List, Optional

PRESET_NAMES = ("conservative", "balanced", "growth")


@dataclass(frozen=True)
class BotConfig:
    name: str
    description: str
    # Scoring weights (must sum to 1.0)
    weights: Dict[str, float]
    # 0-1: how strongly regime signal adjusts weights (0 = ignore, 1 = full apply)
    regime_influence: float
    # Max fraction of portfolio value per position (e.g. 0.10 = 10%)
    max_position_pct: float
    # Minimum cash fraction to hold in reserve
    cash_reserve_pct: float
    # Min candidate score_total required to act
    min_score_threshold: float
    # Max number of positions to hold simultaneously
    max_positions: int

    def to_dict(self) -> Dict:
        return {
            "name": self.name,
            "description": self.description,
            "weights": self.weights,
            "regime_influence": self.regime_influence,
            "max_position_pct": self.max_position_pct,
            "cash_reserve_pct": self.cash_reserve_pct,
            "min_score_threshold": self.min_score_threshold,
            "max_positions": self.max_positions,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict())

    @classmethod
    def from_dict(cls, d: Dict) -> "BotConfig":
        return cls(
            name=d["name"],
            description=d.get("description", ""),
            weights=d["weights"],
            regime_influence=float(d.get("regime_influence", 0.7)),
            max_position_pct=float(d.get("max_position_pct", 0.15)),
            cash_reserve_pct=float(d.get("cash_reserve_pct", 0.05)),
            min_score_threshold=float(d.get("min_score_threshold", 0.0)),
            max_positions=int(d.get("max_positions", 15)),
        )

    @classmethod
    def from_json(cls, s: str) -> "BotConfig":
        return cls.from_dict(json.loads(s))


# ── Built-in presets ──────────────────────────────────────────────────────────

PRESETS: Dict[str, BotConfig] = {
    "conservative": BotConfig(
        name="conservative",
        description=(
            "Capital preservation focus. High risk-weight, full regime influence, "
            "tight position caps and large cash reserve."
        ),
        weights={"risk": 0.45, "value": 0.20, "momentum": 0.25, "catalyst": 0.10},
        regime_influence=1.0,
        max_position_pct=0.10,
        cash_reserve_pct=0.10,
        min_score_threshold=55.0,
        max_positions=10,
    ),
    "balanced": BotConfig(
        name="balanced",
        description=(
            "Default balanced configuration. Equal risk/momentum emphasis, "
            "moderate regime influence."
        ),
        weights={"risk": 0.35, "value": 0.20, "momentum": 0.35, "catalyst": 0.10},
        regime_influence=0.7,
        max_position_pct=0.15,
        cash_reserve_pct=0.05,
        min_score_threshold=45.0,
        max_positions=15,
    ),
    "growth": BotConfig(
        name="growth",
        description=(
            "Aggressive growth. High momentum-weight, low regime influence, "
            "larger position sizes."
        ),
        weights={"risk": 0.25, "value": 0.15, "momentum": 0.45, "catalyst": 0.15},
        regime_influence=0.4,
        max_position_pct=0.20,
        cash_reserve_pct=0.03,
        min_score_threshold=35.0,
        max_positions=20,
    ),
}


def get_preset(name: str) -> BotConfig:
    if name not in PRESETS:
        raise ValueError(f"Unknown bot config preset '{name}'. Valid: {list(PRESETS)}")
    return PRESETS[name]
