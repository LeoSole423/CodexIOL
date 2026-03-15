"""Adapter: translate engine signals → build_candidates() parameter adjustments.

build_candidates() is NEVER modified.  This module converts the outputs of
the three data engines (regime, macro, smart_money) into adjusted `weights`
and `thresholds` dicts that are passed as kwargs into the existing function.

The adapter is purely functional: given signals → return adjusted dicts.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from ..signals import MacroSignal, RegimeSignal, SmartMoneySignal


# ── Default build_candidates weights & thresholds ───────────────────────────

DEFAULT_WEIGHTS: Dict[str, float] = {
    "risk": 0.35,
    "value": 0.20,
    "momentum": 0.35,
    "catalyst": 0.10,
}

DEFAULT_THRESHOLDS: Dict[str, Any] = {
    "trim_weight_pct": 12.0,
    "exit_weight_pct": 15.0,
    "concentration_pct_max": 15.0,
    "sell_momentum_max": 35.0,
    "exit_momentum_max": 20.0,
    "drawdown_exclusion_pct": -25.0,
    "rebuy_dip_threshold_pct": -8.0,
    "liquidity_floor": 40.0,
    "sell_conflict_exit": True,
}


def adjust_weights(
    regime: Optional[RegimeSignal],
    macro: Optional[MacroSignal],
) -> Dict[str, float]:
    """Return adjusted scoring weights based on regime and macro signals.

    Rules:
      - bear/crisis regime  → +5% risk weight, -5% momentum weight
      - crisis regime       → additional +5% risk, -5% value
      - sideways regime     → no change (baseline weights)
      - bull regime         → +3% momentum, -3% risk
    """
    w = dict(DEFAULT_WEIGHTS)

    if regime is None:
        return w

    if regime.regime == "crisis":
        w["risk"] = min(0.60, w["risk"] + 0.10)
        w["momentum"] = max(0.10, w["momentum"] - 0.05)
        w["value"] = max(0.10, w["value"] - 0.05)
    elif regime.regime == "bear":
        w["risk"] = min(0.55, w["risk"] + 0.05)
        w["momentum"] = max(0.15, w["momentum"] - 0.05)
    elif regime.regime == "bull":
        w["risk"] = max(0.25, w["risk"] - 0.03)
        w["momentum"] = min(0.45, w["momentum"] + 0.03)

    # Normalise so weights sum to 1.0
    total = sum(w.values())
    if total > 0:
        w = {k: round(v / total, 4) for k, v in w.items()}

    return w


def adjust_thresholds(
    regime: Optional[RegimeSignal],
    macro: Optional[MacroSignal],
) -> Dict[str, Any]:
    """Return adjusted scoring thresholds based on regime and macro signals.

    Rules:
      - Argentina macro stress > 70 → tighter concentration cap (10% vs 15%)
      - bear/crisis regime → lower trim trigger (10% weight vs 12%)
      - crisis regime → lower exit trigger (12% vs 15%)
    """
    t = dict(DEFAULT_THRESHOLDS)

    if macro is not None and macro.argentina_macro_stress > 70:
        t["concentration_pct_max"] = 10.0  # tighter cap in stressed AR market

    if regime is not None:
        if regime.regime in ("bear", "crisis"):
            t["trim_weight_pct"] = 10.0   # trim earlier
        if regime.regime == "crisis":
            t["exit_weight_pct"] = 12.0   # exit earlier

    return t


def build_catalyst_overrides(
    smart_money: Optional[List[SmartMoneySignal]],
) -> Dict[str, float]:
    """Return per-symbol catalyst score overrides from institutional signals.

    Symbols with high smart-money conviction get a +10 catalyst bonus.
    Symbols with strong distribute signals get a -10 malus.

    Returns {symbol: delta} — applied as additive override in the pipeline wrapper.
    """
    if not smart_money:
        return {}

    overrides: Dict[str, float] = {}
    for sig in smart_money:
        if sig.conviction_score >= 70 and sig.net_institutional_direction == "accumulate":
            overrides[sig.symbol] = +10.0
        elif sig.conviction_score >= 70 and sig.net_institutional_direction == "distribute":
            overrides[sig.symbol] = -10.0

    return overrides


def build_adjusted_params(
    regime: Optional[RegimeSignal],
    macro: Optional[MacroSignal],
    smart_money: Optional[List[SmartMoneySignal]],
) -> Dict[str, Any]:
    """Convenience wrapper: returns all adjusted parameters for build_candidates().

    Returns:
        {
          "weights": {...},
          "thresholds": {...},
          "catalyst_overrides": {symbol: delta},
          "provenance": {...}   # human-readable explanation of changes
        }
    """
    weights = adjust_weights(regime, macro)
    thresholds = adjust_thresholds(regime, macro)
    catalyst_overrides = build_catalyst_overrides(smart_money)

    provenance: Dict[str, Any] = {
        "regime": regime.regime if regime else "unknown",
        "ar_stress": macro.argentina_macro_stress if macro else None,
        "weight_delta": {
            k: round(weights[k] - DEFAULT_WEIGHTS.get(k, 0), 4)
            for k in weights
        },
        "threshold_changes": {
            k: v for k, v in thresholds.items()
            if v != DEFAULT_THRESHOLDS.get(k)
        },
        "catalyst_overrides_count": len(catalyst_overrides),
    }

    return {
        "weights": weights,
        "thresholds": thresholds,
        "catalyst_overrides": catalyst_overrides,
        "provenance": provenance,
    }
