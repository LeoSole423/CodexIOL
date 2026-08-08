"""Adapter: translate engine signals → build_candidates() parameter adjustments.

build_candidates() is NEVER modified.  This module converts the outputs of
the three data engines (regime, macro, smart_money) into adjusted `weights`
and `thresholds` dicts that are passed as kwargs into the existing function.

The adapter is purely functional: given signals → return adjusted dicts.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from ..signals import MacroSignal, RegimeSignal, SmartMoneySignal

# Symbols that represent global equity (react to regime/global_risk_on).
_EQUITY_SYMBOLS = {"SPY", "ACWI", "EEM", "QQQ", "IWM", "VTI"}
# Symbols sensitive to Argentina macro stress.
_AR_SOVEREIGN_SYMBOLS = {"TO26", "AL30", "GD30", "AE38"}
# Symbols classified as defensive / alternative.
_DEFENSIVE_SYMBOLS = {"GLD", "IAU", "TLT", "BIL"}


def engine_signals_to_evidence(
    regime: Optional[RegimeSignal],
    macro: Optional[MacroSignal],
    smart_money: Optional[List[SmartMoneySignal]],
    as_of: str,
    portfolio_symbols: List[str],
) -> List[Dict[str, Any]]:
    """Convert engine signals into synthetic evidence rows for build_candidates().

    Generated rows carry source_tier='official' so they are treated as trusted
    references by evidence_stats().  They are NOT persisted to advisor_evidence —
    they are injected ephemerally into the evidence_map for the current run only.

    Each row follows the advisor_evidence schema (same keys used by load_evidence_rows_grouped).
    """
    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    rows: List[Dict[str, Any]] = []

    def _row(symbol: str, claim: str, stance: str, confidence: str, conflict_key: str) -> Dict[str, Any]:
        return {
            "symbol": symbol,
            "query": "engine_synthetic",
            "source_name": "engine_synthetic",
            "source_url": "",
            "published_date": as_of,
            "retrieved_at_utc": now_utc,
            "claim": claim,
            "confidence": confidence,
            "date_confidence": "high",
            "notes": json.dumps({"source_tier": "official", "stance": stance}, ensure_ascii=True),
            "conflict_key": conflict_key,
        }

    upper_symbols = [s.upper() for s in portfolio_symbols]

    # ── Regime signal → equity symbols ──────────────────────────────────────
    if regime is not None:
        r = regime.regime.lower()
        if r == "bull":
            stance, conf, claim_tmpl = "bullish", "medium", "Regime engine: bull market ({score:.0f}/100)"
        elif r == "bear":
            stance, conf, claim_tmpl = "bearish", "high", "Regime engine: bear market ({score:.0f}/100)"
        elif r == "crisis":
            stance, conf, claim_tmpl = "bearish", "high", "Regime engine: crisis regime ({score:.0f}/100)"
        else:  # sideways
            stance, conf, claim_tmpl = "neutral", "low", "Regime engine: sideways ({score:.0f}/100)"

        claim = claim_tmpl.format(score=float(regime.regime_score))
        for sym in upper_symbols:
            if sym in _EQUITY_SYMBOLS:
                rows.append(_row(sym, claim, stance, conf, f"{sym}:regime"))

        # Defensive symbols: inverse regime (bear/crisis → bullish for GLD)
        if r in ("bear", "crisis"):
            def_claim = f"Regime engine: defensive assets favored in {r} ({regime.regime_score:.0f}/100)"
            for sym in upper_symbols:
                if sym in _DEFENSIVE_SYMBOLS:
                    rows.append(_row(sym, def_claim, "bullish", "high", f"{sym}:regime"))

    # ── Macro signal ────────────────────────────────────────────────────────
    if macro is not None:
        # Global risk-on → equity boost
        global_risk = float(macro.global_risk_on or 50.0)
        if global_risk >= 65.0:
            claim = f"Macro engine: global risk-on at {global_risk:.0f}/100"
            for sym in upper_symbols:
                if sym in _EQUITY_SYMBOLS:
                    rows.append(_row(sym, claim, "bullish", "medium", f"{sym}:macro_global"))
        elif global_risk <= 35.0:
            claim = f"Macro engine: global risk-off at {global_risk:.0f}/100"
            for sym in upper_symbols:
                if sym in _EQUITY_SYMBOLS:
                    rows.append(_row(sym, claim, "bearish", "medium", f"{sym}:macro_global"))

        # Argentina macro stress → sovereign bonds
        ar_stress = float(macro.argentina_macro_stress or 50.0)
        if ar_stress >= 70.0:
            claim = f"Macro engine: Argentina macro stress high ({ar_stress:.0f}/100)"
            for sym in upper_symbols:
                if sym in _AR_SOVEREIGN_SYMBOLS:
                    rows.append(_row(sym, claim, "bearish", "medium", f"{sym}:macro_ar"))
        elif ar_stress <= 30.0:
            claim = f"Macro engine: Argentina macro stress low ({ar_stress:.0f}/100)"
            for sym in upper_symbols:
                if sym in _AR_SOVEREIGN_SYMBOLS:
                    rows.append(_row(sym, claim, "bullish", "medium", f"{sym}:macro_ar"))

    # ── Smart money signal → per-symbol ─────────────────────────────────────
    if smart_money:
        sm_by_symbol = {sig.symbol.upper(): sig for sig in smart_money}
        for sym in upper_symbols:
            sig = sm_by_symbol.get(sym)
            if sig is None:
                continue
            conv = float(sig.conviction_score or 0.0)
            if conv < 60.0:
                continue
            direction = str(sig.net_institutional_direction or "").lower()
            if direction == "accumulate":
                claim = f"Smart money engine: institutional accumulation of {sym} (conviction={conv:.0f})"
                rows.append(_row(sym, claim, "bullish", "high", f"{sym}:institutional"))
            elif direction == "distribute":
                claim = f"Smart money engine: institutional distribution of {sym} (conviction={conv:.0f})"
                rows.append(_row(sym, claim, "bearish", "high", f"{sym}:institutional"))

    return rows


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
