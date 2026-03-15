"""Detects discrete market events by comparing consecutive engine snapshots.

Events are detected by loading the two most recent snapshots of each engine
and comparing key fields for threshold breaches or state transitions.
No IOL API calls — purely DB-driven.
"""
from __future__ import annotations

import sqlite3
from dataclasses import dataclass, field
from typing import Any, Dict, List, Literal, Optional


Severity = Literal["critical", "high", "medium"]

EVENT_TYPE = Literal[
    "regime_change",
    "volatility_spike",
    "volatility_calm",
    "macro_stress_high",
    "macro_stress_low",
    "risk_off",
    "risk_on",
    "smart_money_accumulate",
    "smart_money_distribute",
]


@dataclass
class EngineEvent:
    event_type: str
    severity: Severity
    symbol: Optional[str]     # None for market-wide events
    description: str
    payload: Dict[str, Any] = field(default_factory=dict)


# ── Regime events ─────────────────────────────────────────────────────────────

def detect_regime_events(prev: Any, curr: Any) -> List[EngineEvent]:
    """Compare two regime snapshots and return detected events."""
    events: List[EngineEvent] = []
    if prev is None or curr is None:
        return events

    # Regime state change
    if prev.regime != curr.regime:
        severity: Severity = "critical" if "crisis" in (prev.regime, curr.regime) else "high"
        events.append(EngineEvent(
            event_type="regime_change",
            severity=severity,
            symbol=None,
            description=f"Market regime changed: {prev.regime} -> {curr.regime}",
            payload={
                "prev_regime": prev.regime,
                "curr_regime": curr.regime,
                "curr_score": getattr(curr, "regime_score", None),
            },
        ))

    # Volatility regime spike
    _VOL_ORDER = {"low": 0, "normal": 1, "high": 2, "extreme": 3}
    prev_vol = getattr(prev, "volatility_regime", "normal") or "normal"
    curr_vol = getattr(curr, "volatility_regime", "normal") or "normal"
    prev_vol_rank = _VOL_ORDER.get(prev_vol, 1)
    curr_vol_rank = _VOL_ORDER.get(curr_vol, 1)

    if curr_vol_rank >= 3 and prev_vol_rank < 3:
        events.append(EngineEvent(
            event_type="volatility_spike",
            severity="critical",
            symbol=None,
            description=f"Volatility spiked to {curr_vol} from {prev_vol}",
            payload={"prev_vol": prev_vol, "curr_vol": curr_vol},
        ))
    elif curr_vol_rank <= 1 and prev_vol_rank >= 2:
        events.append(EngineEvent(
            event_type="volatility_calm",
            severity="medium",
            symbol=None,
            description=f"Volatility calmed to {curr_vol} from {prev_vol}",
            payload={"prev_vol": prev_vol, "curr_vol": curr_vol},
        ))

    return events


# ── Macro events ─────────────────────────────────────────────────────────────

def detect_macro_events(prev: Any, curr: Any) -> List[EngineEvent]:
    """Compare two macro snapshots and return detected events."""
    events: List[EngineEvent] = []
    if prev is None or curr is None:
        return events

    prev_stress = float(getattr(prev, "argentina_macro_stress", 50) or 50)
    curr_stress = float(getattr(curr, "argentina_macro_stress", 50) or 50)
    prev_risk_on = float(getattr(prev, "global_risk_on", 50) or 50)
    curr_risk_on = float(getattr(curr, "global_risk_on", 50) or 50)

    # Macro stress high — crossed above 70
    if curr_stress >= 70.0 and prev_stress < 70.0:
        severity: Severity = "critical" if curr_stress >= 85.0 else "high"
        events.append(EngineEvent(
            event_type="macro_stress_high",
            severity=severity,
            symbol=None,
            description=f"Argentina macro stress spiked to {curr_stress:.0f} (was {prev_stress:.0f})",
            payload={"prev_stress": prev_stress, "curr_stress": curr_stress},
        ))

    # Macro stress low — crossed below 30 (opportunity signal)
    if curr_stress <= 30.0 and prev_stress > 30.0:
        events.append(EngineEvent(
            event_type="macro_stress_low",
            severity="medium",
            symbol=None,
            description=f"Argentina macro stress dropped to {curr_stress:.0f} — potential opportunity",
            payload={"prev_stress": prev_stress, "curr_stress": curr_stress},
        ))

    # Global risk-off — crossed below 30
    if curr_risk_on <= 30.0 and prev_risk_on > 30.0:
        events.append(EngineEvent(
            event_type="risk_off",
            severity="high",
            symbol=None,
            description=f"Global risk appetite dropped to {curr_risk_on:.0f} (was {prev_risk_on:.0f})",
            payload={"prev_risk_on": prev_risk_on, "curr_risk_on": curr_risk_on},
        ))

    # Global risk-on — crossed above 70
    if curr_risk_on >= 70.0 and prev_risk_on < 70.0:
        events.append(EngineEvent(
            event_type="risk_on",
            severity="medium",
            symbol=None,
            description=f"Global risk appetite rose to {curr_risk_on:.0f} (was {prev_risk_on:.0f})",
            payload={"prev_risk_on": prev_risk_on, "curr_risk_on": curr_risk_on},
        ))

    return events


# ── Smart money events ────────────────────────────────────────────────────────

def detect_smart_money_events(
    prev_signals: List[Any],
    curr_signals: List[Any],
) -> List[EngineEvent]:
    """Compare smart money signal lists and return direction-flip events."""
    events: List[EngineEvent] = []

    prev_by_symbol = {s.symbol: s for s in prev_signals if s}
    curr_by_symbol = {s.symbol: s for s in curr_signals if s}

    for symbol, curr in curr_by_symbol.items():
        conviction = float(getattr(curr, "conviction_score", 0) or 0)
        if conviction < 60.0:
            continue  # Only strong signals

        direction = getattr(curr, "net_institutional_direction", "neutral") or "neutral"
        prev = prev_by_symbol.get(symbol)
        prev_direction = getattr(prev, "net_institutional_direction", "neutral") or "neutral"

        if direction == "accumulate" and prev_direction != "accumulate":
            events.append(EngineEvent(
                event_type="smart_money_accumulate",
                severity="high",
                symbol=symbol,
                description=(
                    f"Institutional accumulation detected for {symbol} "
                    f"(conviction {conviction:.0f})"
                ),
                payload={
                    "symbol": symbol,
                    "prev_direction": prev_direction,
                    "curr_direction": direction,
                    "conviction": conviction,
                },
            ))
        elif direction == "distribute" and prev_direction != "distribute":
            severity: Severity = "high" if conviction >= 75.0 else "medium"
            events.append(EngineEvent(
                event_type="smart_money_distribute",
                severity=severity,
                symbol=symbol,
                description=(
                    f"Institutional distribution detected for {symbol} "
                    f"(conviction {conviction:.0f})"
                ),
                payload={
                    "symbol": symbol,
                    "prev_direction": prev_direction,
                    "curr_direction": direction,
                    "conviction": conviction,
                },
            ))

    return events


# ── Main entry point ──────────────────────────────────────────────────────────

def detect_all_events(conn: sqlite3.Connection, as_of: str) -> List[EngineEvent]:
    """Load the two most recent snapshots of each engine and detect events.

    Returns a list of EngineEvents sorted by severity (critical first).
    """
    from iol_engines.macro.engine import MacroMomentumEngine
    from iol_engines.regime.engine import MarketRegimeEngine
    from iol_engines.smart_money.engine import SmartMoneyEngine

    events: List[EngineEvent] = []

    # Regime: load 2 most recent snapshots
    cur_regime = MarketRegimeEngine().load_latest(conn, as_of)
    prev_regime = _load_second_latest_regime(conn, as_of)
    events.extend(detect_regime_events(prev_regime, cur_regime))

    # Macro: load 2 most recent snapshots
    cur_macro = MacroMomentumEngine().load_latest(conn, as_of)
    prev_macro = _load_second_latest_macro(conn, as_of)
    events.extend(detect_macro_events(prev_macro, cur_macro))

    # Smart money: load current and previous
    cur_sm = SmartMoneyEngine().load_latest(conn, as_of) or []
    prev_sm = _load_prev_smart_money(conn, as_of)
    events.extend(detect_smart_money_events(prev_sm, cur_sm))

    # Sort: critical first, then high, then medium
    _severity_order = {"critical": 0, "high": 1, "medium": 2}
    events.sort(key=lambda e: _severity_order.get(e.severity, 3))
    return events


# ── Snapshot loading helpers ──────────────────────────────────────────────────

def _load_second_latest_regime(conn: sqlite3.Connection, as_of: str) -> Any:
    """Load the second-most-recent regime snapshot (the one before as_of)."""
    from iol_engines.regime.engine import RegimeSignal

    cur = conn.cursor()
    cur.execute(
        """
        SELECT as_of, regime, regime_score, confidence, volatility_regime,
               breadth_score, defensive_weight_adjustment, favored_asset_classes_json
        FROM engine_regime_snapshots
        WHERE as_of < ?
        ORDER BY as_of DESC LIMIT 1
        """,
        (as_of,),
    )
    row = cur.fetchone()
    if not row:
        return None
    try:
        import json
        return RegimeSignal(
            as_of=row[0],
            regime=row[1],
            regime_score=row[2],
            confidence=row[3],
            volatility_regime=row[4],
            breadth_score=row[5],
            defensive_weight_adjustment=row[6],
            favored_asset_classes=json.loads(row[7] or "[]"),
        )
    except Exception:
        return None


def _load_second_latest_macro(conn: sqlite3.Connection, as_of: str) -> Any:
    """Load the second-most-recent macro snapshot."""
    from iol_engines.macro.engine import MacroSignal

    cur = conn.cursor()
    cur.execute(
        """
        SELECT as_of, argentina_macro_stress, global_risk_on,
               inflation_mom_pct, bcra_rate_pct, fed_rate_pct,
               us_cpi_yoy_pct, usd_ars_official, usd_ars_blue,
               cedear_fx_premium_pct, sentiment_score
        FROM engine_macro_snapshots
        WHERE as_of < ?
        ORDER BY as_of DESC LIMIT 1
        """,
        (as_of,),
    )
    row = cur.fetchone()
    if not row:
        return None
    try:
        return MacroSignal(
            as_of=row[0],
            argentina_macro_stress=row[1],
            global_risk_on=row[2],
            inflation_mom_pct=row[3],
            bcra_rate_pct=row[4],
            fed_rate_pct=row[5],
            us_cpi_yoy_pct=row[6],
            usd_ars_official=row[7],
            usd_ars_blue=row[8],
            cedear_fx_premium_pct=row[9],
            sentiment_score=row[10],
        )
    except Exception:
        return None


def _load_prev_smart_money(conn: sqlite3.Connection, as_of: str) -> List[Any]:
    """Load smart money signals from the snapshot prior to as_of."""
    from iol_engines.smart_money.engine import SmartMoneySignal

    # Find the second-latest as_of date
    cur = conn.cursor()
    cur.execute(
        """
        SELECT DISTINCT as_of FROM engine_smart_money_snapshots
        WHERE as_of < ?
        ORDER BY as_of DESC LIMIT 1
        """,
        (as_of,),
    )
    row = cur.fetchone()
    if not row:
        return []
    prev_as_of = row[0]

    cur.execute(
        """
        SELECT as_of, symbol, net_institutional_direction, conviction_score,
               top_holders_added_json, top_holders_trimmed_json, latest_13f_date
        FROM engine_smart_money_snapshots
        WHERE as_of = ?
        """,
        (prev_as_of,),
    )
    import json
    results = []
    for r in cur.fetchall():
        try:
            results.append(SmartMoneySignal(
                as_of=r[0],
                symbol=r[1],
                net_institutional_direction=r[2],
                conviction_score=r[3],
                top_holders_added=json.loads(r[4] or "[]"),
                top_holders_trimmed=json.loads(r[5] or "[]"),
                latest_13f_date=r[6],
            ))
        except Exception:
            pass
    return results
