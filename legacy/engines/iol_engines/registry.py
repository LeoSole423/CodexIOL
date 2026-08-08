"""Engine registry and full pipeline orchestrator.

run_full_engine_pipeline() runs all 5 engines in sequence, using cached
signals where available, and returns a complete StrategyActionPlan.

Staleness policy (how old a cached signal can be before re-running):
  - Regime:      1 day
  - Macro:       1 day
  - Smart Money: 7 days  (data is quarterly)
  - Opportunity: not re-run here — uses existing candidates from DB
  - Strategy:    always re-run (fast, pure DB reads)
"""
from __future__ import annotations

import sqlite3
from datetime import date, timedelta
from typing import Any, Dict, List, Optional

from .macro.engine import MacroMomentumEngine
from .opportunity.adapter import build_adjusted_params
from .regime.engine import MarketRegimeEngine
from .signals import MacroSignal, RegimeSignal, SmartMoneySignal, StrategyActionPlan
from .smart_money.engine import SmartMoneyEngine
from .strategy.engine import PortfolioStrategyEngine


# Staleness thresholds in days.
_REGIME_STALE_DAYS = 1
_MACRO_STALE_DAYS = 1
_SMART_MONEY_STALE_DAYS = 7


def _is_stale(signal_as_of: Optional[str], reference_as_of: str, max_days: int) -> bool:
    """Return True if the cached signal is older than max_days vs. reference_as_of."""
    if signal_as_of is None:
        return True
    try:
        ref = date.fromisoformat(reference_as_of)
        sig = date.fromisoformat(signal_as_of)
        return (ref - sig).days >= max_days
    except ValueError:
        return True


def _get_regime_snapshot_id(conn: sqlite3.Connection, as_of: str) -> Optional[int]:
    row = conn.execute(
        "SELECT id FROM engine_regime_snapshots WHERE as_of = ? LIMIT 1", (as_of,)
    ).fetchone()
    return row[0] if row else None


def _get_macro_snapshot_id(conn: sqlite3.Connection, as_of: str) -> Optional[int]:
    row = conn.execute(
        "SELECT id FROM engine_macro_snapshots WHERE as_of = ? LIMIT 1", (as_of,)
    ).fetchone()
    return row[0] if row else None


def run_full_engine_pipeline(
    as_of: str,
    conn: sqlite3.Connection,
    *,
    budget_ars: Optional[float] = None,
    force_regime: bool = False,
    force_macro: bool = False,
    skip_smart_money: bool = False,
    skip_external: bool = False,
    verbose: bool = True,
) -> Dict[str, Any]:
    """Run all 5 engines and return a complete result dict.

    Returns:
        {
          "as_of": str,
          "regime": RegimeSignal,
          "macro": MacroSignal,
          "smart_money": List[SmartMoneySignal],
          "adjusted_params": {...},   # weights/thresholds for build_candidates
          "strategy": StrategyActionPlan,
        }
    """

    def log(msg: str) -> None:
        if verbose:
            from rich.console import Console
            Console().print(msg)

    # ── Motor 1: Market Regime ───────────────────────────────────────────────
    log("[bold blue]● Motor 1:[/bold blue] Market Regime Engine")
    regime_engine = MarketRegimeEngine()
    cached_regime = regime_engine.load_latest(conn, as_of)
    if force_regime or _is_stale(
        cached_regime.as_of if cached_regime else None, as_of, _REGIME_STALE_DAYS
    ):
        regime_signal = regime_engine.run(
            as_of, conn, fetch_vix=(not skip_external), force_refresh=True
        )
        log(f"  → Computed: [yellow]{regime_signal.regime}[/yellow] (score {regime_signal.regime_score:.1f})")
    else:
        regime_signal = cached_regime
        log(f"  → Cached:   [yellow]{regime_signal.regime}[/yellow] ({cached_regime.as_of})")

    # ── Motor 2: Macro Momentum ──────────────────────────────────────────────
    log("[bold blue]● Motor 2:[/bold blue] Macro Momentum Engine")
    macro_engine = MacroMomentumEngine()
    cached_macro = macro_engine.load_latest(conn, as_of)
    if force_macro or _is_stale(
        cached_macro.as_of if cached_macro else None, as_of, _MACRO_STALE_DAYS
    ):
        macro_signal = macro_engine.run(
            as_of, conn, force_refresh=True, skip_external=skip_external
        )
        log(
            f"  → Computed: AR stress={macro_signal.argentina_macro_stress:.0f}, "
            f"global risk-on={macro_signal.global_risk_on:.0f}"
        )
    else:
        macro_signal = cached_macro
        log(f"  → Cached:   AR stress={macro_signal.argentina_macro_stress:.0f} ({cached_macro.as_of})")

    # ── Motor 3: Smart Money ─────────────────────────────────────────────────
    smart_money_signals: List[SmartMoneySignal] = []
    if not skip_smart_money:
        log("[bold blue]● Motor 3:[/bold blue] Smart Money Engine")
        sm_engine = SmartMoneyEngine()
        cached_sm = sm_engine.load_latest(conn, as_of) or []
        stale = _is_stale(
            cached_sm[0].as_of if cached_sm else None, as_of, _SMART_MONEY_STALE_DAYS
        )
        if stale:
            log("  → Fetching 13F filings from SEC EDGAR...")
            smart_money_signals = sm_engine.run(as_of, conn)
            log(f"  → Computed: {len(smart_money_signals)} symbol signals")
        else:
            smart_money_signals = cached_sm
            log(f"  → Cached:   {len(smart_money_signals)} signals ({cached_sm[0].as_of})")
    else:
        log("[dim]● Motor 3: Smart Money — skipped[/dim]")

    # ── Motor 4: Opportunity adapter ─────────────────────────────────────────
    log("[bold blue]● Motor 4:[/bold blue] Opportunity Adapter")
    adjusted_params = build_adjusted_params(regime_signal, macro_signal, smart_money_signals)
    prov = adjusted_params["provenance"]
    log(
        f"  → Weights: risk={adjusted_params['weights']['risk']:.2f} "
        f"mom={adjusted_params['weights']['momentum']:.2f} "
        f"val={adjusted_params['weights']['value']:.2f} "
        f"cat={adjusted_params['weights']['catalyst']:.2f}"
    )
    if prov.get("threshold_changes"):
        log(f"  → Threshold overrides: {prov['threshold_changes']}")
    if prov.get("catalyst_overrides_count"):
        log(f"  → Catalyst overrides: {prov['catalyst_overrides_count']} symbols")

    # ── Motor 5: Portfolio Strategy ──────────────────────────────────────────
    log("[bold blue]● Motor 5:[/bold blue] Portfolio Strategy Engine")
    strategy_engine = PortfolioStrategyEngine()
    plan = strategy_engine.run(
        as_of,
        conn,
        regime_signal=regime_signal,
        macro_signal=macro_signal,
        regime_snapshot_id=_get_regime_snapshot_id(conn, as_of),
        macro_snapshot_id=_get_macro_snapshot_id(conn, as_of),
        budget_ars=budget_ars,
    )
    log(
        f"  → Plan: {len(plan.actions)} actions, "
        f"ARS {plan.total_deployed_ars:,.0f} deployed, "
        f"defensive={'yes' if plan.defensive_overlay_applied else 'no'}"
    )

    return {
        "as_of": as_of,
        "regime": regime_signal,
        "macro": macro_signal,
        "smart_money": smart_money_signals,
        "adjusted_params": adjusted_params,
        "strategy": plan,
    }
