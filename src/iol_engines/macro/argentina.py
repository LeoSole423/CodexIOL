"""Argentina macro data fetchers.

Fetches:
  - BCRA Open Data API (official exchange rate, policy rate)
  - CEDEAR FX premium: computed from market_symbol_snapshots (no external call)

All functions return (value, error_note).  On failure the value is None and
the caller falls back gracefully.
"""
from __future__ import annotations

import json
import sqlite3
import urllib.request
from datetime import date, timedelta
from typing import Dict, List, Optional, Tuple


_BCRA_BASE = "https://api.bcra.gob.ar/estadisticas/v3.0/datosvariable"
_TIMEOUT = 12


def _bcra_get(var_id: int, days_back: int = 30) -> Tuple[Optional[float], str]:
    """Fetch the latest value of a BCRA variable.

    BCRA API: GET /estadisticas/v3.0/datosvariable/{id}/{from}/{to}
    Returns the most recent available observation.
    """
    today = date.today()
    date_from = (today - timedelta(days=days_back)).isoformat()
    date_to = today.isoformat()
    url = f"{_BCRA_BASE}/{var_id}/{date_from}/{date_to}"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "CodexIOL/1.0"})
        with urllib.request.urlopen(req, timeout=_TIMEOUT) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        results = data.get("results", [])
        if not results:
            return None, f"BCRA var {var_id}: no data"
        # results are ordered ascending; last = most recent
        latest = results[-1]
        value = latest.get("valor")
        if value is None:
            return None, f"BCRA var {var_id}: null valor"
        return float(value), ""
    except Exception as exc:
        return None, f"BCRA var {var_id} error: {exc}"


# BCRA variable IDs (documented at api.bcra.gob.ar/estadisticas)
# 1  = Tipo de cambio minorista (oficial USD/ARS)
# 7  = Tasa de política monetaria (%)
# 272 = Tipo de pase pasivo a 1 día (%)  — alternative policy rate
_VAR_USD_OFFICIAL = 1
_VAR_POLICY_RATE = 7


def fetch_usd_official() -> Tuple[Optional[float], str]:
    """Official USD/ARS retail rate from BCRA."""
    return _bcra_get(_VAR_USD_OFFICIAL)


def fetch_bcra_rate() -> Tuple[Optional[float], str]:
    """BCRA monetary policy rate (% TNA)."""
    return _bcra_get(_VAR_POLICY_RATE)


# ── CEDEAR FX premium ────────────────────────────────────────────────────────
# CEDEARs trade in ARS but track their USD underlying × an implicit FX rate.
# Premium = (CEDEAR_ARS / official_rate / USD_reference) - 1
# We estimate the implicit FX by comparing pairs of CEDEARs whose
# ratio should equal the ratio of their US prices.
# Simpler: use the CCL (contado con liquidación) approximation from IOL data
# if we have both the CEDEAR price and a known recent USD ADR reference stored.
#
# Since we don't store USD ADR reference prices locally in Phase 2, we compute
# the implicit rate from a basket of CEDEARs using the official rate and a
# known ratio: implicitFX = cedear_price_ars / cedear_ratio / usd_reference.
# We approximate usd_reference using the average daily_var_pct to see
# divergence from official rate.
#
# Conservative approach for Phase 2: return None (not computable without
# the USD reference).  MacroEngine will log a note and continue.

def fetch_cedear_fx_premium(
    conn: sqlite3.Connection,
    as_of: str,
    usd_official: Optional[float],
) -> Tuple[Optional[float], str]:
    """Estimate CEDEAR implicit FX premium vs official rate.

    Uses the price of CEDEAR vs its ARS-equivalent at official rate.
    Returns premium as a fraction (e.g. 0.15 = 15% above official).

    Currently returns None when USD reference is unavailable — will be
    refined in a future phase when USD reference prices are stored.
    """
    # This is intentionally left as a stub for Phase 2.
    # The CEDEAR FX premium requires knowing the US ADR closing price,
    # which is not currently stored in market_symbol_snapshots.
    return None, "CEDEAR FX premium: USD reference not yet available (Phase 2 stub)"


# ── Argentina macro stress score ─────────────────────────────────────────────

def compute_argentina_stress(
    bcra_rate_pct: Optional[float],
    usd_official: Optional[float],
    cedear_premium: Optional[float],
    prev_usd_official: Optional[float],
) -> float:
    """Compute Argentina macro stress index 0-100.

    Components:
      - Policy rate level: very high rates = stress
      - Peso depreciation rate: fast devaluation = stress
      - CEDEAR FX premium: high gap = stress

    Returns 50 (neutral) when data is insufficient.
    """
    score_components: List[float] = []

    # Component 1: Policy rate (max reasonable = 200% TNA)
    if bcra_rate_pct is not None:
        # 0% → 0 stress, 200%+ → 100 stress
        rate_stress = min(100.0, bcra_rate_pct / 2.0)
        score_components.append(rate_stress)

    # Component 2: Devaluation pace
    if usd_official is not None and prev_usd_official is not None and prev_usd_official > 0:
        monthly_deval_pct = (usd_official / prev_usd_official - 1) * 100
        # 0% deval → 0 stress, 10%+ monthly deval → 100 stress
        deval_stress = min(100.0, monthly_deval_pct * 10)
        score_components.append(deval_stress)

    # Component 3: CEDEAR premium
    if cedear_premium is not None:
        # 0% premium → 0, 100% premium → 100 stress
        premium_stress = min(100.0, cedear_premium * 100)
        score_components.append(premium_stress)

    if not score_components:
        return 50.0   # neutral — no data

    return round(sum(score_components) / len(score_components), 1)


def fetch_prev_usd_official(days_back: int = 35) -> Tuple[Optional[float], str]:
    """Fetch the official USD rate from ~30 days ago for devaluation calc."""
    val, err = _bcra_get(_VAR_USD_OFFICIAL, days_back=days_back)
    return val, err
