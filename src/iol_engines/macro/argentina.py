"""Argentina macro data fetchers.

Fetches:
  - BCRA Estadísticas Cambiarias API v1.0 (official USD/ARS rate)
  - CEDEAR FX premium: computed from ARS/cable price pairs in market_symbol_snapshots

BCRA policy rate (var 7) is no longer available via the estadisticas API
(v2 deprecated, v3 returns 404). Fetching is skipped; the stress score
degrades gracefully to available components.

All functions return (value, error_note).  On failure the value is None and
the caller falls back gracefully.
"""
from __future__ import annotations

import json
import sqlite3
import urllib.request
from datetime import date, timedelta
from typing import List, Optional, Tuple

_BCRA_FX_BASE = "https://api.bcra.gob.ar/estadisticascambiarias/v1.0/cotizaciones"
_TIMEOUT = 15
_HEADERS = {"User-Agent": "curl/7.80.0", "Accept": "application/json"}


def _fetch_usd_cotizacion(date_from: str, date_to: str) -> Tuple[Optional[float], str]:
    """Fetch USD/ARS official rate from BCRA estadisticascambiarias API.

    Returns the *most recent* value within the date range.
    """
    url = f"{_BCRA_FX_BASE}/USD?fechadesde={date_from}&fechahasta={date_to}"
    try:
        req = urllib.request.Request(url, headers=_HEADERS)
        with urllib.request.urlopen(req, timeout=_TIMEOUT) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        results = data.get("results", [])
        if not results:
            return None, f"BCRA cotizaciones: no data for {date_from}–{date_to}"
        # API returns newest-first; first entry is the most recent.
        detalle = results[0].get("detalle", [])
        if not detalle:
            return None, "BCRA cotizaciones: empty detalle"
        value = detalle[0].get("tipoCotizacion")
        if value is None:
            return None, "BCRA cotizaciones: null tipoCotizacion"
        return float(value), ""
    except Exception as exc:
        return None, f"BCRA cotizaciones error: {exc}"


def fetch_usd_official() -> Tuple[Optional[float], str]:
    """Official USD/ARS retail rate from BCRA (today or most recent)."""
    today = date.today().isoformat()
    date_from = (date.today() - timedelta(days=5)).isoformat()
    return _fetch_usd_cotizacion(date_from, today)


def fetch_bcra_rate() -> Tuple[Optional[float], str]:
    """BCRA monetary policy rate (% TNA).

    The BCRA estadisticas API that provided this variable (var 7) has been
    deprecated/removed.  Returns None with an informational note so the
    macro stress score degrades gracefully.
    """
    return None, "BCRA policy rate: API unavailable (estadisticas v3 removed)"


def fetch_prev_usd_official(days_back: int = 35) -> Tuple[Optional[float], str]:
    """Fetch the official USD rate from ~30 days ago for devaluation calc."""
    today = date.today()
    date_to = (today - timedelta(days=days_back - 5)).isoformat()
    date_from = (today - timedelta(days=days_back)).isoformat()
    return _fetch_usd_cotizacion(date_from, date_to)


# ── CEDEAR FX premium ────────────────────────────────────────────────────────

def fetch_cedear_fx_premium(
    conn: sqlite3.Connection,
    as_of: str,
    usd_official: Optional[float],
) -> Tuple[Optional[float], str]:
    """Estimate CEDEAR implicit FX premium (CCL) vs official rate.

    Uses pairs stored in market_symbol_snapshots:
      - SYMBOL  = ARS price (e.g. AAPL)
      - SYMBOLC = USD cable price (e.g. AAPLC)

    Implied CCL = ARS_price / USD_cable_price.
    Premium = (median_CCL / usd_official) - 1  (fraction, e.g. 0.04 = 4%).

    Requires at least 5 pairs and a valid usd_official rate.
    """
    if usd_official is None or usd_official <= 0:
        return None, "CEDEAR FX premium: usd_official not available"
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT a.last_price / b.last_price
            FROM market_symbol_snapshots a
            JOIN market_symbol_snapshots b
              ON b.symbol = a.symbol || 'C'
             AND b.snapshot_date = a.snapshot_date
            WHERE a.snapshot_date = (
                SELECT MAX(snapshot_date) FROM market_symbol_snapshots
                WHERE snapshot_date <= ?
            )
            AND a.last_price > 0
            AND b.last_price > 0
            """,
            (as_of,),
        )
        ccls = [row[0] for row in cur.fetchall()]
        if len(ccls) < 5:
            return None, f"CEDEAR FX premium: insufficient pairs ({len(ccls)})"

        ccls_sorted = sorted(ccls)
        n = len(ccls_sorted)
        # Median — robust to outliers from thinly-traded CEDEARs
        mid = n // 2
        median_ccl = (ccls_sorted[mid - 1] + ccls_sorted[mid]) / 2 if n % 2 == 0 else ccls_sorted[mid]

        premium = (median_ccl / usd_official) - 1.0
        return round(premium, 4), ""
    except Exception as exc:
        return None, f"CEDEAR FX premium error: {exc}"


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
        rate_stress = min(100.0, bcra_rate_pct / 2.0)
        score_components.append(rate_stress)

    # Component 2: Devaluation pace
    if usd_official is not None and prev_usd_official is not None and prev_usd_official > 0:
        monthly_deval_pct = (usd_official / prev_usd_official - 1) * 100
        # 0% deval → 0 stress, 10%+ monthly deval → 100 stress; appreciation = 0 stress
        deval_stress = max(0.0, min(100.0, monthly_deval_pct * 10))
        score_components.append(deval_stress)

    # Component 3: CEDEAR premium
    if cedear_premium is not None:
        premium_stress = min(100.0, cedear_premium * 100)
        score_components.append(premium_stress)

    if not score_components:
        return 50.0   # neutral — no data

    return round(sum(score_components) / len(score_components), 1)
