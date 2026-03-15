"""Global macro data fetchers via FRED (Federal Reserve Economic Data).

FRED provides free public CSV endpoints — no API key required.
Endpoint: https://fred.stlouisfed.org/graph/fredgraph.csv?id={SERIES_ID}

Series used:
  FEDFUNDS  — Effective Federal Funds Rate (%)
  CPIAUCSL  — CPI All Urban Consumers (index level, use YoY % change)
  VIXCLS    — CBOE Volatility Index (same as used in regime engine)
"""
from __future__ import annotations

import urllib.request
from typing import List, Optional, Tuple


_FRED_BASE = "https://fred.stlouisfed.org/graph/fredgraph.csv?id="
_TIMEOUT = 12
_HEADERS = {"User-Agent": "CodexIOL/1.0 (macro-engine)"}


def _fetch_fred_series(series_id: str) -> Tuple[List[Tuple[str, float]], str]:
    """Download a FRED series and return sorted list of (date, value)."""
    url = _FRED_BASE + series_id
    try:
        req = urllib.request.Request(url, headers=_HEADERS)
        with urllib.request.urlopen(req, timeout=_TIMEOUT) as resp:
            raw = resp.read().decode("utf-8")
        rows: List[Tuple[str, float]] = []
        for line in raw.splitlines()[1:]:   # skip header
            parts = line.split(",")
            if len(parts) != 2:
                continue
            date_str, val_str = parts[0].strip(), parts[1].strip()
            if val_str in (".", ""):
                continue
            try:
                rows.append((date_str, float(val_str)))
            except ValueError:
                continue
        return rows, ""
    except Exception as exc:
        return [], f"FRED {series_id} error: {exc}"


def _latest_value(series: List[Tuple[str, float]]) -> Optional[float]:
    if not series:
        return None
    return series[-1][1]


def fetch_fed_rate() -> Tuple[Optional[float], str]:
    """Effective Fed Funds Rate (% per annum)."""
    rows, err = _fetch_fred_series("FEDFUNDS")
    return _latest_value(rows), err


def fetch_us_cpi_yoy() -> Tuple[Optional[float], str]:
    """US CPI year-over-year % change (computed from CPIAUCSL index)."""
    rows, err = _fetch_fred_series("CPIAUCSL")
    if not rows or len(rows) < 13:
        return None, err or "CPIAUCSL: insufficient data"
    # YoY = (latest / value_12_months_ago) - 1
    latest = rows[-1][1]
    year_ago = rows[-13][1]
    if year_ago == 0:
        return None, "CPIAUCSL: zero base"
    yoy = (latest / year_ago - 1) * 100
    return round(yoy, 2), ""


def fetch_vix() -> Tuple[Optional[float], str]:
    """CBOE VIX (volatility index)."""
    rows, err = _fetch_fred_series("VIXCLS")
    return _latest_value(rows), err


def compute_global_risk_on(
    fed_rate_pct: Optional[float],
    us_cpi_yoy_pct: Optional[float],
    vix: Optional[float],
) -> float:
    """Global risk-on score 0-100 (higher = more risk appetite).

    Components:
      - Fed rate level: high rates = less risk-on
      - CPI: high inflation = less risk-on
      - VIX: high vol = less risk-on
    """
    components: list[float] = []

    # Fed rate: 0% → risk-on 80, 6%+ → risk-on 20
    if fed_rate_pct is not None:
        rate_score = max(20.0, 80.0 - (fed_rate_pct / 6.0) * 60)
        components.append(rate_score)

    # CPI: 2% target → neutral 50; <2% → 70; >8% → 20
    if us_cpi_yoy_pct is not None:
        if us_cpi_yoy_pct <= 2:
            cpi_score = 70.0
        elif us_cpi_yoy_pct <= 4:
            cpi_score = 50.0
        elif us_cpi_yoy_pct <= 6:
            cpi_score = 35.0
        else:
            cpi_score = 20.0
        components.append(cpi_score)

    # VIX: <15 → 80, 15-25 → 55, 25-35 → 35, >35 → 15
    if vix is not None:
        if vix < 15:
            vix_score = 80.0
        elif vix < 25:
            vix_score = 55.0
        elif vix < 35:
            vix_score = 35.0
        else:
            vix_score = 15.0
        components.append(vix_score)

    if not components:
        return 50.0   # neutral

    return round(sum(components) / len(components), 1)
