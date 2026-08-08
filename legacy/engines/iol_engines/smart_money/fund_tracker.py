"""Registry of tracked institutional funds and their SEC CIKs.

Each fund entry carries a weight (used to compute conviction score) and
the mapping between SEC ticker symbols and IOL-available symbols (CEDEARs).

To add a new fund: append a FundConfig entry to TRACKED_FUNDS.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional


@dataclass(frozen=True)
class FundConfig:
    name: str
    cik: str            # Zero-padded 10-digit CIK string
    aum_weight: float   # Relative weight for conviction aggregation (0-1)
    description: str = ""


# Funds currently tracked.  Start conservative — 3 funds.
TRACKED_FUNDS: List[FundConfig] = [
    FundConfig(
        name="Berkshire Hathaway",
        cik="0001067983",
        aum_weight=1.0,
        description="Warren Buffett's holding company — value investing benchmark",
    ),
    FundConfig(
        name="ARK Innovation ETF (ARKK)",
        cik="0001579982",
        aum_weight=0.6,
        description="Cathie Wood's disruptive innovation fund — growth/tech signal",
    ),
    FundConfig(
        name="Vanguard Group",
        cik="0000102909",
        aum_weight=0.5,
        description="Passive index behemoth — large position changes signal structural shifts",
    ),
]

# Maps SEC ticker → IOL CEDEAR symbol (where different).
# CEDEARs in Argentina often use the same ticker as the US stock.
CEDEAR_SYMBOL_MAP: Dict[str, str] = {
    "BRK.B": "BRK/B",      # Berkshire Class B not a CEDEAR but keep for completeness
    "GOOGL": "GOOGL",
    "GOOG": "GOOGL",
    "META": "META",
}

# CEDEAR-eligible symbols (cross-listed in Argentina via IOL).
# These are the ones we care about for the smart money signal.
CEDEAR_UNIVERSE: List[str] = [
    "AAPL", "MSFT", "AMZN", "NVDA", "GOOGL", "META",
    "TSLA", "JPM", "V", "JNJ", "WMT", "PG", "XOM",
    "BAC", "HD", "KO", "PFE", "ABBV", "AVGO", "ORCL",
    "NFLX", "DIS", "INTC", "AMD", "QCOM", "CSCO",
    "GS", "MS", "C", "WFC", "CVX", "BA", "CAT",
    "MCD", "SBUX", "NKE", "ADBE", "CRM", "NOW",
]


def normalize_sec_ticker(ticker: str) -> str:
    """Map SEC ticker to the IOL-standard symbol."""
    t = ticker.strip().upper()
    return CEDEAR_SYMBOL_MAP.get(t, t)
