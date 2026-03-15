"""Bull/bear market indicators computed from local DB price data.

All functions are pure: they accept a list of (date, price) tuples and
return a scalar or a classification string.  No DB I/O here.
"""
from __future__ import annotations

import math
import statistics
from typing import Dict, List, Optional, Tuple


PriceSeries = List[Tuple[str, float]]   # [(YYYY-MM-DD, last_price), ...]


# ── Moving average helpers ───────────────────────────────────────────────────

def moving_average(prices: List[float], window: int) -> Optional[float]:
    """Simple moving average of the last *window* prices. None if not enough data."""
    if len(prices) < window:
        return None
    return statistics.mean(prices[-window:])


def is_above_ma(prices: List[float], window: int) -> Optional[bool]:
    """True if the last price is above its *window*-day MA."""
    ma = moving_average(prices, window)
    if ma is None or not prices:
        return None
    return prices[-1] > ma


# ── Breadth score ────────────────────────────────────────────────────────────

def breadth_score(symbols_prices: Dict[str, List[float]], ma_window: int = 50) -> float:
    """% of symbols whose last price is above their *ma_window*-day MA.

    Returns 0-100.  Symbols with insufficient history are skipped.
    """
    above = 0
    total = 0
    for prices in symbols_prices.values():
        result = is_above_ma(prices, ma_window)
        if result is None:
            continue
        total += 1
        if result:
            above += 1
    if total == 0:
        return 50.0   # neutral when no data
    return (above / total) * 100.0


# ── Volatility regime ────────────────────────────────────────────────────────

def daily_returns(prices: List[float]) -> List[float]:
    """Compute day-over-day percentage returns."""
    if len(prices) < 2:
        return []
    return [(prices[i] - prices[i - 1]) / prices[i - 1] * 100 for i in range(1, len(prices))]


def rolling_volatility(prices: List[float], window: int = 20) -> Optional[float]:
    """Annualised daily return std-dev over the last *window* days (in %)."""
    rets = daily_returns(prices)
    if len(rets) < window:
        return None
    sample = rets[-window:]
    try:
        std = statistics.stdev(sample)
    except statistics.StatisticsError:
        return None
    return std * math.sqrt(252)   # annualise


def classify_volatility(annualised_vol_pct: Optional[float]) -> str:
    """Map annualised vol % to a regime label."""
    if annualised_vol_pct is None:
        return "normal"
    if annualised_vol_pct < 15:
        return "low"
    if annualised_vol_pct < 30:
        return "normal"
    if annualised_vol_pct < 50:
        return "high"
    return "extreme"


def average_volatility(symbols_prices: Dict[str, List[float]], window: int = 20) -> Optional[float]:
    """Mean annualised volatility across all symbols with enough data."""
    vols = []
    for prices in symbols_prices.values():
        v = rolling_volatility(prices, window)
        if v is not None:
            vols.append(v)
    if not vols:
        return None
    return statistics.mean(vols)


# ── Momentum score ───────────────────────────────────────────────────────────

def momentum_score(prices: List[float], lookback: int = 30) -> Optional[float]:
    """Simple price momentum: (last / price[lookback days ago] - 1) * 100."""
    if len(prices) < lookback + 1:
        return None
    ref = prices[-(lookback + 1)]
    if ref == 0:
        return None
    return (prices[-1] / ref - 1) * 100


def average_momentum(symbols_prices: Dict[str, List[float]], lookback: int = 30) -> float:
    """Average 30-day momentum across all symbols with enough data."""
    scores = []
    for prices in symbols_prices.values():
        m = momentum_score(prices, lookback)
        if m is not None:
            scores.append(m)
    if not scores:
        return 0.0
    return statistics.mean(scores)


# ── Regime classification ────────────────────────────────────────────────────

def compute_regime_score(
    breadth: float,
    avg_momentum_pct: float,
    volatility_regime: str,
) -> float:
    """Composite bull/bear score (0-100).

    Weights:
      - breadth (% above MA50): 55%
      - momentum component:     30%
      - volatility penalty:     15%

    Higher score = more bullish.
    """
    # Normalise momentum to 0-100.
    # A +20% 30-day move maps to 100, -20% maps to 0.
    mom_norm = max(0.0, min(100.0, (avg_momentum_pct + 20) / 40 * 100))

    # Volatility penalty: extreme vol drags score down.
    vol_penalties = {"low": 0, "normal": 0, "high": -8, "extreme": -18}
    vol_adj = vol_penalties.get(volatility_regime, 0)

    score = breadth * 0.55 + mom_norm * 0.30 + vol_adj
    return max(0.0, min(100.0, score))


def classify_regime(regime_score: float, volatility_regime: str) -> str:
    """Map a composite score to a regime label."""
    if volatility_regime == "extreme" and regime_score < 50:
        return "crisis"
    if regime_score >= 65:
        return "bull"
    if regime_score <= 35:
        return "bear"
    return "sideways"


def favored_classes(regime: str) -> List[str]:
    """Asset classes to favour given a regime."""
    mapping = {
        "bull": ["equity"],
        "sideways": ["equity", "bonds"],
        "bear": ["gold", "bonds", "cash"],
        "crisis": ["cash", "gold"],
    }
    return mapping.get(regime, ["equity"])


def defensive_adjustment(regime: str) -> float:
    """How much to shift away from equity (negative = reduce equity %)."""
    mapping = {
        "bull": 0.0,
        "sideways": -0.05,
        "bear": -0.15,
        "crisis": -0.25,
    }
    return mapping.get(regime, 0.0)


def confidence_from_score(regime_score: float) -> float:
    """Confidence 0-1: highest at extremes (very bull or very bear)."""
    # Distance from neutral (50) normalised to 0-1
    return min(1.0, abs(regime_score - 50) / 50)
