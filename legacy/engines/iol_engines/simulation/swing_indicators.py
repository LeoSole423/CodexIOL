"""Technical indicators for swing trading simulation.

All functions are pure: they accept price series and return scalars or
dataclasses.  No DB I/O.  Uses the same PriceSeries type as regime/indicators.py.
"""
from __future__ import annotations

import math
import sqlite3
import statistics
from dataclasses import dataclass
from typing import List, Optional, Tuple


PriceSeries = List[Tuple[str, float]]   # [(YYYY-MM-DD, price), ...]
OHLCSeries = List[Tuple[str, float, float, float, float]]  # [(date, open, high, low, close), ...]


# ── OHLCV loader ──────────────────────────────────────────────────────────────

def _load_ohlcv_series(
    conn: sqlite3.Connection,
    symbol: str,
    as_of: str,
    lookback: int = 60,
) -> tuple:
    """Return (closes, highs, lows) lists from symbol_daily_ohlcv. Falls back to empty lists."""
    from datetime import date, timedelta
    try:
        end = date.fromisoformat(as_of)
        start = (end - timedelta(days=lookback)).isoformat()
    except ValueError:
        return [], [], []
    rows = conn.execute(
        """
        SELECT close, high, low FROM symbol_daily_ohlcv
        WHERE symbol = ? AND trade_date >= ? AND trade_date <= ?
          AND close IS NOT NULL
        ORDER BY trade_date ASC
        """,
        (symbol, start, as_of),
    ).fetchall()
    closes = [float(r[0]) for r in rows]
    highs  = [float(r[1]) if r[1] is not None else float(r[0]) for r in rows]
    lows   = [float(r[2]) if r[2] is not None else float(r[0]) for r in rows]
    return closes, highs, lows


# ── Helpers ──────────────────────────────────────────────────────────────────

def _prices(series: PriceSeries) -> List[float]:
    return [p for _, p in series]


def _ema(values: List[float], period: int) -> List[float]:
    """Exponential moving average."""
    if not values:
        return []
    k = 2.0 / (period + 1)
    result = [values[0]]
    for v in values[1:]:
        result.append(v * k + result[-1] * (1 - k))
    return result


# ── RSI ───────────────────────────────────────────────────────────────────────

def rsi(series: PriceSeries, period: int = 14) -> Optional[float]:
    """Relative Strength Index (0-100). None if insufficient data."""
    prices = _prices(series)
    if len(prices) < period + 1:
        return None
    deltas = [prices[i] - prices[i - 1] for i in range(1, len(prices))]
    recent = deltas[-(period):]
    gains = [d for d in recent if d > 0]
    losses = [-d for d in recent if d < 0]
    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))


# ── MACD ─────────────────────────────────────────────────────────────────────

def macd(
    series: PriceSeries,
    fast: int = 12,
    slow: int = 26,
    signal_period: int = 9,
) -> Optional[Tuple[float, float, float]]:
    """MACD line, signal line, histogram. None if insufficient data.

    Returns: (macd_line, signal_line, histogram)
    """
    prices = _prices(series)
    if len(prices) < slow + signal_period:
        return None
    ema_fast = _ema(prices, fast)
    ema_slow = _ema(prices, slow)
    macd_line = [f - s for f, s in zip(ema_fast[slow - 1:], ema_slow)]
    if len(macd_line) < signal_period:
        return None
    signal_line = _ema(macd_line, signal_period)
    histogram = macd_line[-1] - signal_line[-1]
    return (macd_line[-1], signal_line[-1], histogram)


# ── Bollinger Bands ──────────────────────────────────────────────────────────

def bollinger_bands(
    series: PriceSeries,
    period: int = 20,
    num_std: float = 2.0,
) -> Optional[Tuple[float, float, float]]:
    """Bollinger Bands. None if insufficient data.

    Returns: (upper, middle, lower)
    """
    prices = _prices(series)
    if len(prices) < period:
        return None
    window = prices[-period:]
    mid = statistics.mean(window)
    std = statistics.stdev(window)
    return (mid + num_std * std, mid, mid - num_std * std)


# ── ATR (Average True Range) ─────────────────────────────────────────────────

def atr(
    closes: List[float],
    highs: Optional[List[float]] = None,
    lows: Optional[List[float]] = None,
    period: int = 14,
) -> float:
    """Average True Range. Uses high/low if provided, else close-to-close proxy."""
    if len(closes) < 2:
        return 0.0
    if highs and lows and len(highs) == len(closes) and len(lows) == len(closes):
        trs = []
        for i in range(1, len(closes)):
            high_i = highs[i]
            low_i = lows[i]
            prev_close = closes[i - 1]
            tr = max(high_i - low_i, abs(high_i - prev_close), abs(low_i - prev_close))
            trs.append(tr)
    else:
        # Fallback: close-to-close range
        trs = [abs(closes[i] - closes[i - 1]) for i in range(1, len(closes))]
    if not trs:
        return 0.0
    n = min(period, len(trs))
    return sum(trs[-n:]) / n


def atr_from_close_only(series: PriceSeries, period: int = 14) -> Optional[float]:
    """ATR approximation when only close prices are available.

    Uses |close[i] - close[i-1]| as a proxy for true range.
    """
    prices = _prices(series)
    if len(prices) < period + 1:
        return None
    diffs = [abs(prices[i] - prices[i - 1]) for i in range(1, len(prices))]
    return statistics.mean(diffs[-period:])


# ── Moving average helpers ────────────────────────────────────────────────────

def moving_average(series: PriceSeries, window: int) -> Optional[float]:
    prices = _prices(series)
    if len(prices) < window:
        return None
    return statistics.mean(prices[-window:])


def price_vs_ma(series: PriceSeries, window: int) -> Optional[float]:
    """% deviation of last price vs. its MA. Positive = above MA."""
    ma = moving_average(series, window)
    if ma is None or ma == 0:
        return None
    last = _prices(series)[-1]
    return (last - ma) / ma * 100.0


# ── Aggregate dataclass ───────────────────────────────────────────────────────

@dataclass
class SwingTA:
    """All TA indicators for a single symbol."""
    symbol: str
    last_price: float
    rsi_14: Optional[float]
    macd_line: Optional[float]
    macd_signal: Optional[float]
    macd_histogram: Optional[float]
    bb_upper: Optional[float]
    bb_mid: Optional[float]
    bb_lower: Optional[float]
    atr_14: Optional[float]           # ATR for stop sizing
    ma20_deviation_pct: Optional[float]  # % above/below MA20
    ma50_deviation_pct: Optional[float]
    price_above_ma20: Optional[bool]
    price_above_ma50: Optional[bool]

    @property
    def macd_bullish(self) -> Optional[bool]:
        """True if MACD histogram is positive (bullish momentum)."""
        if self.macd_histogram is None:
            return None
        return self.macd_histogram > 0

    @property
    def bb_position(self) -> Optional[float]:
        """Position within Bollinger Bands (0 = lower, 1 = upper). None if no data."""
        if self.bb_upper is None or self.bb_lower is None or self.bb_upper == self.bb_lower:
            return None
        return (self.last_price - self.bb_lower) / (self.bb_upper - self.bb_lower)


def compute_swing_ta(
    symbol: str,
    price_history: PriceSeries,
    conn: Optional[sqlite3.Connection] = None,
    as_of: Optional[str] = None,
) -> "SwingTA":
    """Compute all swing TA indicators for a symbol from its price history.

    price_history should contain at least 30 days of data for reliable signals.
    If conn and as_of are provided, ATR will use OHLCV high/low data when available.
    """
    prices_list = _prices(price_history)
    last_price = prices_list[-1] if prices_list else 0.0

    rsi_val = rsi(price_history)
    macd_result = macd(price_history)
    bb_result = bollinger_bands(price_history)
    ma20_dev = price_vs_ma(price_history, 20)
    ma50_dev = price_vs_ma(price_history, 50)
    ma20 = moving_average(price_history, 20)
    ma50 = moving_average(price_history, 50)

    closes = [p for _, p in price_history]
    highs_list: List[float] = []
    lows_list: List[float] = []
    if conn is not None and as_of is not None:
        _, highs_list, lows_list = _load_ohlcv_series(conn, symbol, as_of)
    atr_val = atr(closes, highs=highs_list or None, lows=lows_list or None)

    return SwingTA(
        symbol=symbol,
        last_price=last_price,
        rsi_14=rsi_val,
        macd_line=macd_result[0] if macd_result else None,
        macd_signal=macd_result[1] if macd_result else None,
        macd_histogram=macd_result[2] if macd_result else None,
        bb_upper=bb_result[0] if bb_result else None,
        bb_mid=bb_result[1] if bb_result else None,
        bb_lower=bb_result[2] if bb_result else None,
        atr_14=atr_val if atr_val else None,
        ma20_deviation_pct=ma20_dev,
        ma50_deviation_pct=ma50_dev,
        price_above_ma20=(last_price > ma20) if ma20 is not None else None,
        price_above_ma50=(last_price > ma50) if ma50 is not None else None,
    )
