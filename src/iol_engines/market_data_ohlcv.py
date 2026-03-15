"""OHLCV market data helpers and pivot detection.

Pivot detection algorithm:
  A price bar at index i is a HIGH pivot if:
    high[i] > high[i-strength..i-1] AND high[i] > high[i+1..i+strength]
  A LOW pivot is the mirror.
"""
from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from typing import List, Optional, Tuple


# ── OHLCV queries ─────────────────────────────────────────────────────────────

def load_ohlcv(
    conn: sqlite3.Connection,
    symbol: str,
    date_from: str,
    date_to: str,
) -> List[dict]:
    """Return list of OHLCV rows for a symbol in [date_from, date_to]."""
    rows = conn.execute(
        """
        SELECT trade_date, open, high, low, close, prev_close, daily_var_pct
        FROM symbol_daily_ohlcv
        WHERE symbol = ? AND trade_date >= ? AND trade_date <= ?
        ORDER BY trade_date ASC
        """,
        (symbol, date_from, date_to),
    ).fetchall()
    cols = ["trade_date", "open", "high", "low", "close", "prev_close", "daily_var_pct"]
    return [dict(zip(cols, r)) for r in rows]


def load_intraday_ticks(
    conn: sqlite3.Connection,
    symbol: str,
    trade_date: str,
) -> List[Tuple[str, float]]:
    """Return list of (tick_time, price) for a symbol on a given trade_date."""
    rows = conn.execute(
        """
        SELECT tick_time, price FROM symbol_intraday_ticks
        WHERE symbol = ? AND trade_date = ?
        ORDER BY tick_time ASC
        """,
        (symbol, trade_date),
    ).fetchall()
    return [(r[0], float(r[1])) for r in rows]


# ── Pivot detection ────────────────────────────────────────────────────────────

def detect_pivots(
    conn: sqlite3.Connection,
    symbol: str,
    as_of: str,
    lookback_days: int = 60,
    strength: int = 3,
    timeframe: str = "daily",
    persist: bool = True,
) -> List[dict]:
    """
    Detect pivot highs and lows for a symbol up to as_of.

    strength: number of bars on each side that must be lower (for high) or higher (for low).
    Returns list of dicts with keys: symbol, pivot_date, pivot_type, price, strength, timeframe.
    """
    # Load OHLCV for the lookback window
    from datetime import date, timedelta
    try:
        end = date.fromisoformat(as_of)
        start = (end - timedelta(days=lookback_days)).isoformat()
    except ValueError:
        return []

    rows = conn.execute(
        """
        SELECT trade_date, high, low FROM symbol_daily_ohlcv
        WHERE symbol = ? AND trade_date >= ? AND trade_date <= ?
          AND high IS NOT NULL AND low IS NOT NULL
        ORDER BY trade_date ASC
        """,
        (symbol, start, as_of),
    ).fetchall()

    if len(rows) < strength * 2 + 1:
        return []

    dates  = [r[0] for r in rows]
    highs  = [float(r[1]) for r in rows]
    lows   = [float(r[2]) for r in rows]

    pivots: List[dict] = []
    n = len(dates)
    now_str = datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")

    for i in range(strength, n - strength):
        # Pivot HIGH: high[i] is strictly greater than all neighbors within strength
        if all(highs[i] > highs[i - k] for k in range(1, strength + 1)) and \
           all(highs[i] > highs[i + k] for k in range(1, strength + 1)):
            pivots.append({
                "symbol": symbol, "pivot_date": dates[i],
                "pivot_type": "high", "price": round(highs[i], 4),
                "strength": strength, "timeframe": timeframe,
                "detected_at": now_str,
            })

        # Pivot LOW: low[i] is strictly less than all neighbors within strength
        if all(lows[i] < lows[i - k] for k in range(1, strength + 1)) and \
           all(lows[i] < lows[i + k] for k in range(1, strength + 1)):
            pivots.append({
                "symbol": symbol, "pivot_date": dates[i],
                "pivot_type": "low", "price": round(lows[i], 4),
                "strength": strength, "timeframe": timeframe,
                "detected_at": now_str,
            })

    if persist and pivots:
        _persist_pivots(conn, symbol, pivots, timeframe)

    return pivots


def detect_pivots_all_symbols(
    conn: sqlite3.Connection,
    as_of: str,
    strength: int = 3,
    lookback_days: int = 60,
) -> dict:
    """Run pivot detection for all symbols that have OHLCV data."""
    symbols = [
        r[0] for r in conn.execute(
            "SELECT DISTINCT symbol FROM symbol_daily_ohlcv WHERE trade_date <= ? ORDER BY symbol",
            (as_of,),
        ).fetchall()
    ]
    results: dict = {"as_of": as_of, "symbols_processed": 0, "pivots_detected": 0, "by_symbol": {}}
    for sym in symbols:
        pivots = detect_pivots(conn, sym, as_of, lookback_days=lookback_days, strength=strength)
        results["symbols_processed"] += 1
        results["pivots_detected"] += len(pivots)
        if pivots:
            results["by_symbol"][sym] = pivots
    return results


def _persist_pivots(conn: sqlite3.Connection, symbol: str, pivots: List[dict], timeframe: str) -> None:
    """Insert new pivots, skip if already exists for same symbol+date+type+timeframe."""
    for p in pivots:
        conn.execute(
            """
            INSERT OR IGNORE INTO symbol_pivots
                (symbol, pivot_date, pivot_type, price, strength, timeframe, detected_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (p["symbol"], p["pivot_date"], p["pivot_type"],
             p["price"], p["strength"], p["timeframe"], p["detected_at"]),
        )
    conn.commit()


def get_nearest_support_resistance(
    conn: sqlite3.Connection,
    symbol: str,
    current_price: float,
    as_of: str,
    n: int = 3,
) -> dict:
    """Return the nearest N support (lows) and resistance (highs) pivot levels."""
    rows = conn.execute(
        """
        SELECT pivot_type, price FROM symbol_pivots
        WHERE symbol = ? AND pivot_date <= ? AND timeframe = 'daily'
        ORDER BY pivot_date DESC
        LIMIT 60
        """,
        (symbol, as_of),
    ).fetchall()

    supports = sorted(
        [float(r[1]) for r in rows if r[0] == "low" and float(r[1]) < current_price],
        reverse=True,
    )[:n]
    resistances = sorted(
        [float(r[1]) for r in rows if r[0] == "high" and float(r[1]) > current_price],
    )[:n]

    return {
        "symbol": symbol,
        "current_price": current_price,
        "nearest_support": supports,
        "nearest_resistance": resistances,
    }
