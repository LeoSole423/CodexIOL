"""Market Regime Engine — Motor 1.

Detects the current market phase (bull/bear/sideways/crisis) using price
data already stored in the ``market_symbol_snapshots`` table.  No IOL API
calls, no external HTTP in Phase 1 (pass ``fetch_vix=True`` to enable the
optional FRED VIX fetch).
"""
from __future__ import annotations

import json
import sqlite3
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from ..base import BaseEngine
from ..signals import RegimeSignal
from .indicators import (
    average_momentum,
    average_volatility,
    breadth_score,
    classify_regime,
    classify_volatility,
    compute_regime_score,
    confidence_from_score,
    defensive_adjustment,
    favored_classes,
)

# Minimum number of symbols needed to produce a meaningful breadth score.
_MIN_SYMBOLS = 5
# How many calendar days of price history to load.
_LOOKBACK_DAYS = 252


def _load_price_series(
    conn: sqlite3.Connection,
    as_of: str,
    lookback_days: int = _LOOKBACK_DAYS,
) -> Dict[str, List[float]]:
    """Load ordered price series per symbol from market_symbol_snapshots.

    Returns {symbol: [price_oldest, ..., price_newest]}.
    Only symbols with at least 2 data points are included.
    """
    cur = conn.cursor()
    cur.execute(
        """
        SELECT symbol, snapshot_date, last_price
        FROM market_symbol_snapshots
        WHERE snapshot_date <= ?
          AND snapshot_date >= date(?, ?)
          AND last_price IS NOT NULL
          AND last_price > 0
        ORDER BY symbol, snapshot_date ASC
        """,
        (as_of, as_of, f"-{lookback_days} days"),
    )
    rows = cur.fetchall()

    series: Dict[str, List[float]] = defaultdict(list)
    for symbol, _date, price in rows:
        series[symbol].append(float(price))

    return {sym: prices for sym, prices in series.items() if len(prices) >= 2}


def _fetch_vix(as_of: str) -> Optional[float]:
    """Fetch VIX from FRED (optional; returns None on any error)."""
    try:
        import urllib.request
        url = "https://fred.stlouisfed.org/graph/fredgraph.csv?id=VIXCLS"
        req = urllib.request.Request(url, headers={"User-Agent": "CodexIOL/1.0"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            lines = resp.read().decode("utf-8").splitlines()
        # CSV: DATE,VIXCLS — walk backwards to find latest non-null value
        for line in reversed(lines[1:]):
            parts = line.split(",")
            if len(parts) == 2 and parts[1].strip() not in (".", ""):
                return float(parts[1].strip())
    except Exception:
        pass
    return None


def _upsert_regime(conn: sqlite3.Connection, sig: RegimeSignal, raw_inputs: Dict) -> int:
    """Upsert regime signal into engine_regime_snapshots; return row id."""
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO engine_regime_snapshots
            (as_of, created_at_utc, regime, confidence, regime_score,
             favored_asset_classes_json, defensive_weight_adjustment,
             breadth_score, volatility_regime, notes, raw_inputs_json)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(as_of) DO UPDATE SET
            created_at_utc              = excluded.created_at_utc,
            regime                      = excluded.regime,
            confidence                  = excluded.confidence,
            regime_score                = excluded.regime_score,
            favored_asset_classes_json  = excluded.favored_asset_classes_json,
            defensive_weight_adjustment = excluded.defensive_weight_adjustment,
            breadth_score               = excluded.breadth_score,
            volatility_regime           = excluded.volatility_regime,
            notes                       = excluded.notes,
            raw_inputs_json             = excluded.raw_inputs_json
        """,
        (
            sig.as_of,
            datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
            sig.regime,
            sig.confidence,
            sig.regime_score,
            json.dumps(sig.favored_asset_classes),
            sig.defensive_weight_adjustment,
            sig.breadth_score,
            sig.volatility_regime,
            sig.notes,
            json.dumps(raw_inputs),
        ),
    )
    conn.commit()
    return cur.lastrowid or 0


class MarketRegimeEngine(BaseEngine):
    """Detect whether the market is in a bull, bear, sideways, or crisis phase.

    Algorithm (Phase 1 — local data only):
      1. Load last 252 days of price history from market_symbol_snapshots.
      2. Compute breadth score (% symbols above their 50-day MA).
      3. Compute average annualised volatility over 20-day window.
      4. Compute average 30-day momentum.
      5. Combine into composite regime_score (0-100).
      6. Classify regime from score thresholds.
      7. Optionally adjust with VIX from FRED (fetch_vix=True).
    """

    def run(
        self,
        as_of: str,
        conn: sqlite3.Connection,
        *,
        fetch_vix: bool = False,
        force_refresh: bool = False,
    ) -> RegimeSignal:
        """Compute regime signal for *as_of* and upsert into the DB."""
        # Return cached signal unless forced.
        if not force_refresh:
            cached = self.load_latest(conn, as_of)
            if cached is not None and cached.as_of == as_of:
                return cached

        # Load historical prices.
        series = _load_price_series(conn, as_of)
        symbol_count = len(series)

        notes_parts: List[str] = []

        if symbol_count < _MIN_SYMBOLS:
            notes_parts.append(f"Only {symbol_count} symbols available — low confidence.")

        # Core indicators.
        b_score = breadth_score(series)
        avg_vol = average_volatility(series)
        vol_regime = classify_volatility(avg_vol)
        avg_mom = average_momentum(series, lookback=30)

        # Optional VIX adjustment.
        vix: Optional[float] = None
        if fetch_vix:
            vix = _fetch_vix(as_of)
            if vix is not None:
                # High VIX drags regime score down (VIX > 30 = stressed).
                vix_penalty = max(0.0, (vix - 20) / 30 * 15)  # max −15 pts
                avg_mom -= vix_penalty
                notes_parts.append(f"VIX={vix:.1f} applied.")
            else:
                notes_parts.append("VIX fetch failed — skipped.")

        regime_sc = compute_regime_score(b_score, avg_mom, vol_regime)
        regime = classify_regime(regime_sc, vol_regime)
        fav = favored_classes(regime)
        def_adj = defensive_adjustment(regime)
        conf = confidence_from_score(regime_sc)

        raw_inputs = {
            "symbol_count": symbol_count,
            "breadth_score": round(b_score, 2),
            "avg_volatility_annualised": round(avg_vol, 2) if avg_vol is not None else None,
            "avg_momentum_30d": round(avg_mom, 2),
            "vix": vix,
        }

        sig = RegimeSignal(
            as_of=as_of,
            regime=regime,
            confidence=round(conf, 3),
            regime_score=round(regime_sc, 2),
            favored_asset_classes=fav,
            defensive_weight_adjustment=def_adj,
            breadth_score=round(b_score, 2),
            volatility_regime=vol_regime,
            notes=" ".join(notes_parts),
        )

        _upsert_regime(conn, sig, raw_inputs)
        return sig

    def load_latest(self, conn: sqlite3.Connection, as_of: str) -> Optional[RegimeSignal]:
        """Return the most recent cached signal on or before *as_of*."""
        cur = conn.cursor()
        cur.execute(
            """
            SELECT as_of, regime, confidence, regime_score,
                   favored_asset_classes_json, defensive_weight_adjustment,
                   breadth_score, volatility_regime, notes
            FROM engine_regime_snapshots
            WHERE as_of <= ?
            ORDER BY as_of DESC
            LIMIT 1
            """,
            (as_of,),
        )
        row = cur.fetchone()
        if row is None:
            return None
        (
            r_as_of, regime, confidence, regime_score,
            fav_json, def_adj, b_score, vol_regime, notes,
        ) = row
        return RegimeSignal(
            as_of=r_as_of,
            regime=regime,
            confidence=confidence,
            regime_score=regime_score,
            favored_asset_classes=json.loads(fav_json or "[]"),
            defensive_weight_adjustment=def_adj,
            breadth_score=b_score or 50.0,
            volatility_regime=vol_regime or "normal",
            notes=notes or "",
        )
