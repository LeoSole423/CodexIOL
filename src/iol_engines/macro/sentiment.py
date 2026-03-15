"""News sentiment aggregation from the advisor_evidence table.

Reuses _infer_stance_from_text() from iol_cli.evidence_fetch without
importing the entire module (to avoid circular deps).
The function is small enough to reproduce here with the same logic.
"""
from __future__ import annotations

import sqlite3
from typing import Optional


_BULLISH = (
    "upside", "upgrade", "buy", "outperform", "beat",
    "rally", "surge", "record high", "optimis", "bullish",
    "growth", "expansion", "recovery", "positive",
)
_BEARISH = (
    "downgrade", "sell", "underperform", "miss", "slump",
    "drop", "fall", "recession", "inflation", "crisis",
    "bear", "risk", "concern", "warning", "slow",
)
_MACRO_TERMS = (
    "inflation", "interest rate", "fed", "federal reserve", "gdp",
    "economy", "recession", "growth", "monetary", "bcra",
    "indec", "devaluation", "peso", "argentina", "macro",
)


def _stance(text: str) -> float:
    """Return +1 (bullish), -1 (bearish), or 0 (neutral)."""
    t = (text or "").lower()
    bulls = sum(1 for m in _BULLISH if m in t)
    bears = sum(1 for m in _BEARISH if m in t)
    if bulls > bears:
        return 1.0
    if bears > bulls:
        return -1.0
    return 0.0


def _is_macro_claim(text: str) -> bool:
    t = (text or "").lower()
    return any(term in t for term in _MACRO_TERMS)


def compute_macro_sentiment(conn: sqlite3.Connection, lookback_days: int = 30) -> Optional[float]:
    """Aggregate macro sentiment from recent advisor_evidence rows.

    Returns a score in [-100, +100]:
      - positive = net bullish macro headlines
      - negative = net bearish macro headlines
      - None = no relevant evidence found
    """
    cur = conn.cursor()
    cur.execute(
        """
        SELECT claim, confidence
        FROM advisor_evidence
        WHERE retrieved_at_utc >= date('now', ?)
          AND confidence IN ('high', 'medium')
        ORDER BY retrieved_at_utc DESC
        LIMIT 200
        """,
        (f"-{lookback_days} days",),
    )
    rows = cur.fetchall()

    weighted_sum = 0.0
    total_weight = 0.0
    for claim, confidence in rows:
        if not _is_macro_claim(claim):
            continue
        weight = 1.5 if confidence == "high" else 1.0
        weighted_sum += _stance(claim) * weight
        total_weight += weight

    if total_weight == 0:
        return None

    # Normalise to [-100, +100]
    raw = weighted_sum / total_weight   # in [-1, +1]
    return round(raw * 100, 1)
