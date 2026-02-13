from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any, Dict, Iterable, Optional

from .db import Snapshot


def _pct_change(base: float, quote: float) -> Optional[float]:
    if base == 0:
        return None
    return (quote - base) / base * 100.0


@dataclass(frozen=True)
class ReturnBlock:
    from_date: Optional[str]
    to_date: Optional[str]
    delta: Optional[float]
    pct: Optional[float]

    def to_dict(self) -> Dict[str, Any]:
        return {"from": self.from_date, "to": self.to_date, "delta": self.delta, "pct": self.pct}


def compute_return(latest: Optional[Snapshot], base: Optional[Snapshot]) -> ReturnBlock:
    if not latest or not base:
        return ReturnBlock(
            from_date=base.snapshot_date if base else None,
            to_date=latest.snapshot_date if latest else None,
            delta=None,
            pct=None,
        )
    base_v = float(base.total_value or 0.0)
    latest_v = float(latest.total_value or 0.0)
    return ReturnBlock(
        from_date=base.snapshot_date,
        to_date=latest.snapshot_date,
        delta=latest_v - base_v,
        pct=_pct_change(base_v, latest_v),
    )

def compute_daily_return_from_assets(latest: Optional[Snapshot], assets: Iterable[Dict[str, Any]]) -> ReturnBlock:
    """
    Fallback for "Delta diario" when there is only one snapshot in the DB.

    Uses IOL-provided per-asset daily variation (daily_var_pct) on the latest snapshot to build a
    portfolio-level delta for titles. This does not require a previous snapshot.
    """
    if not latest:
        return compute_return(None, None)

    delta = 0.0
    denom_assets = 0.0
    for a in assets or []:
        try:
            v = float(a.get("total_value") or 0.0)
        except Exception:
            v = 0.0
        pct = a.get("daily_var_pct")
        if pct is None:
            continue
        try:
            pct_f = float(pct)
        except Exception:
            continue
        delta += v * pct_f / 100.0
        denom_assets += v

    denom = None
    if latest.titles_value is not None:
        try:
            denom = float(latest.titles_value)
        except Exception:
            denom = None
    if denom is None:
        denom = denom_assets

    pct_out = None
    if denom:
        pct_out = delta / denom * 100.0

    return ReturnBlock(
        from_date=latest.snapshot_date,
        to_date=latest.snapshot_date,
        delta=delta,
        pct=pct_out,
    )


def target_date(latest_date: str, days: int) -> str:
    d = date.fromisoformat(latest_date)
    return (d - timedelta(days=days)).isoformat()
