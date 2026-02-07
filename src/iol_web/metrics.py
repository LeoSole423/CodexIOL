from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any, Dict, Optional

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


def target_date(latest_date: str, days: int) -> str:
    d = date.fromisoformat(latest_date)
    return (d - timedelta(days=days)).isoformat()

