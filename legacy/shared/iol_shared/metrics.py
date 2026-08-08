from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any, Dict, Iterable, List, Optional, Protocol


class SnapshotLike(Protocol):
    snapshot_date: str
    total_value: float
    titles_value: Optional[float]


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


@dataclass(frozen=True)
class EnrichedReturnBlock:
    from_date: Optional[str]
    to_date: Optional[str]
    delta: Optional[float]
    pct: Optional[float]
    real_delta: Optional[float]
    real_pct: Optional[float]
    flow_inferred_ars: Optional[float]
    flow_manual_adjustment_ars: Optional[float]
    flow_total_ars: Optional[float]
    quality_warnings: List[str]
    orders_stats: Optional[Dict[str, int]]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "from": self.from_date,
            "to": self.to_date,
            "delta": self.delta,
            "pct": self.pct,
            "real_delta": self.real_delta,
            "real_pct": self.real_pct,
            "flow_inferred_ars": self.flow_inferred_ars,
            "flow_manual_adjustment_ars": self.flow_manual_adjustment_ars,
            "flow_total_ars": self.flow_total_ars,
            "quality_warnings": list(self.quality_warnings or []),
            "orders_stats": self.orders_stats,
        }


def compute_return(latest: Optional[SnapshotLike], base: Optional[SnapshotLike]) -> ReturnBlock:
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


def enrich_return_block(
    gross: ReturnBlock,
    base: Optional[SnapshotLike],
    flow_inferred_ars: Optional[float],
    flow_manual_adjustment_ars: Optional[float],
    quality_warnings: Optional[Iterable[str]] = None,
    orders_stats: Optional[Dict[str, int]] = None,
    fallback_real_pct: Optional[float] = None,
) -> EnrichedReturnBlock:
    flow_inferred = None if flow_inferred_ars is None else float(flow_inferred_ars)
    flow_manual = None if flow_manual_adjustment_ars is None else float(flow_manual_adjustment_ars)
    flow_total = None
    if flow_inferred is not None or flow_manual is not None:
        flow_total = float(flow_inferred or 0.0) + float(flow_manual or 0.0)

    real_delta = None if gross.delta is None else float(gross.delta) - float(flow_total or 0.0)
    real_pct = None
    base_v = float(base.total_value or 0.0) if base else 0.0
    if real_delta is not None and base and base_v != 0.0:
        real_pct = (real_delta / base_v) * 100.0
    elif real_delta is not None and fallback_real_pct is not None:
        real_pct = float(fallback_real_pct)

    return EnrichedReturnBlock(
        from_date=gross.from_date,
        to_date=gross.to_date,
        delta=gross.delta,
        pct=gross.pct,
        real_delta=real_delta,
        real_pct=real_pct,
        flow_inferred_ars=flow_inferred,
        flow_manual_adjustment_ars=flow_manual,
        flow_total_ars=flow_total,
        quality_warnings=list(quality_warnings or []),
        orders_stats=orders_stats,
    )


def compute_daily_return_from_assets(latest: Optional[SnapshotLike], assets: Iterable[Dict[str, Any]]) -> ReturnBlock:
    if not latest:
        return compute_return(None, None)

    delta = 0.0
    denom_assets = 0.0
    for a in assets or []:
        try:
            value = float(a.get("total_value") or 0.0)
        except Exception:
            value = 0.0
        pct = a.get("daily_var_pct")
        if pct is None:
            continue
        try:
            pct_f = float(pct)
        except Exception:
            continue
        delta += value * pct_f / 100.0
        denom_assets += value

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
