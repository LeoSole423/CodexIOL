from __future__ import annotations

from typing import Any, Dict, List


def _pick_meta(base: Dict[str, Any] | None, end: Dict[str, Any] | None, key: str):
    if end and end.get(key) not in (None, ""):
        return end.get(key)
    if base and base.get(key) not in (None, ""):
        return base.get(key)
    return None


def build_union_movers(base_assets: List[Dict[str, Any]], end_assets: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    base_by = {a.get("symbol"): a for a in (base_assets or []) if a.get("symbol")}
    end_by = {a.get("symbol"): a for a in (end_assets or []) if a.get("symbol")}
    symbols = set(base_by.keys()) | set(end_by.keys())

    out: List[Dict[str, Any]] = []
    for sym in symbols:
        b = base_by.get(sym)
        e = end_by.get(sym)

        base_total = float((b or {}).get("total_value") or 0.0)
        end_total = float((e or {}).get("total_value") or 0.0)
        delta = end_total - base_total
        pct = None if base_total == 0 else (delta / base_total * 100.0)

        out.append(
            {
                "symbol": sym,
                "description": _pick_meta(b, e, "description") or sym,
                "market": _pick_meta(b, e, "market"),
                "type": _pick_meta(b, e, "type"),
                "currency": _pick_meta(b, e, "currency"),
                "plazo": _pick_meta(b, e, "plazo"),
                # Value at end-of-period for display.
                "total_value": end_total,
                "base_total_value": base_total,
                "delta_value": delta,
                "delta_pct": pct,
            }
        )
    return out

