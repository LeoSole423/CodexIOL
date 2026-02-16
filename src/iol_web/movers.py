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


def build_union_movers_pnl(
    base_assets: List[Dict[str, Any]],
    end_assets: List[Dict[str, Any]],
    cashflows_by_symbol: Dict[str, Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """
    Period movers adjusted by trade cashflows.

    cashflows_by_symbol[symbol] expects keys:
      - buy_amount (float)
      - sell_amount (float)
    """
    base_by = {a.get("symbol"): a for a in (base_assets or []) if a.get("symbol")}
    end_by = {a.get("symbol"): a for a in (end_assets or []) if a.get("symbol")}
    symbols = set(base_by.keys()) | set(end_by.keys()) | set((cashflows_by_symbol or {}).keys())

    out: List[Dict[str, Any]] = []
    for sym in symbols:
        b = base_by.get(sym)
        e = end_by.get(sym)

        base_total = float((b or {}).get("total_value") or 0.0)
        end_total = float((e or {}).get("total_value") or 0.0)

        cf = (cashflows_by_symbol or {}).get(sym) or {}
        buys = float(cf.get("buy_amount") or 0.0)
        sells = float(cf.get("sell_amount") or 0.0)

        pnl = (end_total - base_total) + sells - buys
        exposure = base_total + buys
        pct = None if exposure == 0 else (pnl / exposure * 100.0)
        closed_position = (base_total > 0.0) and (end_total == 0.0)
        liquidated_to_cash = closed_position and (sells > 0.0)
        cashflow_missing_for_close = closed_position and (sells == 0.0)
        flow_tag = "none"
        if liquidated_to_cash:
            flow_tag = "liquidated"
        elif cashflow_missing_for_close:
            flow_tag = "missing_cashflow"

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
                # Keep frontend contract: delta_* used as the ranking/display metric.
                "delta_value": pnl,
                "delta_pct": pct,
                # Debug-friendly extras (safe to ignore in UI).
                "buy_amount": buys,
                "sell_amount": sells,
                "exposure": exposure,
                "closed_position": closed_position,
                "liquidated_to_cash": liquidated_to_cash,
                "cashflow_missing_for_close": cashflow_missing_for_close,
                "flow_tag": flow_tag,
            }
        )
    return out
