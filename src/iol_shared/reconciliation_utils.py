from __future__ import annotations

from typing import Any, Dict, List, Optional, Protocol


class CashSnapshotLike(Protocol):
    cash_total_ars: Optional[float]
    cash_disponible_ars: Optional[float]
    cash_disponible_usd: Optional[float]


def snapshot_cash_ars(snap: Optional[CashSnapshotLike]) -> Optional[float]:
    if not snap:
        return None
    if snap.cash_total_ars is not None:
        try:
            return float(snap.cash_total_ars)
        except Exception:
            return None
    if snap.cash_disponible_ars is not None:
        try:
            return float(snap.cash_disponible_ars)
        except Exception:
            return None
    return None


def snapshot_cash_components(snap: Optional[CashSnapshotLike]) -> Dict[str, Optional[float]]:
    if not snap:
        return {"cash_total_ars": None, "cash_ars": None, "cash_usd": None}
    cash_total = snapshot_cash_ars(snap)
    cash_ars = None
    cash_usd = None
    try:
        if snap.cash_disponible_ars is not None:
            cash_ars = float(snap.cash_disponible_ars)
    except Exception:
        cash_ars = None
    try:
        if snap.cash_disponible_usd is not None:
            cash_usd = float(snap.cash_disponible_usd)
    except Exception:
        cash_usd = None
    return {"cash_total_ars": cash_total, "cash_ars": cash_ars, "cash_usd": cash_usd}


def implied_fx_ars_per_usd(
    cash_total_ars: Optional[float],
    cash_ars: Optional[float],
    cash_usd: Optional[float],
) -> Optional[float]:
    try:
        if cash_total_ars is None or cash_ars is None or cash_usd is None:
            return None
        usd = float(cash_usd)
        if abs(usd) <= 1e-9:
            return None
        return (float(cash_total_ars) - float(cash_ars)) / usd
    except Exception:
        return None


def norm_currency(v: Any) -> str:
    s = str(v or "").strip().upper()
    if s in ("ARS", "PESO_ARGENTINO", "PESO ARGENTINO", "PESOS", "$", "AR$"):
        return "ARS"
    if s in ("USD", "US$", "U$S", "DOLAR", "DOLAR_ESTADOUNIDENSE", "DOLAR ESTADOUNIDENSE"):
        return "USD"
    if not s:
        return "ARS"
    return s


def norm_movement_kind(v: Any) -> str:
    return str(v or "").strip().lower() or "correction_unknown"


def movement_amount_to_ars(
    movement: Dict[str, Any],
    fx_end_ars_per_usd: Optional[float],
    warnings: List[str],
) -> Optional[float]:
    try:
        amount_f = float(movement.get("amount"))
    except Exception:
        warnings.append("MOVEMENTS_AMOUNT_INVALID")
        return None
    ccy = norm_currency(movement.get("currency"))
    if ccy == "ARS":
        return amount_f
    if ccy == "USD":
        if fx_end_ars_per_usd is None:
            warnings.append("MOVEMENTS_USD_NO_FX")
            return None
        return amount_f * float(fx_end_ars_per_usd)
    warnings.append("MOVEMENTS_CURRENCY_UNSUPPORTED")
    return None


def aggregate_imported_movements(
    rows: List[Dict[str, Any]],
    fx_end_ars_per_usd: Optional[float],
) -> Dict[str, Any]:
    imported_external = 0.0
    imported_internal = 0.0
    imported_dividend = 0.0
    imported_fee = 0.0
    imported_count = 0
    warnings: List[str] = []
    for movement in rows:
        kind = norm_movement_kind(movement.get("kind"))
        amt_ars = movement_amount_to_ars(movement, fx_end_ars_per_usd, warnings)
        if amt_ars is None:
            continue
        imported_count += 1
        if kind in ("external_deposit", "external_withdraw"):
            imported_external += float(amt_ars)
        else:
            imported_internal += float(amt_ars)
            if kind == "dividend_or_coupon_income":
                imported_dividend += float(amt_ars)
            if kind == "operational_fee_or_tax":
                imported_fee += float(amt_ars)

    return {
        "rows_count": int(imported_count),
        "imported_external_ars": float(imported_external),
        "imported_internal_ars": float(imported_internal),
        "imported_dividend_ars": float(imported_dividend),
        "imported_fee_ars": float(imported_fee),
        "warnings": list(dict.fromkeys(warnings)),
    }
