from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse

from .api_cashflows import build_cashflows_router
from .api_quality import build_quality_router
from iol_shared.reconciliation_utils import (
    aggregate_imported_movements as shared_aggregate_imported_movements,
    implied_fx_ars_per_usd,
    norm_currency,
    snapshot_cash_ars,
    snapshot_cash_components,
)
from .api_advisor import advisor_history, advisor_latest, advisor_opportunities_latest, router as advisor_router
from .api_inflation import build_inflation_router
from .api_portfolio import allocation, assets_performance, latest, movers, router as portfolio_router
from .api_reconciliation import (
    reconciliation_apply,
    reconciliation_dismiss,
    reconciliation_interval,
    reconciliation_latest,
    reconciliation_open,
    router as reconciliation_router,
)
from . import db as dbmod
from .inflation_ar import get_inflation_series
from .metrics import compute_daily_return_from_assets, compute_return, enrich_return_block, target_date


router = APIRouter(prefix="/api")
router.include_router(advisor_router)
router.include_router(portfolio_router)
router.include_router(reconciliation_router)


def _parse_date(v: Optional[str]) -> Optional[str]:
    if v is None:
        return None
    v = v.strip()
    if not v:
        return None
    date.fromisoformat(v)
    return v


def _snapshot_cash_ars(snap: Optional[dbmod.Snapshot]) -> Optional[float]:
    return snapshot_cash_ars(snap)


EXTERNAL_DISPLAY_KINDS = {"external_deposit_probable", "external_withdraw_probable"}


def _norm_movement_kind(v: Any) -> str:
    s = str(v or "").strip().lower()
    if not s:
        return "correction_unknown"
    return s


def _snapshot_cash_components(snap: Optional[dbmod.Snapshot]) -> Dict[str, Optional[float]]:
    return snapshot_cash_components(snap)


def _movement_amount_to_ars(
    movement: Dict[str, Any],
    fx_end_ars_per_usd: Optional[float],
    warnings: List[str],
) -> Optional[float]:
    amount = movement.get("amount")
    try:
        amount_f = float(amount)
    except Exception:
        warnings.append("MOVEMENTS_AMOUNT_INVALID")
        return None
    ccy = _norm_currency(movement.get("currency"))
    if ccy == "ARS":
        return amount_f
    if ccy == "USD":
        if fx_end_ars_per_usd is None:
            warnings.append("MOVEMENTS_USD_NO_FX")
            return None
        return amount_f * float(fx_end_ars_per_usd)
    warnings.append("MOVEMENTS_CURRENCY_UNSUPPORTED")
    return None


def _aggregate_imported_movements(
    conn,
    base_date_exclusive: str,
    end_date_inclusive: str,
    fx_end_ars_per_usd: Optional[float],
) -> Dict[str, Any]:
    rows = dbmod.list_account_cash_movements(conn, base_date_exclusive, end_date_inclusive)
    return shared_aggregate_imported_movements(rows, fx_end_ars_per_usd)


def _snapshot_data_freshness(snap: Optional[dbmod.Snapshot]) -> Dict[str, Any]:
    if not snap:
        return {"status": "missing", "days_stale": None, "snapshot_date": None, "retrieved_at": None}
    days_stale = None
    try:
        days_stale = max(0, (date.today() - date.fromisoformat(snap.snapshot_date)).days)
    except Exception:
        days_stale = None
    status = "fresh"
    if days_stale is None:
        status = "unknown"
    elif days_stale > 3:
        status = "stale"
    elif days_stale > 1:
        status = "aging"
    return {
        "status": status,
        "days_stale": days_stale,
        "snapshot_date": snap.snapshot_date,
        "retrieved_at": snap.retrieved_at,
    }


def _orders_coverage_payload(stats: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    s = dict(stats or {})
    total = int(s.get("total", 0) or 0)
    ignored = int(s.get("ignored", 0) or 0)
    effective_total = max(0, total - ignored)
    classified = int(s.get("classified", 0) or 0)
    coverage_pct = (float(classified) / float(effective_total) * 100.0) if effective_total > 0 else 0.0
    status = "none"
    if effective_total <= 0:
        status = "none"
    elif int(s.get("unclassified", 0) or 0) > 0 or int(s.get("amount_missing", 0) or 0) > 0:
        status = "partial"
    else:
        status = "full"
    out = dict(s)
    out.update({"effective_total": effective_total, "coverage_pct": coverage_pct, "status": status})
    return out


def _movements_coverage_payload(imported_rows_count: int, warnings: Optional[List[str]] = None) -> Dict[str, Any]:
    warns = set(str(w) for w in (warnings or []))
    blocking_warns = warns - {"ORDERS_NONE"}
    status = "none"
    if imported_rows_count > 0 and not blocking_warns:
        status = "imported"
    elif imported_rows_count > 0:
        status = "partial"
    return {
        "rows_count": int(imported_rows_count or 0),
        "warnings": sorted(warns),
        "status": status,
    }


def _flow_confidence_from_inputs(
    *,
    base: Optional[dbmod.Snapshot],
    warnings: Optional[List[str]],
    orders_stats: Optional[Dict[str, Any]],
    imported_rows_count: int,
) -> str:
    warns = set(str(w) for w in (warnings or []))
    if not base:
        return "low"
    if {"CASH_MISSING", "ORDERS_INCOMPLETE", "INFERENCE_PARTIAL"} & warns:
        return "low"
    if int(imported_rows_count or 0) > 0:
        return "high"
    order_cov = _orders_coverage_payload(orders_stats)
    if str(order_cov.get("status")) == "full" and int(order_cov.get("effective_total") or 0) > 0:
        return "medium"
    return "low"


def _decorate_return_payload(
    payload: Dict[str, Any],
    *,
    latest: Optional[dbmod.Snapshot],
    base: Optional[dbmod.Snapshot],
    interval_meta: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    out = dict(payload or {})
    warnings = list(out.get("quality_warnings") or [])
    order_stats = out.get("orders_stats")
    imported_rows_count = int((interval_meta or {}).get("_imported_rows_count") or 0)
    flow_confidence = _flow_confidence_from_inputs(
        base=base,
        warnings=warnings,
        orders_stats=order_stats,
        imported_rows_count=imported_rows_count,
    )
    out["flow_confidence"] = flow_confidence
    out["estimated"] = bool(flow_confidence != "high")
    out["data_freshness"] = _snapshot_data_freshness(latest)
    out["orders_coverage"] = _orders_coverage_payload(order_stats)
    out["movements_coverage"] = _movements_coverage_payload(imported_rows_count, warnings)
    out["flow_breakdown"] = {
        "gross_delta_ars": out.get("delta"),
        "market_delta_ars": out.get("real_delta") if out.get("real_delta") is not None else out.get("delta"),
        "external_flow_ars": out.get("flow_total_ars"),
        "inferred_external_flow_ars": out.get("flow_inferred_ars"),
        "manual_adjustment_ars": out.get("flow_manual_adjustment_ars"),
        "fx_revaluation_ars": (interval_meta or {}).get("fx_revaluation_ars"),
        "imported_external_ars": (interval_meta or {}).get("imported_external_ars"),
        "imported_internal_ars": (interval_meta or {}).get("imported_internal_ars"),
    }
    return out


def _compute_interval_flow_v2(
    conn,
    base_snap: dbmod.Snapshot,
    end_snap: dbmod.Snapshot,
    include_threshold: bool,
) -> Optional[Dict[str, Any]]:
    warnings: List[str] = []
    base_cash = _snapshot_cash_components(base_snap)
    end_cash = _snapshot_cash_components(end_snap)

    cash_total_base = base_cash.get("cash_total_ars")
    cash_total_end = end_cash.get("cash_total_ars")
    if cash_total_base is None or cash_total_end is None:
        cash_total_delta = 0.0
        warnings.append("CASH_MISSING")
    else:
        cash_total_delta = float(cash_total_end) - float(cash_total_base)

    cash_ars_base = base_cash.get("cash_ars")
    cash_ars_end = end_cash.get("cash_ars")
    cash_usd_base = base_cash.get("cash_usd")
    cash_usd_end = end_cash.get("cash_usd")
    cash_ars_delta = None
    cash_usd_delta = None
    if cash_ars_base is not None and cash_ars_end is not None:
        cash_ars_delta = float(cash_ars_end) - float(cash_ars_base)
    if cash_usd_base is not None and cash_usd_end is not None:
        cash_usd_delta = float(cash_usd_end) - float(cash_usd_base)

    fx_base = implied_fx_ars_per_usd(cash_total_base, cash_ars_base, cash_usd_base)
    fx_end = implied_fx_ars_per_usd(cash_total_end, cash_ars_end, cash_usd_end)
    if fx_base is not None and fx_end is not None and cash_usd_base is not None:
        fx_revaluation_ars = float(cash_usd_base) * (float(fx_end) - float(fx_base))
    else:
        fx_revaluation_ars = 0.0

    dt_from = f"{base_snap.snapshot_date}T23:59:59"
    dt_to = f"{end_snap.snapshot_date}T23:59:59"
    amounts, stats = dbmod.orders_flow_summary(conn, dt_from, dt_to, currency="peso_Argentino")
    if stats.get("unclassified", 0) > 0 or stats.get("amount_missing", 0) > 0:
        warnings.append("ORDERS_INCOMPLETE")

    buy_amount = float(amounts.get("buy_amount") or 0.0)
    sell_amount = float(amounts.get("sell_amount") or 0.0)
    income_amount = float(amounts.get("income_amount") or 0.0)
    fee_amount = float(amounts.get("fee_amount") or 0.0)
    order_fee_internal_ars = -abs(float(fee_amount or 0.0))

    imported = _aggregate_imported_movements(conn, base_snap.snapshot_date, end_snap.snapshot_date, fx_end)
    for w in imported.get("warnings") or []:
        warnings.append(str(w))

    external_raw = float(cash_total_delta) + buy_amount - sell_amount - income_amount
    imported_internal = float(imported.get("imported_internal_ars") or 0.0)
    imported_external = float(imported.get("imported_external_ars") or 0.0)
    external_adjusted = external_raw - float(fx_revaluation_ars) - imported_internal - order_fee_internal_ars
    external_final = imported_external if abs(imported_external) > 1e-9 else external_adjusted

    traded_gross = abs(buy_amount) + abs(sell_amount) + abs(income_amount) + abs(fee_amount)
    residual_ratio = (abs(external_final) / traded_gross) if traded_gross > 0 else None

    has_imported = int(imported.get("rows_count") or 0) > 0
    if include_threshold and (abs(external_final) < 100.0) and (abs(fx_revaluation_ars) < 100.0) and (not has_imported):
        return None

    if "CASH_MISSING" in warnings or "ORDERS_INCOMPLETE" in warnings:
        kind = "correction"
    elif external_final > 0:
        kind = "deposit"
    elif external_final < 0:
        kind = "withdraw"
    else:
        kind = "correction"

    return {
        "flow_date": end_snap.snapshot_date,
        "kind": kind,
        "amount_ars": float(external_final),
        "base_snapshot": base_snap.snapshot_date,
        "end_snapshot": end_snap.snapshot_date,
        "cash_delta_ars": float(cash_total_delta),
        "cash_total_delta_ars": float(cash_total_delta),
        "cash_ars_delta": cash_ars_delta,
        "cash_usd_delta": cash_usd_delta,
        "buy_amount_ars": buy_amount,
        "sell_amount_ars": sell_amount,
        "income_amount_ars": income_amount,
        "fee_amount_ars": fee_amount,
        "external_raw_ars": float(external_raw),
        "external_adjusted_ars": float(external_adjusted),
        "external_final_ars": float(external_final),
        "fx_revaluation_ars": float(fx_revaluation_ars),
        "imported_internal_ars": float(imported_internal),
        "imported_external_ars": float(imported_external),
        "quality_warnings": list(dict.fromkeys(warnings)),
        "orders_stats": stats,
        "residual_ratio": residual_ratio,
        "_traded_gross": traded_gross,
        "_has_imported_movements": has_imported,
        "_imported_rows_count": int(imported.get("rows_count") or 0),
        "_imported_dividend_ars": float(imported.get("imported_dividend_ars") or 0.0),
        "_imported_fee_ars": float(imported.get("imported_fee_ars") or 0.0),
        "_orders_total": int(stats.get("total", 0) or 0),
    }


def _return_with_flows(
    conn,
    latest: Optional[dbmod.Snapshot],
    base: Optional[dbmod.Snapshot],
    gross_block,
):
    if not latest:
        payload = enrich_return_block(
            gross=gross_block,
            base=base,
            flow_inferred_ars=None,
            flow_manual_adjustment_ars=None,
            quality_warnings=["INFERENCE_PARTIAL"],
            orders_stats=None,
        ).to_dict()
        return _decorate_return_payload(payload, latest=latest, base=base, interval_meta=None)

    # One-snapshot fallback (no base): keep useful daily estimate, mark as partial.
    if not base:
        payload = enrich_return_block(
            gross=gross_block,
            base=base,
            flow_inferred_ars=0.0,
            flow_manual_adjustment_ars=0.0,
            quality_warnings=["INFERENCE_PARTIAL"],
            orders_stats=None,
            fallback_real_pct=gross_block.pct,
        ).to_dict()
        return _decorate_return_payload(payload, latest=latest, base=base, interval_meta=None)

    warnings = []
    iv = _compute_interval_flow_v2(conn, base, latest, include_threshold=False)
    if iv is None:
        order_stats = None
        warnings.append("INFERENCE_PARTIAL")
        flow_inferred = 0.0
    else:
        order_stats = iv.get("orders_stats")
        warnings.extend(list(iv.get("quality_warnings") or []))
        if (order_stats or {}).get("total", 0) == 0:
            warnings.append("ORDERS_NONE")
        flow_inferred = float(iv.get("external_final_ars") or iv.get("amount_ars") or 0.0)
    flow_manual = dbmod.manual_cashflow_sum(conn, base.snapshot_date, latest.snapshot_date)
    payload = enrich_return_block(
        gross=gross_block,
        base=base,
        flow_inferred_ars=flow_inferred,
        flow_manual_adjustment_ars=flow_manual,
        quality_warnings=list(dict.fromkeys(warnings)),
        orders_stats=order_stats,
    ).to_dict()
    return _decorate_return_payload(payload, latest=latest, base=base, interval_meta=iv)


def _flow_quality_incomplete(row: Dict[str, Any]) -> bool:
    warns = set(str(w) for w in (row.get("quality_warnings") or []))
    return ("CASH_MISSING" in warns) or ("ORDERS_INCOMPLETE" in warns)


def _flow_date_or_none(v: Any) -> Optional[date]:
    try:
        return date.fromisoformat(str(v or ""))
    except Exception:
        return None


def _annotate_flow_rows(rows: List[Dict[str, Any]]) -> None:
    """
    Enrich rows in-place with display classification for inferred flows (v2 taxonomy).
    """
    for row in rows:
        if "_traded_gross" not in row:
            b = abs(float(row.get("buy_amount_ars") or 0.0))
            s = abs(float(row.get("sell_amount_ars") or 0.0))
            i = abs(float(row.get("income_amount_ars") or 0.0))
            f = abs(float(row.get("fee_amount_ars") or 0.0))
            row["_traded_gross"] = b + s + i + f
        if row.get("residual_ratio") is None:
            tg = float(row.get("_traded_gross") or 0.0)
            amt = abs(float(row.get("external_final_ars") or row.get("amount_ars") or 0.0))
            row["residual_ratio"] = (amt / tg) if tg > 0 else None

    pair_by_idx: Dict[int, int] = {}
    candidates: List[Tuple[float, int, int, int]] = []
    for i in range(len(rows)):
        ri = rows[i]
        if _flow_quality_incomplete(ri):
            continue
        ai = float(ri.get("external_final_ars") or ri.get("amount_ars") or 0.0)
        if abs(ai) <= 1e-9:
            continue
        di = _flow_date_or_none(ri.get("flow_date"))
        if di is None:
            continue
        ti = float(ri.get("_traded_gross") or 0.0)

        for j in range(i + 1, len(rows)):
            rj = rows[j]
            if _flow_quality_incomplete(rj):
                continue
            aj = float(rj.get("external_final_ars") or rj.get("amount_ars") or 0.0)
            if abs(aj) <= 1e-9 or (ai * aj) >= 0:
                continue
            dj = _flow_date_or_none(rj.get("flow_date"))
            if dj is None:
                continue
            delta_days = abs((dj - di).days)
            if delta_days > 2:
                continue

            tj = float(rj.get("_traded_gross") or 0.0)
            mag_similarity = abs(abs(ai) - abs(aj)) / max(abs(ai), abs(aj))
            pair_net_ratio = abs(ai + aj) / (abs(ai) + abs(aj))
            denom = abs(ai) + abs(aj)
            pair_trade_coverage = ((ti + tj) / denom) if denom > 0 else 0.0
            if mag_similarity <= 0.25 and pair_net_ratio <= 0.20 and pair_trade_coverage >= 0.75:
                score = pair_net_ratio + mag_similarity
                candidates.append((score, delta_days, i, j))

    candidates.sort(key=lambda it: (it[0], it[1], it[2], it[3]))
    used_idx = set()
    for _, _, i, j in candidates:
        if i in used_idx or j in used_idx:
            continue
        pair_by_idx[i] = j
        pair_by_idx[j] = i
        used_idx.add(i)
        used_idx.add(j)

    # Settlement carryover smoothing:
    # When adjacent opposite-sign rows look like a liquidation offset
    # (one side traded, the other side no trades), collapse both rows
    # to a stabilized net amount on the traded side and zero on the carryover side.
    settlement_pair_by_idx: Dict[int, int] = {}
    amount_override_by_idx: Dict[int, float] = {}
    settlement_used_idx = set()
    for i in range(len(rows) - 1):
        j = i + 1
        if i in used_idx or j in used_idx or i in settlement_used_idx or j in settlement_used_idx:
            continue

        ri = rows[i]
        rj = rows[j]
        if _flow_quality_incomplete(ri) or _flow_quality_incomplete(rj):
            continue

        ai = float(ri.get("external_final_ars") or ri.get("amount_ars") or 0.0)
        aj = float(rj.get("external_final_ars") or rj.get("amount_ars") or 0.0)
        if abs(ai) <= 1e-9 or abs(aj) <= 1e-9 or (ai * aj) >= 0:
            continue

        di = _flow_date_or_none(ri.get("flow_date"))
        dj = _flow_date_or_none(rj.get("flow_date"))
        if di is None or dj is None:
            continue
        delta_days = (dj - di).days
        if delta_days < 1 or delta_days > 3:
            continue

        ti = float(ri.get("_traded_gross") or 0.0)
        tj = float(rj.get("_traded_gross") or 0.0)
        i_traded = ti > 1e-9
        j_traded = tj > 1e-9
        if i_traded == j_traded:
            continue

        max_abs = max(abs(ai), abs(aj))
        if max_abs < 100.0:
            continue

        near_cancel = abs(ai + aj) <= 0.08 * (abs(ai) + abs(aj))
        traded_side = ti if i_traded else tj
        double_count_like = abs(abs(ai + aj) - traded_side) <= max(5000.0, 0.20 * traded_side)
        if not (near_cancel or double_count_like):
            continue

        anchor = i if i_traded else j
        carry = j if i_traded else i
        amount_override_by_idx[anchor] = ai + aj
        amount_override_by_idx[carry] = 0.0
        settlement_pair_by_idx[anchor] = carry
        settlement_pair_by_idx[carry] = anchor
        settlement_used_idx.add(anchor)
        settlement_used_idx.add(carry)

    for i, row in enumerate(rows):
        kind = str(row.get("kind") or "").lower()
        amount = float(row.get("external_final_ars") or row.get("amount_ars") or 0.0)
        if i in amount_override_by_idx:
            amount = float(amount_override_by_idx[i])
            row["amount_ars"] = amount
            row["external_final_ars"] = amount
            if abs(amount) <= 1e-9:
                row["kind"] = "correction"
            else:
                row["kind"] = "deposit" if amount > 0 else "withdraw"
            kind = str(row.get("kind") or "").lower()
        residual_ratio = row.get("residual_ratio")
        imported_external = float(row.get("imported_external_ars") or 0.0)
        imported_internal = float(row.get("imported_internal_ars") or 0.0)
        imported_dividend = float(row.get("_imported_dividend_ars") or 0.0)
        imported_fee = float(row.get("_imported_fee_ars") or 0.0)
        fx_revaluation = float(row.get("fx_revaluation_ars") or 0.0)
        income_amount = float(row.get("income_amount_ars") or 0.0)
        fee_amount = float(row.get("fee_amount_ars") or 0.0)
        orders_total = int(row.get("_orders_total") or 0)
        has_imported = bool(row.get("_has_imported_movements"))

        display_kind = "external_deposit_probable" if amount >= 0 else "external_withdraw_probable"
        display_label = "Flujo externo probable (+)" if amount >= 0 else "Flujo externo probable (-)"
        confidence = "medium"
        reason_code = "EXTERNAL_FINAL_SIGN"
        reason_detail = "Clasificado por signo del flujo externo final (v2)."
        paired_flow_date = None
        paired_amount_ars = None

        if _flow_quality_incomplete(row):
            display_kind = "correction_unknown"
            display_label = "Correcci\u00f3n"
            confidence = "high"
            reason_code = "QUALITY_INCOMPLETE"
            reason_detail = "Datos incompletos de caja/\u00f3rdenes; revisar manualmente."
        elif i in settlement_pair_by_idx:
            j = settlement_pair_by_idx[i]
            paired = rows[j]
            paired_flow_date = paired.get("flow_date")
            paired_amount_ars = float(amount_override_by_idx.get(j, float(paired.get("amount_ars") or 0.0)))
            if abs(amount) <= 1e-9:
                display_kind = "settlement_carryover"
                display_label = "Liquidaci\u00f3n compensada"
                confidence = "medium"
                reason_code = "SETTLEMENT_CARRYOVER"
                reason_detail = (
                    f"Compensado por liquidaci\u00f3n cercana con {paired_flow_date}; no se aplica como flujo externo."
                    if paired_flow_date
                    else "Compensado por liquidaci\u00f3n cercana; no se aplica como flujo externo."
                )
            else:
                display_kind = "external_deposit_probable" if amount >= 0 else "external_withdraw_probable"
                display_label = "Flujo externo probable (+)" if amount >= 0 else "Flujo externo probable (-)"
                confidence = "medium"
                reason_code = "SETTLEMENT_SMOOTHED"
                reason_detail = (
                    f"Flujo suavizado por compensaci\u00f3n de liquidaci\u00f3n cercana con {paired_flow_date}."
                    if paired_flow_date
                    else "Flujo suavizado por compensaci\u00f3n de liquidaci\u00f3n cercana."
                )
        elif i in pair_by_idx:
            j = pair_by_idx[i]
            paired = rows[j]
            paired_flow_date = paired.get("flow_date")
            paired_amount_ars = float(paired.get("amount_ars") or 0.0)
            display_kind = "rotation_internal"
            display_label = "Rotaci\u00f3n interna probable"
            confidence = "medium"
            reason_code = "ROTATION_PAIR"
            reason_detail = (
                f"Par opuesto cercano con {paired_flow_date}; neto combinado bajo."
                if paired_flow_date
                else "Par opuesto cercano; neto combinado bajo."
            )
        elif abs(imported_external) > 1e-9:
            display_kind = "external_deposit_probable" if amount >= 0 else "external_withdraw_probable"
            display_label = "Flujo externo importado (+)" if amount >= 0 else "Flujo externo importado (-)"
            confidence = "high"
            reason_code = "IMPORTED_EXTERNAL_PRIORITY"
            reason_detail = "Se prioriza movimiento externo expl\u00edcito importado."
        elif (
            abs(amount) < 100.0
            and abs(fx_revaluation) >= 100.0
            and orders_total == 0
            and abs(imported_external) <= 1e-9
        ):
            display_kind = "fx_revaluation_usd_cash"
            display_label = "Revaluaci\u00f3n FX de caja USD"
            confidence = "high"
            reason_code = "FX_REVALUATION_USD_CASH"
            reason_detail = "Movimiento explicado por variaci\u00f3n de tipo de cambio sobre caja USD."
        elif abs(amount) < 250.0 and (income_amount > 1e-9 or imported_dividend > 1e-9):
            display_kind = "dividend_or_coupon_income"
            display_label = "Dividendo/Renta probable"
            confidence = "high" if (imported_dividend > 1e-9) else "medium"
            reason_code = "DIVIDEND_OR_COUPON"
            reason_detail = "Ingreso interno por dividendos/rentas; no se aplica como flujo externo."
        elif (
            fee_amount > 1e-9
            or abs(imported_fee) > 1e-9
            or (
                kind == "withdraw"
                and isinstance(residual_ratio, (int, float))
                and float(residual_ratio) <= 0.03
                and abs(imported_external) <= 1e-9
            )
        ):
            display_kind = "operational_fee_or_tax"
            display_label = "Costo/Impuesto operativo"
            confidence = "medium"
            reason_code = "OPERATIONAL_FEE_OR_TAX"
            reason_detail = "Salida interna por costos/impuestos operativos."
        elif abs(amount) < 100.0 and has_imported and abs(imported_internal) > 1e-9 and abs(imported_external) <= 1e-9:
            display_kind = "correction_unknown"
            display_label = "Correcci\u00f3n"
            confidence = "medium"
            reason_code = "IMPORTED_INTERNAL_NEUTRALIZED"
            reason_detail = "Movimiento interno importado compensado; sin flujo externo neto relevante."

        row["display_kind"] = display_kind
        row["display_label"] = display_label
        row["confidence"] = confidence
        row["reason_code"] = reason_code
        row["reason_detail"] = reason_detail
        row["paired_flow_date"] = paired_flow_date
        row["paired_amount_ars"] = paired_amount_ars
        row.pop("_traded_gross", None)
        row.pop("_has_imported_movements", None)
        row.pop("_imported_dividend_ars", None)
        row.pop("_imported_fee_ars", None)
        row.pop("_orders_total", None)


cashflows_router, cashflows_auto, cashflows_manual, cashflows_manual_add, cashflows_manual_delete = build_cashflows_router(
    parse_date=_parse_date,
    compute_interval_flow=_compute_interval_flow_v2,
    annotate_flow_rows=_annotate_flow_rows,
)
router.include_router(cashflows_router)


@router.get("/health")
def health() -> Dict[str, Any]:
    return {"ok": True}


@router.get("/snapshots")
def snapshots(
    date_from: Optional[str] = Query(None, alias="from"),
    date_to: Optional[str] = Query(None, alias="to"),
    mode: str = Query("raw"),
):
    try:
        f = _parse_date(date_from)
        t = _parse_date(date_to)
        m = (mode or "raw").strip().lower()
        if m not in ("raw", "market"):
            return JSONResponse(status_code=400, content={"error": "mode must be raw|market"})
        conn = dbmod.get_conn()
    except FileNotFoundError:
        return []

    try:
        rows = dbmod.snapshots_series(conn, f, t)
        if m == "raw":
            return [{"date": d, "total_value": v} for d, v in rows]

        # "market" mode: de-bias series by subtracting inferred external flows
        # on each interval (same inference base logic used in returns, plus
        # classification to avoid netting internal rotation as external flow).
        if not rows:
            return []

        intervals: List[Dict[str, Any]] = []
        for idx in range(1, len(rows)):
            base_d, base_v = rows[idx - 1]
            end_d, end_v = rows[idx]
            base_snap = dbmod.snapshot_on_or_before(conn, base_d)
            end_snap = dbmod.snapshot_on_or_before(conn, end_d)
            gross_delta = float(end_v or 0.0) - float(base_v or 0.0)
            if not base_snap or not end_snap:
                intervals.append(
                    {
                        "flow_date": end_d,
                        "gross_delta": gross_delta,
                        "amount_ars": 0.0,
                        "kind": "correction",
                        "external_raw_ars": 0.0,
                        "external_adjusted_ars": 0.0,
                        "external_final_ars": 0.0,
                        "fx_revaluation_ars": 0.0,
                        "imported_internal_ars": 0.0,
                        "imported_external_ars": 0.0,
                        "cash_total_delta_ars": 0.0,
                        "cash_ars_delta": None,
                        "cash_usd_delta": None,
                        "buy_amount_ars": 0.0,
                        "sell_amount_ars": 0.0,
                        "income_amount_ars": 0.0,
                        "fee_amount_ars": 0.0,
                        "quality_warnings": ["INFERENCE_PARTIAL"],
                        "residual_ratio": None,
                    }
                )
                continue

            iv = _compute_interval_flow_v2(conn, base_snap, end_snap, include_threshold=False)
            if iv is None:
                iv = {
                    "flow_date": end_d,
                    "kind": "correction",
                    "amount_ars": 0.0,
                    "external_raw_ars": 0.0,
                    "external_adjusted_ars": 0.0,
                    "external_final_ars": 0.0,
                    "fx_revaluation_ars": 0.0,
                    "imported_internal_ars": 0.0,
                    "imported_external_ars": 0.0,
                    "cash_delta_ars": 0.0,
                    "cash_total_delta_ars": 0.0,
                    "cash_ars_delta": None,
                    "cash_usd_delta": None,
                    "buy_amount_ars": 0.0,
                    "sell_amount_ars": 0.0,
                    "income_amount_ars": 0.0,
                    "fee_amount_ars": 0.0,
                    "quality_warnings": [],
                    "residual_ratio": None,
                }
            stats = iv.get("orders_stats") or {}
            if int(stats.get("total", 0) or 0) == 0:
                warns = list(iv.get("quality_warnings") or [])
                if "ORDERS_NONE" not in warns:
                    warns.append("ORDERS_NONE")
                iv["quality_warnings"] = warns
            iv["gross_delta"] = gross_delta
            intervals.append(iv)

        _annotate_flow_rows(intervals)

        out: List[Dict[str, Any]] = []
        first_d, first_v = rows[0]
        adjusted_prev = float(first_v or 0.0)
        out.append(
            {
                "date": first_d,
                "total_value": adjusted_prev,
                "raw_total_value": float(first_v or 0.0),
                "flow_total_ars": 0.0,
                "quality_warnings": [],
            }
        )

        for idx in range(1, len(rows)):
            end_d, end_v = rows[idx]
            iv = intervals[idx - 1]
            gross_delta = float(iv.get("gross_delta") or 0.0)
            flow_total = float(iv.get("amount_ars") or 0.0)
            display_kind = str(iv.get("display_kind") or "")
            # Apply adjustment only for external-flow-like movements.
            applied_flow = flow_total if display_kind in EXTERNAL_DISPLAY_KINDS else 0.0
            adjusted_prev = adjusted_prev + (gross_delta - applied_flow)
            out.append(
                {
                    "date": end_d,
                    "total_value": adjusted_prev,
                    "raw_total_value": float(end_v or 0.0),
                    "flow_total_ars": flow_total,
                    "applied_flow_ars": applied_flow,
                    "external_raw_ars": float(iv.get("external_raw_ars") or 0.0),
                    "external_adjusted_ars": float(iv.get("external_adjusted_ars") or 0.0),
                    "external_final_ars": float(iv.get("external_final_ars") or flow_total),
                    "fx_revaluation_ars": float(iv.get("fx_revaluation_ars") or 0.0),
                    "imported_internal_ars": float(iv.get("imported_internal_ars") or 0.0),
                    "imported_external_ars": float(iv.get("imported_external_ars") or 0.0),
                    "display_kind": display_kind,
                    "display_label": iv.get("display_label"),
                    "reason_code": iv.get("reason_code"),
                    "reason_detail": iv.get("reason_detail"),
                    "quality_warnings": list(iv.get("quality_warnings") or []),
                }
            )
        return out
    finally:
        conn.close()


inflation_router, inflation, kpi_monthly_vs_inflation, compare_inflation, compare_inflation_series, compare_inflation_annual = build_inflation_router(
    parse_date=_parse_date,
    compute_return=compute_return,
    return_with_flows=_return_with_flows,
    get_inflation_series=lambda **kwargs: get_inflation_series(**kwargs),
)
router.include_router(inflation_router)

quality_router, quality = build_quality_router(
    returns_fn=lambda: returns(),
    monthly_kpi_fn=lambda: kpi_monthly_vs_inflation(),
    snapshot_data_freshness_fn=_snapshot_data_freshness,
)
router.include_router(quality_router)


@router.get("/returns")
def returns():
    try:
        conn = dbmod.get_conn()
    except FileNotFoundError:
        empty = _return_with_flows(None, None, None, compute_return(None, None))
        return {"daily": empty, "weekly": empty, "monthly": empty, "yearly": empty, "ytd": empty, "inception": empty}

    try:
        latest = dbmod.latest_snapshot(conn)
        if not latest:
            empty = _return_with_flows(conn, None, None, compute_return(None, None))
            return {"daily": empty, "weekly": empty, "monthly": empty, "yearly": empty, "ytd": empty, "inception": empty}

        base_daily = dbmod.snapshot_before(conn, latest.snapshot_date)
        base_weekly = dbmod.snapshot_on_or_before(conn, target_date(latest.snapshot_date, 7))
        latest_d = date.fromisoformat(latest.snapshot_date)
        month_start = latest_d.replace(day=1).isoformat()
        base_monthly = dbmod.first_snapshot_in_range(conn, month_start, latest.snapshot_date) or latest

        y = latest_d.year
        base_yearly = dbmod.first_snapshot_of_year(conn, y, latest.snapshot_date) or latest
        base_ytd = base_yearly
        base_inception = dbmod.earliest_snapshot(conn) or latest

        daily_gross = None
        if base_daily:
            daily_gross = compute_return(latest, base_daily)
        else:
            # If there is only one snapshot in the DB, returns between snapshots can't be computed.
            # Still show a useful daily delta using IOL-provided per-asset daily variation.
            assets = dbmod.assets_for_snapshot(conn, latest.snapshot_date)
            daily_gross = compute_daily_return_from_assets(latest, assets)

        return {
            "daily": _return_with_flows(conn, latest, base_daily, daily_gross),
            "weekly": _return_with_flows(conn, latest, base_weekly, compute_return(latest, base_weekly)),
            "monthly": _return_with_flows(conn, latest, base_monthly, compute_return(latest, base_monthly)),
            "yearly": _return_with_flows(conn, latest, base_yearly, compute_return(latest, base_yearly)),
            "ytd": _return_with_flows(conn, latest, base_ytd, compute_return(latest, base_ytd)),
            "inception": _return_with_flows(conn, latest, base_inception, compute_return(latest, base_inception)),
        }
    finally:
        conn.close()






