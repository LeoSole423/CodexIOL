from __future__ import annotations

import calendar
from datetime import date, datetime, timezone
import json
from typing import Any, Dict, List, Optional, Tuple

from fastapi import APIRouter, Body, Query
from fastapi.responses import JSONResponse

from iol_advisor.service import (
    load_briefing_history_payload,
    load_latest_briefing_payload,
    load_latest_opportunity_payload,
)
from iol_reconciliation.service import (
    apply_proposal as apply_reconciliation_proposal,
    dismiss_proposal as dismiss_reconciliation_proposal,
    ensure_latest_run as ensure_latest_reconciliation_run,
    explain_interval as explain_reconciliation_interval,
    get_latest_payload as get_latest_reconciliation_payload,
    get_open_payload as get_open_reconciliation_payload,
)
from . import db as dbmod
from .inflation_ar import get_inflation_series
from .inflation_compare import compounded_inflation_pct, inflation_factor_for_date, month_key
from .metrics import compute_daily_return_from_assets, compute_return, enrich_return_block, target_date
from .movers import build_union_movers, build_union_movers_pnl


router = APIRouter(prefix="/api")


def _parse_date(v: Optional[str]) -> Optional[str]:
    if v is None:
        return None
    v = v.strip()
    if not v:
        return None
    date.fromisoformat(v)
    return v


def _advisor_cadence(v: str) -> str:
    c = str(v or "").strip().lower()
    if c not in ("daily", "weekly"):
        raise ValueError("cadence must be daily|weekly")
    return c


def _snapshot_cash_ars(snap: Optional[dbmod.Snapshot]) -> Optional[float]:
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


EXTERNAL_DISPLAY_KINDS = {"external_deposit_probable", "external_withdraw_probable"}


def _norm_currency(v: Any) -> str:
    s = str(v or "").strip().upper()
    if s in ("ARS", "PESO_ARGENTINO", "PESO ARGENTINO", "PESOS", "$", "AR$"):
        return "ARS"
    if s in ("USD", "US$", "U$S", "DOLAR", "DOLAR_ESTADOUNIDENSE", "DOLAR ESTADOUNIDENSE"):
        return "USD"
    if not s:
        return "ARS"
    return s


def _norm_movement_kind(v: Any) -> str:
    s = str(v or "").strip().lower()
    if not s:
        return "correction_unknown"
    return s


def _snapshot_cash_components(snap: Optional[dbmod.Snapshot]) -> Dict[str, Optional[float]]:
    if not snap:
        return {"cash_total_ars": None, "cash_ars": None, "cash_usd": None}
    cash_total = _snapshot_cash_ars(snap)
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


def _implied_fx_ars_per_usd(cash_total_ars: Optional[float], cash_ars: Optional[float], cash_usd: Optional[float]) -> Optional[float]:
    try:
        if cash_total_ars is None or cash_ars is None or cash_usd is None:
            return None
        usd = float(cash_usd)
        if abs(usd) <= 1e-9:
            return None
        return (float(cash_total_ars) - float(cash_ars)) / usd
    except Exception:
        return None


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
    imported_external = 0.0
    imported_internal = 0.0
    imported_dividend = 0.0
    imported_fee = 0.0
    imported_count = 0
    warnings: List[str] = []
    for mv in rows:
        kind = _norm_movement_kind(mv.get("kind"))
        amt_ars = _movement_amount_to_ars(mv, fx_end_ars_per_usd, warnings)
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

    fx_base = _implied_fx_ars_per_usd(cash_total_base, cash_ars_base, cash_usd_base)
    fx_end = _implied_fx_ars_per_usd(cash_total_end, cash_ars_end, cash_usd_end)
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


def _add_months(year: int, month: int, delta_months: int) -> tuple[int, int]:
    # month: 1..12
    n = year * 12 + (month - 1) + int(delta_months)
    y = n // 12
    m = (n % 12) + 1
    return int(y), int(m)


def _month_range_end_date(year: int, month: int) -> str:
    last_day = calendar.monthrange(int(year), int(month))[1]
    return f"{int(year):04d}-{int(month):02d}-{int(last_day):02d}"


def _monthly_vs_inflation_payload(
    month: Optional[str],
    from_date: Optional[str],
    to_date: Optional[str],
    status: str,
    market_pct: Optional[float] = None,
    market_delta_ars: Optional[float] = None,
    contributions_ars: Optional[float] = None,
    net_pct: Optional[float] = None,
    net_delta_ars: Optional[float] = None,
    inflation_pct: Optional[float] = None,
    inflation_projected: bool = False,
    real_vs_inflation_pct: Optional[float] = None,
    beats_inflation: Optional[bool] = None,
    quality_warnings: Optional[list[str]] = None,
    inflation_available_to: Optional[str] = None,
    flow_confidence: Optional[str] = None,
    data_freshness: Optional[Dict[str, Any]] = None,
    orders_coverage: Optional[Dict[str, Any]] = None,
    movements_coverage: Optional[Dict[str, Any]] = None,
    estimated: Optional[bool] = None,
) -> Dict[str, Any]:
    return {
        "month": month,
        "from": from_date,
        "to": to_date,
        "market_pct": market_pct,
        "market_delta_ars": market_delta_ars,
        "contributions_ars": contributions_ars,
        "net_pct": net_pct,
        "net_delta_ars": net_delta_ars,
        "inflation_pct": inflation_pct,
        "inflation_projected": bool(inflation_projected),
        "real_vs_inflation_pct": real_vs_inflation_pct,
        "beats_inflation": beats_inflation,
        "quality_warnings": list(quality_warnings or []),
        "inflation_available_to": inflation_available_to,
        "flow_confidence": flow_confidence,
        "data_freshness": data_freshness,
        "orders_coverage": orders_coverage,
        "movements_coverage": movements_coverage,
        "estimated": bool(estimated) if estimated is not None else False,
        "status": status,
    }


@router.get("/latest")
def latest():
    try:
        conn = dbmod.get_conn()
    except FileNotFoundError:
        return {"snapshot": None, "assets": [], "message": "DB not found"}
    try:
        snap = dbmod.latest_snapshot(conn)
        if not snap:
            return {"snapshot": None, "assets": [], "message": "No snapshots"}
        assets = dbmod.assets_for_snapshot(conn, snap.snapshot_date)
        assets.sort(key=lambda a: float(a.get("total_value") or 0.0), reverse=True)
        return {
            "snapshot": {
                "snapshot_date": snap.snapshot_date,
                "total_value": snap.total_value,
                "currency": snap.currency,
                "titles_value": snap.titles_value,
                "cash_total_ars": snap.cash_total_ars,
                "cash_disponible_ars": snap.cash_disponible_ars,
                "cash_disponible_usd": snap.cash_disponible_usd,
                "retrieved_at": snap.retrieved_at,
                "close_time": snap.close_time,
                "minutes_from_close": snap.minutes_from_close,
                "source": snap.source,
            },
            "assets": assets,
        }
    finally:
        conn.close()


@router.get("/advisor/latest")
def advisor_latest(cadence: str = "daily"):
    try:
        cadence_v = _advisor_cadence(cadence)
    except ValueError as exc:
        return JSONResponse(status_code=400, content={"error": str(exc)})
    payload = load_latest_briefing_payload(dbmod.resolve_db_path(), cadence_v)
    return {"cadence": cadence_v, "briefing": payload}


@router.get("/advisor/history")
def advisor_history(cadence: Optional[str] = None, limit: int = 20):
    cadence_v = None
    if cadence is not None and str(cadence).strip():
        try:
            cadence_v = _advisor_cadence(cadence)
        except ValueError as exc:
            return JSONResponse(status_code=400, content={"error": str(exc)})
    limit_v = int(limit)
    if limit_v < 1 or limit_v > 200:
        return JSONResponse(status_code=400, content={"error": "limit must be 1..200"})
    rows = load_briefing_history_payload(dbmod.resolve_db_path(), cadence_v, limit_v)
    return {"cadence": cadence_v, "rows": rows}


@router.get("/advisor/opportunities/latest")
def advisor_opportunities_latest():
    payload = load_latest_opportunity_payload(dbmod.resolve_db_path())
    if not payload:
        return {"run": None}
    return {"run": payload}


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


def _quality_row(
    row_id: str,
    label: str,
    value: str,
    kind: str,
    detail: str,
    *,
    sources: Optional[List[str]] = None,
    codes: Optional[List[str]] = None,
) -> Dict[str, Any]:
    return {
        "id": row_id,
        "label": label,
        "value": value,
        "kind": kind,
        "detail": detail,
        "sources": list(sources or []),
        "codes": list(codes or []),
    }


def _latest_evidence_stats(conn, as_of: str) -> Dict[str, Any]:
    cols = {r[1] for r in conn.execute("PRAGMA table_info(advisor_evidence)").fetchall()}
    if not cols:
        return {"latest_retrieved_at": None, "recent_14d": 0, "recent_45d": 0}
    row = conn.execute(
        """
        SELECT
            MAX(retrieved_at_utc) AS latest_retrieved_at,
            SUM(CASE WHEN substr(retrieved_at_utc, 1, 10) >= ? THEN 1 ELSE 0 END) AS recent_14d,
            SUM(CASE WHEN substr(retrieved_at_utc, 1, 10) >= ? THEN 1 ELSE 0 END) AS recent_45d
        FROM advisor_evidence
        """,
        (target_date(as_of, 14), target_date(as_of, 45)),
    ).fetchone()
    return dict(row or {})


def _cashflow_import_stats(conn, as_of: str) -> Dict[str, Any]:
    cols = {r[1] for r in conn.execute("PRAGMA table_info(account_cash_movements)").fetchall()}
    if not cols:
        return {"total_rows": 0, "recent_rows": 0, "latest_movement_date": None}
    row = conn.execute(
        """
        SELECT
            COUNT(*) AS total_rows,
            SUM(CASE WHEN movement_date >= ? THEN 1 ELSE 0 END) AS recent_rows,
            MAX(movement_date) AS latest_movement_date
        FROM account_cash_movements
        """,
        (target_date(as_of, 30),),
    ).fetchone()
    return dict(row or {})


def _latest_run_quality(conn) -> Dict[str, Any]:
    cols = {r[1] for r in conn.execute("PRAGMA table_info(advisor_opportunity_runs)").fetchall()}
    if not cols or "created_at_utc" not in cols or "as_of" not in cols:
        return {"run_metrics": None, "created_at_utc": None, "as_of": None}
    select_metrics = "run_metrics_json" if "run_metrics_json" in cols else "NULL AS run_metrics_json"
    status_filter = "WHERE status = 'ok'" if "status" in cols else ""
    row = conn.execute(
        f"""
        SELECT {select_metrics}, created_at_utc, as_of
        FROM advisor_opportunity_runs
        {status_filter}
        ORDER BY id DESC
        LIMIT 1
        """
    ).fetchone()
    if not row:
        return {"run_metrics": None, "created_at_utc": None, "as_of": None}
    metrics = {}
    try:
        metrics = json.loads(str(row["run_metrics_json"] or "{}"))
        if not isinstance(metrics, dict):
            metrics = {}
    except Exception:
        metrics = {}
    return {
        "run_metrics": metrics,
        "created_at_utc": row["created_at_utc"],
        "as_of": row["as_of"],
    }


def _reconciliation_quality_summary(conn, latest_snapshot_date: Optional[str]) -> Dict[str, Any]:
    try:
        payload = ensure_latest_reconciliation_run(conn, as_of=latest_snapshot_date, days=30)
    except Exception:
        return {
            "coverage_mode": "none",
            "open_intervals": 0,
            "suppressed_intervals": 0,
            "counts": {},
            "headline": "No se pudo analizar la conciliacion.",
            "latest_run_id": None,
        }
    summary = dict(payload.get("summary") or {})
    summary["latest_run_id"] = payload.get("id")
    summary["created_at_utc"] = payload.get("created_at_utc")
    return summary


def _reconciliation_kind(summary: Dict[str, Any]) -> str:
    open_intervals = int(summary.get("open_intervals") or 0)
    coverage_mode = str(summary.get("coverage_mode") or "none")
    if open_intervals > 0:
        return "warn"
    if coverage_mode in ("imported", "manual", "mixed"):
        return "ok"
    return "info"


@router.get("/quality")
def quality():
    ret = returns()
    monthly_kpi = kpi_monthly_vs_inflation()
    try:
        conn = dbmod.get_conn_rw()
    except FileNotFoundError:
        return {"rows": []}

    try:
        latest = dbmod.latest_snapshot(conn)
        latest_snapshot_date = latest.snapshot_date if latest else date.today().isoformat()
        period_blocks = [
            {"label": "Día", "block": ret.get("daily") or {}},
            {"label": "Semana", "block": ret.get("weekly") or {}},
            {"label": "Mes", "block": ret.get("monthly") or {}},
            {"label": "Año", "block": ret.get("yearly") or {}},
            {"label": "Desde inicio", "block": ret.get("inception") or {}},
        ]

        warn_set = set()
        warns_by_source: List[str] = []
        coverage_count = 0
        for item in period_blocks:
            block = item["block"]
            if hasValid := bool(block.get("from")) and bool(block.get("to")) and str(block.get("from")) != str(block.get("to")):
                coverage_count += 1
            warns = [str(w) for w in (block.get("quality_warnings") or [])]
            if warns:
                warns_by_source.append(f"{item['label']}: {', '.join(warns)}")
            for w in warns:
                warn_set.add(w)
        monthly_warns = [str(w) for w in (monthly_kpi.get("quality_warnings") or [])]
        if monthly_warns:
            warns_by_source.append(f"KPI mensual: {', '.join(monthly_warns)}")
        for w in monthly_warns:
            warn_set.add(w)

        reconciliation_summary = _reconciliation_quality_summary(conn, latest_snapshot_date)
        reconciliation_kind = _reconciliation_kind(reconciliation_summary)
        reconciliation_sources = [
            f"Cobertura: {str(reconciliation_summary.get('coverage_mode') or 'none')}",
            f"Abiertos: {int(reconciliation_summary.get('open_intervals') or 0)}",
            f"Importados 30d: {int(((reconciliation_summary.get('import_stats') or {}).get('recent_rows') or 0))}",
            f"Manuales 30d: {int(((reconciliation_summary.get('manual_stats') or {}).get('recent_rows') or 0))}",
        ]

        critical_warns = ["CASH_MISSING", "ORDERS_INCOMPLETE", "INFERENCE_PARTIAL"]
        critical_count = len([w for w in critical_warns if w in warn_set])
        inference_kind = "ok"
        inference_value = "OK"
        inference_detail = "No se detectan señales críticas de inferencia."
        if reconciliation_kind == "warn":
            inference_kind = "warn"
            inference_value = f"Revisar ({int(reconciliation_summary.get('open_intervals') or 0)})"
            inference_detail = str(reconciliation_summary.get("headline") or "Hay intervalos pendientes de conciliación.")
        elif reconciliation_kind == "ok":
            coverage_mode = str(reconciliation_summary.get("coverage_mode") or "manual")
            inference_kind = "ok"
            inference_value = {
                "imported": "Importado",
                "manual": "Manual OK",
                "mixed": "Mixto",
            }.get(coverage_mode, "OK")
            inference_detail = str(reconciliation_summary.get("headline") or "La inferencia quedó conciliada.")
        elif critical_count > 0:
            inference_kind = "warn"
            inference_value = f"Revisar ({critical_count})"
            inference_detail = "El retorno real sigue dependiendo de inferencias parciales o cobertura incompleta."
        elif warn_set:
            inference_kind = "info"
            inference_value = "Estimado"
            inference_detail = "Hay señales informativas; el cálculo es usable pero no totalmente confirmado."

        freshness = _snapshot_data_freshness(latest)
        fresh_kind = "ok" if freshness.get("status") == "fresh" else ("info" if freshness.get("status") == "aging" else "warn")
        fresh_value = "Actualizado"
        if freshness.get("status") == "aging":
            fresh_value = f"{freshness.get('days_stale')}d"
        elif freshness.get("status") in ("stale", "missing"):
            fresh_value = "Desactualizado"

        cashflow_stats = _cashflow_import_stats(conn, latest_snapshot_date)
        imported_recent = int(cashflow_stats.get("recent_rows") or 0)
        manual_recent = int(((reconciliation_summary.get("manual_stats") or {}).get("recent_rows") or 0))
        coverage_mode = str(reconciliation_summary.get("coverage_mode") or "none")
        cashflow_kind = "warn" if coverage_mode == "none" else ("ok" if reconciliation_kind == "ok" else "info")
        cashflow_value = {
            "imported": f"{imported_recent} importados",
            "manual": f"{manual_recent} manuales",
            "mixed": "Mixto",
        }.get(coverage_mode, "Sin cobertura")
        cashflow_detail = (
            "La conciliación usa movimientos importados confirmados."
            if coverage_mode == "imported"
            else (
                "La conciliación quedó resuelta con ajustes manuales auditados."
                if coverage_mode == "manual"
                else (
                    "La conciliación combina movimientos importados y ajustes manuales."
                    if coverage_mode == "mixed"
                    else "No hay cobertura suficiente de cashflows; falta importar movimientos o confirmar ajustes."
                )
            )
        )

        evidence_stats = _latest_evidence_stats(conn, latest_snapshot_date)
        latest_evidence = str(evidence_stats.get("latest_retrieved_at") or "")
        evidence_kind = "warn"
        evidence_value = "Sin evidencia"
        evidence_detail = "No hay evidencia reciente para sostener el reranking."
        if latest_evidence:
            try:
                age_days = max(0, (date.fromisoformat(latest_snapshot_date) - date.fromisoformat(latest_evidence[:10])).days)
            except Exception:
                age_days = None
            if age_days is not None and age_days <= 7:
                evidence_kind = "ok"
                evidence_value = f"{int(evidence_stats.get('recent_14d') or 0)} fresca"
                evidence_detail = "La evidencia reciente está dentro de la ventana operativa."
            elif age_days is not None and age_days <= 21:
                evidence_kind = "info"
                evidence_value = f"{age_days}d"
                evidence_detail = "La evidencia empieza a perder frescura para oportunidades nuevas."
            else:
                evidence_value = "Vieja"
                evidence_detail = "La evidencia disponible está vieja para operar con confianza."

        run_health = _latest_run_quality(conn)
        metrics = dict(run_health.get("run_metrics") or {})
        dispersion = float(metrics.get("score_dispersion") or 0.0)
        fresh_ratio = float(metrics.get("fresh_evidence_ratio") or 0.0)
        scoring_kind = "warn"
        scoring_value = "Sin run"
        scoring_detail = "Todavía no hay una corrida reciente de oportunidades con métricas registradas."
        if metrics:
            if dispersion >= 10.0 and fresh_ratio >= 0.30:
                scoring_kind = "ok"
                scoring_value = f"Disp. {dispersion:.1f}"
                scoring_detail = "El scoring muestra dispersión suficiente y evidencia fresca razonable."
            elif dispersion >= 5.0:
                scoring_kind = "info"
                scoring_value = f"Disp. {dispersion:.1f}"
                scoring_detail = "El scoring discrimina algo, pero todavía hay margen para mayor señal."
            else:
                scoring_value = f"Disp. {dispersion:.1f}"
                scoring_detail = "El scoring sigue demasiado comprimido para tomarlo como ranking fuerte."

        ipc_status = str(monthly_kpi.get("status") or "")
        ipc_kind = "info"
        ipc_value = "Sin dato"
        ipc_detail = "No hay KPI mensual de IPC disponible."
        if ipc_status == "ok" and bool(monthly_kpi.get("inflation_projected")):
            ipc_kind = "warn"
            ipc_value = "Estimado"
            ipc_detail = "El IPC del mes actual se proyecta con información parcial."
        elif ipc_status == "ok":
            ipc_kind = "ok"
            ipc_value = "OK"
            ipc_detail = "El KPI mensual usa un IPC disponible."
        elif ipc_status == "inflation_unavailable":
            ipc_kind = "warn"
            ipc_value = "No disponible"
            ipc_detail = "No se pudo obtener el IPC para la ventana actual."
        elif ipc_status == "insufficient_snapshots":
            ipc_kind = "info"
            ipc_value = "Sin base"
            ipc_detail = "Faltan snapshots para comparar contra inflación."

        coverage_kind = "ok" if coverage_count == len(period_blocks) else ("info" if coverage_count >= 3 else "warn")
        rows = [
            _quality_row(
                "quality_inference",
                "Calidad de inferencia",
                inference_value,
                inference_kind,
                inference_detail,
                sources=(warns_by_source or ["Sin warnings por ventanas."]) + reconciliation_sources,
                codes=sorted(set(list(warn_set) + [f"RECON_{str(reconciliation_summary.get('coverage_mode') or 'none').upper()}"])),
            ),
            _quality_row(
                "snapshot_freshness",
                "Frescura de snapshots",
                fresh_value,
                fresh_kind,
                "Última foto de cartera y retrieval operativo.",
                sources=[
                    f"Snapshot: {freshness.get('snapshot_date') or '-'}",
                    f"Retrieval: {freshness.get('retrieved_at') or '-'}",
                    f"Días stale: {freshness.get('days_stale')}",
                ],
                codes=[str(freshness.get("status") or "unknown").upper()],
            ),
            _quality_row(
                "cashflow_imports",
                "Cobertura cashflows",
                cashflow_value,
                cashflow_kind,
                cashflow_detail,
                sources=[
                    f"Rows totales: {int(cashflow_stats.get('total_rows') or 0)}",
                    f"Rows 30d: {imported_recent}",
                    f"Ajustes 30d: {manual_recent}",
                    f"Último movimiento: {cashflow_stats.get('latest_movement_date') or '-'}",
                ],
                codes=[
                    "CASHFLOW_IMPORTED" if imported_recent > 0 else "CASHFLOW_IMPORT_MISSING",
                    f"COVERAGE_{coverage_mode.upper()}",
                ],
            ),
            _quality_row(
                "reconciliation_queue",
                "Cola de conciliación",
                f"{int(reconciliation_summary.get('open_intervals') or 0)} abiertas",
                reconciliation_kind,
                str(reconciliation_summary.get("headline") or "Sin datos de conciliación."),
                sources=reconciliation_sources,
                codes=[
                    f"RECONCILIATION_{str(reconciliation_summary.get('coverage_mode') or 'none').upper()}",
                    f"OPEN_{int(reconciliation_summary.get('open_intervals') or 0)}",
                ],
            ),
            _quality_row(
                "evidence_freshness",
                "Frescura de evidencia",
                evidence_value,
                evidence_kind,
                evidence_detail,
                sources=[
                    f"Última evidencia: {latest_evidence or '-'}",
                    f"Filas 14d: {int(evidence_stats.get('recent_14d') or 0)}",
                    f"Filas 45d: {int(evidence_stats.get('recent_45d') or 0)}",
                ],
                codes=["EVIDENCE_FRESH" if evidence_kind == "ok" else "EVIDENCE_STALE"],
            ),
            _quality_row(
                "scoring_health",
                "Salud del scoring",
                scoring_value,
                scoring_kind,
                scoring_detail,
                sources=[
                    f"Run as_of: {run_health.get('as_of') or '-'}",
                    f"Dispersion: {dispersion:.2f}",
                    f"Fresh evidence ratio: {fresh_ratio:.1%}",
                ],
                codes=["SCORING_HEALTHY" if scoring_kind == "ok" else "SCORING_DEGENERATE"],
            ),
            _quality_row(
                "ipc_monthly",
                "Estado IPC mensual",
                ipc_value,
                ipc_kind,
                ipc_detail,
                sources=[
                    f"Status KPI: {ipc_status or '-'}",
                    f"Mes KPI: {monthly_kpi.get('month') or '-'}",
                ],
                codes=[str(w) for w in monthly_warns],
            ),
            _quality_row(
                "coverage_windows",
                "Cobertura de ventanas",
                f"{coverage_count}/5 con base valida",
                coverage_kind,
                "Cuántas ventanas de retorno tienen base histórica usable.",
                sources=[f"{item['label']}: {item['block'].get('from')} -> {item['block'].get('to')}" for item in period_blocks],
            ),
        ]
        return {"rows": rows, "meta": {"snapshot_date": latest_snapshot_date}}
    finally:
        conn.close()


@router.get("/cashflows/auto")
def cashflows_auto(days: int = 30):
    try:
        days_n = int(days)
    except Exception:
        return JSONResponse(status_code=400, content={"error": "days must be an integer (1..365)"})
    if days_n < 1 or days_n > 365:
        return JSONResponse(status_code=400, content={"error": "days must be 1..365"})

    try:
        conn = dbmod.get_conn()
    except FileNotFoundError:
        return {"from": None, "to": None, "days": days_n, "rows": []}

    try:
        latest = dbmod.latest_snapshot(conn)
        if not latest:
            return {"from": None, "to": None, "days": days_n, "rows": []}

        to_date = latest.snapshot_date
        from_date = target_date(to_date, days_n)
        snap_dates = [d for (d, _) in dbmod.snapshots_series(conn, from_date, to_date)]
        if len(snap_dates) < 2:
            return {"from": from_date, "to": to_date, "days": days_n, "rows": []}

        rows: List[Dict[str, Any]] = []
        for i in range(1, len(snap_dates)):
            base_snap = dbmod.snapshot_on_or_before(conn, snap_dates[i - 1])
            end_snap = dbmod.snapshot_on_or_before(conn, snap_dates[i])
            if not base_snap or not end_snap:
                continue
            row = _compute_interval_flow_v2(conn, base_snap, end_snap, include_threshold=True)
            if row is not None:
                rows.append(row)

        _annotate_flow_rows(rows)

        rows.sort(key=lambda r: (str(r.get("flow_date") or ""), float(r.get("amount_ars") or 0.0)), reverse=True)
        return {"from": from_date, "to": to_date, "days": days_n, "rows": rows}
    finally:
        conn.close()


@router.get("/cashflows/manual")
def cashflows_manual(date_from: Optional[str] = Query(None, alias="from"), date_to: Optional[str] = Query(None, alias="to")):
    try:
        f = _parse_date(date_from)
        t = _parse_date(date_to)
        conn = dbmod.get_conn_rw()
    except FileNotFoundError:
        return []
    except Exception:
        return JSONResponse(status_code=400, content={"error": "invalid date format (YYYY-MM-DD)"})

    try:
        return dbmod.list_manual_cashflow_adjustments(conn, f, t)
    finally:
        conn.close()


@router.post("/cashflows/manual")
def cashflows_manual_add(payload: Dict[str, Any] = Body(...)):
    try:
        flow_date = _parse_date(str(payload.get("flow_date") or ""))
        if flow_date is None:
            return JSONResponse(status_code=400, content={"error": "flow_date is required (YYYY-MM-DD)"})
        kind = str(payload.get("kind") or "").strip().lower()
        amount_raw = payload.get("amount_ars")
        if amount_raw is None:
            return JSONResponse(status_code=400, content={"error": "amount_ars is required"})
        amount = float(amount_raw)
        note = payload.get("note")
        conn = dbmod.get_conn_rw()
    except FileNotFoundError:
        return JSONResponse(status_code=404, content={"error": "DB not found"})
    except ValueError as e:
        return JSONResponse(status_code=400, content={"error": str(e)})
    except Exception:
        return JSONResponse(status_code=400, content={"error": "invalid payload"})

    try:
        row = dbmod.add_manual_cashflow_adjustment(conn, flow_date, kind, amount, note)
        return row
    except ValueError as e:
        return JSONResponse(status_code=400, content={"error": str(e)})
    finally:
        conn.close()


@router.delete("/cashflows/manual/{row_id}")
def cashflows_manual_delete(row_id: int):
    try:
        conn = dbmod.get_conn_rw()
    except FileNotFoundError:
        return JSONResponse(status_code=404, content={"error": "DB not found"})

    try:
        ok = dbmod.delete_manual_cashflow_adjustment(conn, int(row_id))
        if not ok:
            return JSONResponse(status_code=404, content={"error": "not found"})
        return {"ok": True, "id": int(row_id)}
    finally:
        conn.close()


@router.get("/reconciliation/latest")
def reconciliation_latest(as_of: Optional[str] = Query(None, alias="as_of")):
    try:
        conn = dbmod.get_conn_rw()
    except FileNotFoundError:
        return {"summary": {}, "intervals": [], "proposals": []}
    try:
        return get_latest_reconciliation_payload(conn, as_of=as_of, ensure=True)
    finally:
        conn.close()


@router.get("/reconciliation/open")
def reconciliation_open(as_of: Optional[str] = Query(None, alias="as_of")):
    try:
        conn = dbmod.get_conn_rw()
    except FileNotFoundError:
        return {"run": {"summary": {}}, "rows": []}
    try:
        return get_open_reconciliation_payload(conn, as_of=as_of, ensure=True)
    finally:
        conn.close()


@router.post("/reconciliation/apply")
def reconciliation_apply(payload: Dict[str, Any] = Body(...)):
    proposal_id = payload.get("proposal_id")
    if proposal_id is None:
        return JSONResponse(status_code=400, content={"error": "proposal_id is required"})
    try:
        conn = dbmod.get_conn_rw()
    except FileNotFoundError:
        return JSONResponse(status_code=404, content={"error": "DB not found"})
    try:
        return apply_reconciliation_proposal(conn, int(proposal_id), note=payload.get("note"))
    except ValueError as exc:
        return JSONResponse(status_code=400, content={"error": str(exc)})
    finally:
        conn.close()


@router.post("/reconciliation/dismiss")
def reconciliation_dismiss(payload: Dict[str, Any] = Body(...)):
    proposal_id = payload.get("proposal_id")
    reason = str(payload.get("reason") or "").strip()
    if proposal_id is None:
        return JSONResponse(status_code=400, content={"error": "proposal_id is required"})
    if not reason:
        return JSONResponse(status_code=400, content={"error": "reason is required"})
    try:
        conn = dbmod.get_conn_rw()
    except FileNotFoundError:
        return JSONResponse(status_code=404, content={"error": "DB not found"})
    try:
        return dismiss_reconciliation_proposal(conn, int(proposal_id), reason=reason)
    except ValueError as exc:
        return JSONResponse(status_code=400, content={"error": str(exc)})
    finally:
        conn.close()


@router.get("/reconciliation/interval/{interval_id}")
def reconciliation_interval(interval_id: int):
    try:
        conn = dbmod.get_conn_rw()
    except FileNotFoundError:
        return JSONResponse(status_code=404, content={"error": "DB not found"})
    try:
        return explain_reconciliation_interval(conn, int(interval_id))
    except ValueError as exc:
        return JSONResponse(status_code=404, content={"error": str(exc)})
    finally:
        conn.close()


@router.get("/inflation")
def inflation(date_from: Optional[str] = Query(None, alias="from"), date_to: Optional[str] = Query(None, alias="to")):
    """
    Argentina monthly inflation (IPC INDEC) as percentage, by month.

    Uses datos.gob.ar series API with local cache (see src/iol_web/inflation_ar.py).
    """
    f = _parse_date(date_from)
    t = _parse_date(date_to)
    try:
        res = get_inflation_series(start_date=f, end_date=t)
    except Exception as e:
        return JSONResponse(status_code=502, content={"error": f"failed to fetch inflation: {type(e).__name__}"})

    months = []
    for d, v in res.data or []:
        try:
            pct = float(v) * 100.0
        except Exception:
            pct = None
        months.append({"month": str(d)[:7], "date": str(d), "inflation_pct": pct})

    dates = [d for d, _ in (res.data or [])]
    available_from = min(dates)[:7] if dates else None
    available_to = max(dates)[:7] if dates else None

    return {
        "series_id": res.series_id,
        "source": res.source,
        "stale": bool(res.stale),
        "fetched_at": res.fetched_at,
        "available_from": available_from,
        "available_to": available_to,
        "months": months,
    }


@router.get("/kpi/monthly-vs-inflation")
def kpi_monthly_vs_inflation():
    try:
        conn = dbmod.get_conn()
    except FileNotFoundError:
        return _monthly_vs_inflation_payload(
            month=None,
            from_date=None,
            to_date=None,
            status="insufficient_snapshots",
        )

    try:
        latest = dbmod.latest_snapshot(conn)
        if not latest:
            return _monthly_vs_inflation_payload(
                month=None,
                from_date=None,
                to_date=None,
                status="insufficient_snapshots",
            )

        latest_d = date.fromisoformat(latest.snapshot_date)
        month = f"{latest_d.year:04d}-{latest_d.month:02d}"
        month_start = f"{latest_d.year:04d}-{latest_d.month:02d}-01"
        from_snap_in_month = dbmod.first_snapshot_in_range(conn, month_start, latest.snapshot_date)
        from_snap = from_snap_in_month
        if not from_snap_in_month or from_snap_in_month.snapshot_date == latest.snapshot_date:
            # Month-in-progress fallback: use the latest snapshot on/before month start
            # so we can still show an MTD signal with a single snapshot in the current month.
            from_snap = dbmod.snapshot_on_or_before(conn, month_start)
        if not from_snap or from_snap.snapshot_date == latest.snapshot_date:
            return _monthly_vs_inflation_payload(
                month=month,
                from_date=from_snap.snapshot_date if from_snap else None,
                to_date=latest.snapshot_date,
                status="insufficient_snapshots",
            )

        gross = compute_return(latest, from_snap)
        with_flows = _return_with_flows(conn, latest, from_snap, gross)

        market_pct = with_flows.get("pct")
        market_delta_ars = with_flows.get("delta")
        contributions_ars = with_flows.get("flow_total_ars")
        net_pct = with_flows.get("real_pct")
        net_delta_ars = with_flows.get("real_delta")
        quality_warnings = with_flows.get("quality_warnings") or []
        flow_confidence = with_flows.get("flow_confidence")
        data_freshness = with_flows.get("data_freshness")
        orders_coverage = with_flows.get("orders_coverage")
        movements_coverage = with_flows.get("movements_coverage")
        estimated = with_flows.get("estimated")

        inflation_available_to = None
        inflation_pct = None
        inflation_projected = False
        try:
            # Same projection rule as compare_inflation:
            # if current month IPC is not published yet, use the last published month as estimate.
            infl_start = f"{max(2017, latest_d.year - 1):04d}-01-01"
            infl_end = f"{latest_d.year:04d}-{latest_d.month:02d}-01"
            infl = get_inflation_series(start_date=infl_start, end_date=infl_end)
            infl_pct = infl.inflation_pct_by_month()
            infl_dates = [d for d, _ in (infl.data or [])]
            inflation_available_to = max(infl_dates)[:7] if infl_dates else None

            inflation_pct = infl_pct.get(month)
            last_known_inflation_pct = None
            if infl_pct:
                try:
                    last_month = sorted(infl_pct.keys())[-1]
                    last_known_inflation_pct = float(infl_pct.get(last_month))
                except Exception:
                    last_known_inflation_pct = None

            if (
                inflation_pct is None
                and last_known_inflation_pct is not None
                and inflation_available_to is not None
                and month > inflation_available_to
            ):
                inflation_pct = last_known_inflation_pct
                inflation_projected = True
        except Exception:
            inflation_pct = None

        if inflation_pct is None:
            return _monthly_vs_inflation_payload(
                month=month,
                from_date=from_snap.snapshot_date,
                to_date=latest.snapshot_date,
                status="inflation_unavailable",
                market_pct=market_pct,
                market_delta_ars=market_delta_ars,
                contributions_ars=contributions_ars,
                net_pct=net_pct,
                net_delta_ars=net_delta_ars,
                quality_warnings=quality_warnings,
                inflation_available_to=inflation_available_to,
                flow_confidence=flow_confidence,
                data_freshness=data_freshness,
                orders_coverage=orders_coverage,
                movements_coverage=movements_coverage,
                estimated=estimated,
            )

        real_vs_inflation_pct = None
        if net_pct is not None:
            try:
                real_vs_inflation_pct = ((1.0 + float(net_pct) / 100.0) / (1.0 + float(inflation_pct) / 100.0) - 1.0) * 100.0
            except Exception:
                real_vs_inflation_pct = None

        beats_inflation = None if real_vs_inflation_pct is None else bool(real_vs_inflation_pct > 0.0)
        return _monthly_vs_inflation_payload(
            month=month,
            from_date=from_snap.snapshot_date,
            to_date=latest.snapshot_date,
            status="ok",
            market_pct=market_pct,
            market_delta_ars=market_delta_ars,
            contributions_ars=contributions_ars,
            net_pct=net_pct,
            net_delta_ars=net_delta_ars,
            inflation_pct=inflation_pct,
            inflation_projected=inflation_projected,
            real_vs_inflation_pct=real_vs_inflation_pct,
            beats_inflation=beats_inflation,
            quality_warnings=quality_warnings,
            inflation_available_to=inflation_available_to,
            flow_confidence=flow_confidence,
            data_freshness=data_freshness,
            orders_coverage=orders_coverage,
            movements_coverage=movements_coverage,
            estimated=estimated,
        )
    finally:
        conn.close()


@router.get("/compare/inflation")
def compare_inflation(months: int = 12):
    """
    Compare portfolio monthly return (calendar month) vs Argentina inflation (IPC INDEC).
    """
    months = int(months)
    if months < 1 or months > 120:
        return JSONResponse(status_code=400, content={"error": "months must be 1..120"})

    try:
        conn = dbmod.get_conn()
    except FileNotFoundError:
        return {"stale": False, "rows": []}

    try:
        latest = dbmod.latest_snapshot(conn)
        if not latest:
            return {"stale": False, "rows": []}

        latest_d = date.fromisoformat(latest.snapshot_date)
        y_to = latest_d.year
        m_to = latest_d.month
        y_from, m_from = _add_months(y_to, m_to, -(months - 1))

        date_from = f"{y_from:04d}-{m_from:02d}-01"
        date_to = _month_range_end_date(y_to, m_to)

        monthly = dbmod.monthly_first_last_series(conn, date_from=date_from, date_to=date_to)
        if not monthly:
            return {"stale": False, "rows": []}

        infl_start = f"{y_from:04d}-{m_from:02d}-01"
        infl_end = f"{y_to:04d}-{m_to:02d}-01"
        infl = get_inflation_series(start_date=infl_start, end_date=infl_end)
        infl_pct = infl.inflation_pct_by_month()
        infl_dates = [d for d, _ in (infl.data or [])]
        infl_available_from = min(infl_dates)[:7] if infl_dates else None
        infl_available_to = max(infl_dates)[:7] if infl_dates else None

        # Projection: until INDEC publishes the current month, use the last available month inflation as estimate.
        last_known_inflation_pct = None
        if infl_pct:
            try:
                last_month = sorted(infl_pct.keys())[-1]
                last_known_inflation_pct = float(infl_pct.get(last_month))
            except Exception:
                last_known_inflation_pct = None

        rows = []
        projected_count = 0
        for r in monthly:
            month = r["month"]
            first_date = r["first_date"]
            last_date = r["last_date"]
            first_v = float(r["first_value"] or 0.0)
            last_v = float(r["last_value"] or 0.0)

            portfolio_pct = None
            if first_date != last_date and first_v != 0.0:
                portfolio_pct = (last_v / first_v - 1.0) * 100.0

            inflation_projected = False
            inflation_pct = infl_pct.get(month)
            if (
                inflation_pct is None
                and last_known_inflation_pct is not None
                and infl_available_to is not None
                and month > infl_available_to
            ):
                inflation_pct = last_known_inflation_pct
                inflation_projected = True
                projected_count += 1
            real_pct = None
            if portfolio_pct is not None and inflation_pct is not None:
                try:
                    real_pct = ((1.0 + portfolio_pct / 100.0) / (1.0 + inflation_pct / 100.0) - 1.0) * 100.0
                except Exception:
                    real_pct = None

            rows.append(
                {
                    "month": month,
                    "from": first_date,
                    "to": last_date,
                    "portfolio_pct": portfolio_pct,
                    "inflation_pct": inflation_pct,
                    "inflation_projected": inflation_projected,
                    "real_pct": real_pct,
                }
            )

        return {
            "stale": bool(infl.stale),
            "inflation_available_from": infl_available_from,
            "inflation_available_to": infl_available_to,
            "projection_used": projected_count > 0,
            "projection_source_month": infl_available_to,
            "projection_inflation_pct": last_known_inflation_pct,
            "projection_months": projected_count,
            "rows": rows,
        }
    finally:
        conn.close()


@router.get("/compare/inflation/series")
def compare_inflation_series(
    date_from: Optional[str] = Query(None, alias="from"),
    date_to: Optional[str] = Query(None, alias="to"),
):
    """
    Time series of portfolio vs inflation (step-monthly) using the same snapshot dates.

    Returns both value series (ARS) and index series (base 100).
    """
    try:
        f = _parse_date(date_from)
        t = _parse_date(date_to)
        conn = dbmod.get_conn()
    except FileNotFoundError:
        return {"stale": False, "labels": [], "portfolio_value": [], "inflation_value": [], "portfolio_index": [], "inflation_index": []}

    try:
        latest = dbmod.latest_snapshot(conn)
        earliest = dbmod.earliest_snapshot(conn)
        if not latest or not earliest:
            return {"stale": False, "labels": [], "portfolio_value": [], "inflation_value": [], "portfolio_index": [], "inflation_index": []}

        f2 = f or earliest.snapshot_date
        t2 = t or latest.snapshot_date
        series = dbmod.snapshots_series(conn, f2, t2)
        if not series:
            return {"stale": False, "labels": [], "portfolio_value": [], "inflation_value": [], "portfolio_index": [], "inflation_index": []}

        base_date = series[0][0]
        base_value = float(series[0][1] or 0.0)

        # Fetch inflation for the months covering the series range.
        # Query monthly points by first-of-month dates. Include at least the previous year so we can
        # project from the last known inflation even if the series range starts after the latest IPC published.
        y0 = max(2017, date.fromisoformat(str(base_date)).year - 1)
        infl_start = f"{y0:04d}-01-01"
        infl_end = f"{month_key(t2)}-01"
        infl = get_inflation_series(start_date=infl_start, end_date=infl_end)
        infl_pct = infl.inflation_pct_by_month()
        infl_dates = [d for d, _ in (infl.data or [])]
        infl_available_to = max(infl_dates)[:7] if infl_dates else None

        # Projection: last known month inflation can be used as estimate for months after available_to.
        last_known_inflation_pct = None
        if infl_pct:
            try:
                last_month = sorted(infl_pct.keys())[-1]
                last_known_inflation_pct = float(infl_pct.get(last_month))
            except Exception:
                last_known_inflation_pct = None

        labels = []
        p_vals = []
        i_vals = []
        p_idx = []
        i_idx = []
        projected = 0

        for d, v in series:
            d_s = str(d)
            cur_v = float(v or 0.0)
            labels.append(d_s)
            p_vals.append(cur_v)
            if base_value:
                p_idx.append(cur_v / base_value * 100.0)
            else:
                p_idx.append(None)

            proj_month = None
            if infl_available_to and month_key(d_s) > infl_available_to and last_known_inflation_pct is not None:
                proj_month = month_key(d_s)

            factor = inflation_factor_for_date(
                base_date=base_date,
                target_date=d_s,
                infl_pct_by_month=infl_pct,
                projection_month=proj_month,
                projection_pct=last_known_inflation_pct if proj_month else None,
            )
            if factor is None or not base_value:
                i_vals.append(None)
                i_idx.append(None)
            else:
                # Only count projection as "used" when target month differs from the base month;
                # within the base month the step-monthly model applies no inflation yet.
                if proj_month and month_key(d_s) != month_key(base_date):
                    projected += 1
                i_vals.append(base_value * factor)
                i_idx.append(factor * 100.0)

        return {
            "stale": bool(infl.stale),
            "inflation_available_to": infl_available_to,
            "projection_used": projected > 0,
            "labels": labels,
            "portfolio_value": p_vals,
            "inflation_value": i_vals,
            "portfolio_index": p_idx,
            "inflation_index": i_idx,
        }
    finally:
        conn.close()


@router.get("/compare/inflation/annual")
def compare_inflation_annual(years: int = 10):
    """
    Annual (calendar year) portfolio return vs compounded inflation for the same snapshot interval.

    Adds a YTD row for the latest year.
    """
    years = int(years)
    if years < 1 or years > 50:
        return JSONResponse(status_code=400, content={"error": "years must be 1..50"})

    try:
        conn = dbmod.get_conn()
    except FileNotFoundError:
        return {"stale": False, "rows": []}

    try:
        latest = dbmod.latest_snapshot(conn)
        earliest = dbmod.earliest_snapshot(conn)
        if not latest or not earliest:
            return {"stale": False, "rows": []}

        latest_d = date.fromisoformat(latest.snapshot_date)
        latest_year = latest_d.year
        earliest_year = date.fromisoformat(earliest.snapshot_date).year

        start_year = max(earliest_year, latest_year - years + 1)

        # Include at least one year before the start year so we have "last known" inflation for projection.
        infl_start = f"{max(2017, start_year - 1):04d}-01-01"
        infl_end = f"{latest_year:04d}-{latest_d.month:02d}-01"
        infl = get_inflation_series(start_date=infl_start, end_date=infl_end)
        infl_pct = infl.inflation_pct_by_month()
        infl_dates = [d for d, _ in (infl.data or [])]
        infl_available_to = max(infl_dates)[:7] if infl_dates else None

        last_known_inflation_pct = None
        if infl_pct:
            try:
                last_month = sorted(infl_pct.keys())[-1]
                last_known_inflation_pct = float(infl_pct.get(last_month))
            except Exception:
                last_known_inflation_pct = None

        rows = []
        projection_used = False

        def _calc_row(label: str, from_snap: dbmod.Snapshot, to_snap: dbmod.Snapshot, partial: bool) -> Dict[str, Any]:
            nonlocal projection_used
            base_v = float(from_snap.total_value or 0.0)
            end_v = float(to_snap.total_value or 0.0)
            portfolio_pct = None if base_v == 0.0 else (end_v / base_v - 1.0) * 100.0

            proj_month = None
            if infl_available_to and last_known_inflation_pct is not None and month_key(to_snap.snapshot_date) > infl_available_to:
                proj_month = month_key(to_snap.snapshot_date)

            infl_pct_comp, _, projected = compounded_inflation_pct(
                from_date=from_snap.snapshot_date,
                to_date=to_snap.snapshot_date,
                infl_pct_by_month=infl_pct,
                projection_month=proj_month,
                projection_pct=last_known_inflation_pct if proj_month else None,
            )
            if projected:
                projection_used = True

            real_pct = None
            if portfolio_pct is not None and infl_pct_comp is not None:
                try:
                    real_pct = ((1.0 + portfolio_pct / 100.0) / (1.0 + infl_pct_comp / 100.0) - 1.0) * 100.0
                except Exception:
                    real_pct = None

            return {
                "label": label,
                "from": from_snap.snapshot_date,
                "to": to_snap.snapshot_date,
                "portfolio_pct": portfolio_pct,
                "inflation_pct": infl_pct_comp,
                "real_pct": real_pct,
                "partial": bool(partial),
                "inflation_projected": bool(projected),
            }

        for y in range(start_year, latest_year + 1):
            start = f"{y:04d}-01-01"
            end = f"{y:04d}-12-31"
            from_snap = dbmod.first_snapshot_in_range(conn, start, end)
            to_snap = dbmod.last_snapshot_in_range(conn, start, end)
            if not from_snap or not to_snap:
                continue
            partial = (from_snap.snapshot_date != start) or (to_snap.snapshot_date != end)
            row = _calc_row(str(y), from_snap, to_snap, partial=partial)
            if row:
                rows.append(row)

        # YTD row
        y_start_snap = dbmod.first_snapshot_of_year(conn, latest_year, latest.snapshot_date) or earliest
        if y_start_snap:
            ytd_label = f"YTD {latest_year}"
            partial_ytd = True
            row = _calc_row(ytd_label, y_start_snap, latest, partial=partial_ytd)
            if row:
                rows.append(row)

        return {
            "stale": bool(infl.stale),
            "inflation_available_to": infl_available_to,
            "projection_used": bool(projection_used),
            "rows": rows,
        }
    finally:
        conn.close()


@router.get("/movers")
def movers(
    kind: str = "daily",
    limit: int = 10,
    period: Optional[str] = None,
    month: Optional[int] = None,
    year: Optional[int] = None,
    metric: str = "pnl",
    currency: str = "peso_Argentino",
):
    kind = (kind or "").strip().lower()
    if kind not in ("daily", "total", "period"):
        return JSONResponse(status_code=400, content={"error": "kind must be daily|total|period"})
    limit = int(limit)
    if limit < 1 or limit > 100:
        return JSONResponse(status_code=400, content={"error": "limit must be 1..100"})

    try:
        conn = dbmod.get_conn()
    except FileNotFoundError:
        return {"gainers": [], "losers": []}

    try:
        end_snap = dbmod.latest_snapshot(conn)
        if not end_snap:
            return {"gainers": [], "losers": []}
        end_assets = dbmod.assets_for_snapshot(conn, end_snap.snapshot_date)

        if kind == "period":
            p = (period or "daily").strip().lower()
            if p not in ("daily", "weekly", "monthly", "yearly", "ytd"):
                return JSONResponse(status_code=400, content={"error": "period must be daily|weekly|monthly|yearly|ytd"})

            metric_norm = (metric or "pnl").strip().lower()
            if metric_norm not in ("pnl", "valuation"):
                return JSONResponse(status_code=400, content={"error": "metric must be pnl|valuation"})

            currency_norm = (currency or "peso_Argentino").strip()
            if not currency_norm:
                currency_norm = "peso_Argentino"

            def _filter_assets(rows):
                if currency_norm in ("all", None):
                    return list(rows or [])
                if currency_norm == "unknown":
                    return [a for a in (rows or []) if (a.get("currency") in (None, ""))]
                return [a for a in (rows or []) if a.get("currency") == currency_norm]

            if p == "daily":
                # Daily movers directly from IOL-provided daily variation on latest snapshot.
                enriched = []
                for a in _filter_assets(end_assets):
                    cur_val = float(a.get("total_value") or 0.0)
                    pct = a.get("daily_var_pct")
                    try:
                        pct_f = float(pct) if pct is not None else None
                    except Exception:
                        pct_f = None
                    delta = None if pct_f is None else (cur_val * pct_f / 100.0)
                    aa = dict(a)
                    aa["base_total_value"] = None
                    aa["delta_value"] = delta
                    aa["delta_pct"] = pct_f
                    enriched.append(aa)

                def metric(a: Dict[str, Any]) -> float:
                    try:
                        return float(a.get("delta_value") or 0.0)
                    except Exception:
                        return 0.0

                gainers = sorted(enriched, key=metric, reverse=True)
                losers = sorted(enriched, key=metric, reverse=False)
                return {
                    "period": p,
                    "from": end_snap.snapshot_date,
                    "to": end_snap.snapshot_date,
                    "metric": metric_norm,
                    "currency": currency_norm,
                    "warnings": [],
                    "gainers": gainers[:limit],
                    "losers": losers[:limit],
                }

            latest_d = date.fromisoformat(end_snap.snapshot_date)
            latest_year = latest_d.year
            latest_month = latest_d.month

            base_snap = None
            period_end_snap = end_snap

            if p == "weekly":
                base_snap = dbmod.snapshot_on_or_before(conn, target_date(end_snap.snapshot_date, 7))
            elif p == "monthly":
                y = int(year) if year is not None else latest_year
                m = int(month) if month is not None else latest_month
                if m < 1 or m > 12:
                    return JSONResponse(status_code=400, content={"error": "month must be 1..12"})
                if y != latest_year:
                    return JSONResponse(status_code=400, content={"error": "monthly period only supports latest year"})
                last_day = calendar.monthrange(y, m)[1]
                start = f"{y:04d}-{m:02d}-01"
                end = f"{y:04d}-{m:02d}-{last_day:02d}"
                base_snap = dbmod.first_snapshot_in_range(conn, start, end)
                period_end_snap = dbmod.last_snapshot_in_range(conn, start, end)
            elif p == "yearly":
                y = int(year) if year is not None else latest_year
                start = f"{y:04d}-01-01"
                end = f"{y:04d}-12-31"
                base_snap = dbmod.first_snapshot_in_range(conn, start, end)
                period_end_snap = dbmod.last_snapshot_in_range(conn, start, end)
            else:  # ytd
                base_snap = dbmod.first_snapshot_of_year(conn, latest_year, end_snap.snapshot_date) or dbmod.earliest_snapshot(conn)

            if not base_snap or not period_end_snap:
                # For calendar periods, if there are no snapshots in that range we return null dates.
                if p in ("monthly", "yearly"):
                    return {
                        "period": p,
                        "from": None,
                        "to": None,
                        "metric": metric_norm,
                        "currency": currency_norm,
                        "warnings": [],
                        "gainers": [],
                        "losers": [],
                    }
                return {
                    "period": p,
                    "from": None,
                    "to": end_snap.snapshot_date,
                    "metric": metric_norm,
                    "currency": currency_norm,
                    "warnings": [],
                    "gainers": [],
                    "losers": [],
                }

            base_assets = _filter_assets(dbmod.assets_for_snapshot(conn, base_snap.snapshot_date))
            end_assets_period = _filter_assets(dbmod.assets_for_snapshot(conn, period_end_snap.snapshot_date))

            warnings = []
            orders_stats = None
            if metric_norm == "valuation":
                enriched = build_union_movers(base_assets, end_assets_period)
            else:
                # Baseline snapshot represents end-of-day state for base date.
                # Exclude base-day operations to avoid counting cashflows that happened before that snapshot.
                dt_from = f"{base_snap.snapshot_date}T23:59:59"
                dt_to = f"{period_end_snap.snapshot_date}T23:59:59"
                cashflows, stats = dbmod.orders_cashflows_by_symbol(conn, dt_from, dt_to, currency=currency_norm)
                orders_stats = stats
                if stats.get("total", 0) == 0:
                    warnings.append("ORDERS_NONE")
                elif stats.get("unclassified", 0) > 0 or stats.get("amount_missing", 0) > 0:
                    warnings.append("ORDERS_INCOMPLETE")
                enriched = build_union_movers_pnl(base_assets, end_assets_period, cashflows)

            def metric(a: Dict[str, Any]) -> float:
                try:
                    return float(a.get("delta_value") or 0.0)
                except Exception:
                    return 0.0

            gainers = sorted(enriched, key=metric, reverse=True)
            losers = sorted(enriched, key=metric, reverse=False)
            return {
                "period": p,
                "from": base_snap.snapshot_date,
                "to": period_end_snap.snapshot_date,
                "metric": metric_norm,
                "currency": currency_norm,
                "warnings": warnings,
                "orders_stats": orders_stats,
                "gainers": gainers[:limit],
                "losers": losers[:limit],
            }

        key = "daily_var_points" if kind == "daily" else "gain_amount"

        def metric(a: Dict[str, Any]) -> float:
            v = a.get(key)
            if v is None:
                return 0.0
            try:
                return float(v)
            except Exception:
                return 0.0

        gainers = sorted(end_assets, key=metric, reverse=True)
        losers = sorted(end_assets, key=metric, reverse=False)
        return {"gainers": gainers[:limit], "losers": losers[:limit]}
    finally:
        conn.close()


@router.get("/assets/performance")
def assets_performance(
    period: str = Query(...),
    month: Optional[int] = None,
    year: Optional[int] = None,
):
    p = (period or "").strip().lower()
    if p not in ("daily", "weekly", "monthly", "yearly", "accumulated"):
        return JSONResponse(status_code=400, content={"error": "period must be daily|weekly|monthly|yearly|accumulated"})

    try:
        conn = dbmod.get_conn()
    except FileNotFoundError:
        return {"period": p, "from": None, "to": None, "warnings": [], "orders_stats": None, "rows": []}

    try:
        end_snap = dbmod.latest_snapshot(conn)
        if not end_snap:
            return {"period": p, "from": None, "to": None, "warnings": [], "orders_stats": None, "rows": []}

        latest_d = date.fromisoformat(end_snap.snapshot_date)
        latest_year = latest_d.year
        latest_month = latest_d.month

        base_snap = None
        period_end_snap = end_snap

        if p == "daily":
            pass
        elif p == "weekly":
            base_snap = dbmod.snapshot_on_or_before(conn, target_date(end_snap.snapshot_date, 7))
        elif p == "monthly":
            y = int(year) if year is not None else latest_year
            m = int(month) if month is not None else latest_month
            if m < 1 or m > 12:
                return JSONResponse(status_code=400, content={"error": "month must be 1..12"})
            if y != latest_year:
                return JSONResponse(status_code=400, content={"error": "monthly period only supports latest year"})
            last_day = calendar.monthrange(y, m)[1]
            start = f"{y:04d}-{m:02d}-01"
            end = f"{y:04d}-{m:02d}-{last_day:02d}"
            base_snap = dbmod.first_snapshot_in_range(conn, start, end)
            period_end_snap = dbmod.last_snapshot_in_range(conn, start, end)
        elif p == "yearly":
            y = int(year) if year is not None else latest_year
            start = f"{y:04d}-01-01"
            end = f"{y:04d}-12-31"
            base_snap = dbmod.first_snapshot_in_range(conn, start, end)
            period_end_snap = dbmod.last_snapshot_in_range(conn, start, end)
        else:  # accumulated
            base_snap = dbmod.earliest_snapshot(conn)

        if p != "daily" and (not base_snap or not period_end_snap):
            if p in ("monthly", "yearly"):
                return {"period": p, "from": None, "to": None, "warnings": [], "orders_stats": None, "rows": []}
            return {
                "period": p,
                "from": None,
                "to": end_snap.snapshot_date,
                "warnings": [],
                "orders_stats": None,
                "rows": [],
            }

        rows = []
        warnings = []
        orders_stats = None

        if p == "daily":
            end_assets = dbmod.assets_for_snapshot(conn, end_snap.snapshot_date)
            for a in end_assets:
                cur_val = float(a.get("total_value") or 0.0)
                pct = a.get("daily_var_pct")
                try:
                    pct_f = float(pct) if pct is not None else None
                except Exception:
                    pct_f = None
                selected = None if pct_f is None else (cur_val * pct_f / 100.0)
                rows.append(
                    {
                        "symbol": a.get("symbol"),
                        "description": a.get("description"),
                        "currency": a.get("currency") or "unknown",
                        "market": a.get("market"),
                        "type": a.get("type"),
                        "total_value": cur_val,
                        "base_total_value": None,
                        "selected_value": selected,
                        "selected_pct": pct_f,
                        "flow_tag": "none",
                    }
                )
            from_date = end_snap.snapshot_date
            to_date = end_snap.snapshot_date
        else:
            base_assets = dbmod.assets_for_snapshot(conn, base_snap.snapshot_date)
            end_assets_period = dbmod.assets_for_snapshot(conn, period_end_snap.snapshot_date)
            # Baseline snapshot is end-of-day; do not include base-date operations in period cashflows.
            dt_from = f"{base_snap.snapshot_date}T23:59:59"
            dt_to = f"{period_end_snap.snapshot_date}T23:59:59"
            cashflows, stats = dbmod.orders_cashflows_by_symbol(conn, dt_from, dt_to, currency="all")
            orders_stats = stats
            if stats.get("total", 0) == 0:
                warnings.append("ORDERS_NONE")
            elif stats.get("unclassified", 0) > 0 or stats.get("amount_missing", 0) > 0:
                warnings.append("ORDERS_INCOMPLETE")
            enriched = build_union_movers_pnl(base_assets, end_assets_period, cashflows)
            for r in enriched:
                rows.append(
                    {
                        "symbol": r.get("symbol"),
                        "description": r.get("description"),
                        "currency": r.get("currency") or "unknown",
                        "market": r.get("market"),
                        "type": r.get("type"),
                        "total_value": float(r.get("total_value") or 0.0),
                        "base_total_value": float(r.get("base_total_value") or 0.0),
                        "selected_value": r.get("delta_value"),
                        "selected_pct": r.get("delta_pct"),
                        "flow_tag": r.get("flow_tag") or "none",
                    }
                )
            from_date = base_snap.snapshot_date
            to_date = period_end_snap.snapshot_date

        total_visible = sum(float(r.get("total_value") or 0.0) for r in rows)
        for r in rows:
            val = float(r.get("total_value") or 0.0)
            r["weight_pct"] = ((val / total_visible) * 100.0) if total_visible > 0 else 0.0

        def _sort_key(row: Dict[str, Any]):
            try:
                selected = float(row.get("selected_value") or 0.0)
            except Exception:
                selected = 0.0
            try:
                total = float(row.get("total_value") or 0.0)
            except Exception:
                total = 0.0
            sym = str(row.get("symbol") or "")
            return (-selected, -total, sym)

        rows.sort(key=_sort_key)

        return {
            "period": p,
            "from": from_date,
            "to": to_date,
            "warnings": warnings,
            "orders_stats": orders_stats,
            "rows": rows,
        }
    finally:
        conn.close()


@router.get("/allocation")
def allocation(group_by: str = "symbol", include_cash: int = 0):
    group_by = (group_by or "").strip().lower()
    include_cash = int(include_cash)
    if include_cash not in (0, 1):
        return JSONResponse(status_code=400, content={"error": "include_cash must be 0|1"})
    if group_by not in ("symbol", "type", "market", "currency"):
        return JSONResponse(status_code=400, content={"error": "group_by must be symbol|type|market|currency"})

    try:
        conn = dbmod.get_conn()
    except FileNotFoundError:
        return []

    try:
        snap = dbmod.latest_snapshot(conn)
        if not snap:
            return []
        items = [{"key": k, "value": v} for (k, v) in dbmod.allocation(conn, snap.snapshot_date, group_by=group_by)]
        if include_cash == 1:
            # Prefer "total cash in ARS terms" derived from totalEnPesos - titles_value.
            cash_v = snap.cash_total_ars if snap.cash_total_ars is not None else snap.cash_disponible_ars
            if cash_v is not None:
                items.append({"key": "Cash (ARS eq.)", "value": float(cash_v or 0.0)})
            items.sort(key=lambda kv: float(kv["value"] or 0.0), reverse=True)
        return items
    finally:
        conn.close()
