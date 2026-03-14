from __future__ import annotations

import json
import sqlite3
from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from iol_shared import portfolio_db as shared_db
from iol_shared.reconciliation_utils import (
    aggregate_imported_movements,
    implied_fx_ars_per_usd,
    snapshot_cash_components,
)


RESOLUTION_TYPES = {"import", "manual_adjustment", "ignore_internal", "review_orders"}
PROPOSAL_OPEN_STATUSES = {"open", "pending"}
INTERVAL_THRESHOLD_ARS = 100.0


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _loads_json(v: Any, default: Any) -> Any:
    if v is None:
        return default
    if isinstance(v, (dict, list)):
        return v
    try:
        out = json.loads(str(v))
    except Exception:
        return default
    return out if isinstance(out, type(default)) else default


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    try:
        row = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name = ?",
            (str(table),),
        ).fetchone()
    except Exception:
        return False
    return bool(row)


def _ensure_reconciliation_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS reconciliation_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at_utc TEXT NOT NULL,
            as_of TEXT NOT NULL,
            date_from TEXT NOT NULL,
            date_to TEXT NOT NULL,
            days INTEGER NOT NULL,
            status TEXT NOT NULL,
            summary_json TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS reconciliation_intervals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id INTEGER NOT NULL,
            interval_key TEXT NOT NULL,
            base_snapshot_date TEXT NOT NULL,
            end_snapshot_date TEXT NOT NULL,
            state TEXT NOT NULL,
            issue_code TEXT,
            confidence TEXT,
            impact_on_inference TEXT NOT NULL,
            analysis_json TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS reconciliation_proposals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id INTEGER NOT NULL,
            interval_id INTEGER NOT NULL,
            interval_key TEXT NOT NULL,
            issue_code TEXT NOT NULL,
            resolution_type TEXT NOT NULL,
            suggested_kind TEXT,
            suggested_amount_ars REAL,
            confidence TEXT NOT NULL,
            confidence_score REAL NOT NULL,
            reason TEXT NOT NULL,
            source_basis TEXT NOT NULL,
            impact_on_inference TEXT NOT NULL,
            status TEXT NOT NULL,
            created_at_utc TEXT NOT NULL,
            applied_at_utc TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS reconciliation_resolutions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            proposal_id INTEGER NOT NULL,
            interval_id INTEGER NOT NULL,
            interval_key TEXT NOT NULL,
            issue_code TEXT NOT NULL,
            resolution_type TEXT NOT NULL,
            status TEXT NOT NULL,
            manual_cashflow_id INTEGER,
            note TEXT,
            created_at_utc TEXT NOT NULL
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_reconciliation_runs_asof ON reconciliation_runs(as_of, id DESC)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_reconciliation_intervals_run_end ON reconciliation_intervals(run_id, end_snapshot_date DESC)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_reconciliation_intervals_key ON reconciliation_intervals(interval_key)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_reconciliation_proposals_open ON reconciliation_proposals(status, confidence_score DESC, id DESC)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_reconciliation_resolutions_interval ON reconciliation_resolutions(interval_key, issue_code, id DESC)")
    conn.commit()


def _latest_snapshot_date(conn: sqlite3.Connection) -> Optional[str]:
    row = conn.execute(
        "SELECT snapshot_date FROM portfolio_snapshots ORDER BY snapshot_date DESC LIMIT 1"
    ).fetchone()
    return str(row["snapshot_date"]) if row and row["snapshot_date"] else None


def _aggregate_imported_movements(
    conn: sqlite3.Connection,
    base_date_exclusive: str,
    end_date_inclusive: str,
    fx_end_ars_per_usd: Optional[float],
) -> Dict[str, Any]:
    rows = shared_db.list_account_cash_movements(conn, base_date_exclusive, end_date_inclusive)
    aggregated = aggregate_imported_movements(rows, fx_end_ars_per_usd)
    return {
        "rows_count": int(aggregated.get("rows_count") or 0),
        "imported_external_ars": float(aggregated.get("imported_external_ars") or 0.0),
        "imported_internal_ars": float(aggregated.get("imported_internal_ars") or 0.0),
        "warnings": list(aggregated.get("warnings") or []),
    }


def _manual_rows_count(conn: sqlite3.Connection, date_from_exclusive: str, date_to_inclusive: str) -> int:
    if not _table_exists(conn, "manual_cashflow_adjustments"):
        return 0
    row = conn.execute(
        """
        SELECT COUNT(*) AS cnt
        FROM manual_cashflow_adjustments
        WHERE flow_date > ? AND flow_date <= ?
        """,
        (date_from_exclusive, date_to_inclusive),
    ).fetchone()
    return int((row["cnt"] if row and row["cnt"] is not None else 0) or 0)


def _recent_import_stats(conn: sqlite3.Connection, latest_snapshot_date: Optional[str]) -> Dict[str, Any]:
    if not latest_snapshot_date or not _table_exists(conn, "account_cash_movements"):
        return {"total_rows": 0, "recent_rows": 0, "latest_movement_date": None}
    latest_d = date.fromisoformat(str(latest_snapshot_date))
    cutoff = latest_d.fromordinal(latest_d.toordinal() - 30).isoformat()
    row = conn.execute(
        """
        SELECT
          COUNT(*) AS total_rows,
          SUM(CASE WHEN movement_date >= ? THEN 1 ELSE 0 END) AS recent_rows,
          MAX(movement_date) AS latest_movement_date
        FROM account_cash_movements
        """,
        (cutoff,),
    ).fetchone()
    return {
        "total_rows": int((row["total_rows"] if row and row["total_rows"] is not None else 0) or 0),
        "recent_rows": int((row["recent_rows"] if row and row["recent_rows"] is not None else 0) or 0),
        "latest_movement_date": str(row["latest_movement_date"]) if row and row["latest_movement_date"] else None,
    }


def _recent_manual_stats(conn: sqlite3.Connection, latest_snapshot_date: Optional[str]) -> Dict[str, Any]:
    if not latest_snapshot_date or not _table_exists(conn, "manual_cashflow_adjustments"):
        return {"total_rows": 0, "recent_rows": 0, "latest_flow_date": None}
    latest_d = date.fromisoformat(str(latest_snapshot_date))
    cutoff = latest_d.fromordinal(latest_d.toordinal() - 30).isoformat()
    row = conn.execute(
        """
        SELECT
          COUNT(*) AS total_rows,
          SUM(CASE WHEN flow_date >= ? THEN 1 ELSE 0 END) AS recent_rows,
          MAX(flow_date) AS latest_flow_date
        FROM manual_cashflow_adjustments
        """,
        (cutoff,),
    ).fetchone()
    return {
        "total_rows": int((row["total_rows"] if row and row["total_rows"] is not None else 0) or 0),
        "recent_rows": int((row["recent_rows"] if row and row["recent_rows"] is not None else 0) or 0),
        "latest_flow_date": str(row["latest_flow_date"]) if row and row["latest_flow_date"] else None,
    }


def _reconciliation_resolution_map(conn: sqlite3.Connection) -> Dict[Tuple[str, str], Dict[str, Any]]:
    if not _table_exists(conn, "reconciliation_resolutions"):
        return {}
    rows = conn.execute(
        """
        SELECT interval_key, issue_code, status, resolution_type, created_at_utc, note
        FROM reconciliation_resolutions
        ORDER BY id DESC
        """
    ).fetchall()
    out: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for row in rows or []:
        key = (str(row["interval_key"] or ""), str(row["issue_code"] or ""))
        if key not in out:
            out[key] = dict(row)
    return out


def _confidence_label(score: float) -> str:
    if score >= 0.85:
        return "high"
    if score >= 0.6:
        return "medium"
    return "low"


def _source_basis(interval: Dict[str, Any]) -> List[str]:
    out = ["snapshots"]
    if int(interval.get("orders_total") or 0) > 0:
        out.append("orders")
    if int(interval.get("imported_rows_count") or 0) > 0:
        out.append("imported_movements")
    if int(interval.get("manual_rows_count") or 0) > 0:
        out.append("manual_adjustments")
    return out


def _quality_state_from_interval(interval: Dict[str, Any]) -> Tuple[str, str]:
    warnings = set(str(w) for w in (interval.get("quality_warnings") or []))
    if "CASH_MISSING" in warnings:
        return "missing_data", "CASH_MISSING"
    if "ORDERS_INCOMPLETE" in warnings:
        return "orders_incomplete", "ORDERS_INCOMPLETE"
    return "needs_review", "INFERENCE_PARTIAL"


def _classify_internal(interval: Dict[str, Any]) -> Optional[Tuple[str, str, float]]:
    residual_ratio = interval.get("residual_ratio")
    try:
        residual_ratio_f = float(residual_ratio) if residual_ratio is not None else None
    except Exception:
        residual_ratio_f = None
    buy = abs(float(interval.get("buy_amount_ars") or 0.0))
    sell = abs(float(interval.get("sell_amount_ars") or 0.0))
    if residual_ratio_f is not None and residual_ratio_f <= 0.03 and (buy > 0 or sell > 0):
        return ("ignore_internal", "OPERATIONAL_FEE_OR_TAX", 0.9)
    if buy > 0 and sell > 0 and residual_ratio_f is not None and residual_ratio_f <= 0.25:
        return ("ignore_internal", "ROTATION_INTERNAL", 0.75)
    return None


def _proposal_for_interval(interval: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    issue_state, issue_code = _quality_state_from_interval(interval)
    if issue_state == "missing_data":
        return {
            "issue_code": issue_code,
            "resolution_type": "import",
            "suggested_kind": None,
            "suggested_amount_ars": None,
            "confidence": "low",
            "confidence_score": 0.35,
            "reason": "Falta cash total en snapshots; primero hace falta completar datos base o importar movimientos confirmados.",
            "impact_on_inference": "keeps_warning",
        }
    if issue_state == "orders_incomplete":
        return {
            "issue_code": issue_code,
            "resolution_type": "review_orders",
            "suggested_kind": None,
            "suggested_amount_ars": None,
            "confidence": "medium",
            "confidence_score": 0.7,
            "reason": "Hay órdenes sin clasificar o sin monto; conviene revisar backfill antes de proponer cashflows.",
            "impact_on_inference": "keeps_warning",
        }

    internal = _classify_internal(interval)
    if internal is not None:
        resolution_type, internal_code, score = internal
        return {
            "issue_code": internal_code,
            "resolution_type": resolution_type,
            "suggested_kind": None,
            "suggested_amount_ars": None,
            "confidence": _confidence_label(score),
            "confidence_score": score,
            "reason": "El residual parece interno u operativo; no conviene crear un flujo externo.",
            "impact_on_inference": "clears_warning",
        }

    residual_after_manual = float(interval.get("residual_after_manual_ars") or 0.0)
    imported_rows = int(interval.get("imported_rows_count") or 0)
    traded_gross = float(interval.get("traded_gross_ars") or 0.0)
    if abs(residual_after_manual) < INTERVAL_THRESHOLD_ARS:
        return None
    suggested_kind = "deposit" if residual_after_manual > 0 else "withdraw"
    if imported_rows == 0 and traded_gross <= 0:
        score = 0.82
        return {
            "issue_code": "MISSING_IMPORTED_MOVEMENT",
            "resolution_type": "import",
            "suggested_kind": suggested_kind,
            "suggested_amount_ars": abs(residual_after_manual),
            "confidence": _confidence_label(score),
            "confidence_score": score,
            "reason": "El movimiento parece un flujo externo puro; conviene importar o registrar el movimiento de cuenta.",
            "impact_on_inference": "clears_warning",
        }
    score = 0.78 if traded_gross > 0 else 0.65
    return {
        "issue_code": "INFERENCE_PARTIAL",
        "resolution_type": "manual_adjustment",
        "suggested_kind": suggested_kind,
        "suggested_amount_ars": abs(residual_after_manual),
        "confidence": _confidence_label(score),
        "confidence_score": score,
        "reason": "El residual sigue sin reconciliar; conviene confirmar el flujo y cargar un ajuste manual auditado.",
        "impact_on_inference": "clears_warning",
    }


def _build_interval(
    conn: sqlite3.Connection,
    base_snap: shared_db.Snapshot,
    end_snap: shared_db.Snapshot,
    resolution_map: Dict[Tuple[str, str], Dict[str, Any]],
) -> Dict[str, Any]:
    warnings: List[str] = []
    base_cash = snapshot_cash_components(base_snap)
    end_cash = snapshot_cash_components(end_snap)
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
    cash_ars_delta = None if cash_ars_base is None or cash_ars_end is None else float(cash_ars_end) - float(cash_ars_base)
    cash_usd_delta = None if cash_usd_base is None or cash_usd_end is None else float(cash_usd_end) - float(cash_usd_base)

    fx_base = implied_fx_ars_per_usd(cash_total_base, cash_ars_base, cash_usd_base)
    fx_end = implied_fx_ars_per_usd(cash_total_end, cash_ars_end, cash_usd_end)
    if fx_base is not None and fx_end is not None and cash_usd_base is not None:
        fx_revaluation_ars = float(cash_usd_base) * (float(fx_end) - float(fx_base))
    else:
        fx_revaluation_ars = 0.0

    dt_from = f"{base_snap.snapshot_date}T23:59:59"
    dt_to = f"{end_snap.snapshot_date}T23:59:59"
    amounts, stats = shared_db.orders_flow_summary(conn, dt_from, dt_to, currency="peso_Argentino")
    if stats.get("unclassified", 0) > 0 or stats.get("amount_missing", 0) > 0:
        warnings.append("ORDERS_INCOMPLETE")

    buy_amount = float(amounts.get("buy_amount") or 0.0)
    sell_amount = float(amounts.get("sell_amount") or 0.0)
    income_amount = float(amounts.get("income_amount") or 0.0)
    fee_amount = float(amounts.get("fee_amount") or 0.0)
    order_fee_internal_ars = -abs(float(fee_amount or 0.0))

    imported = _aggregate_imported_movements(conn, base_snap.snapshot_date, end_snap.snapshot_date, fx_end)
    for warn in imported.get("warnings") or []:
        warnings.append(str(warn))

    external_raw = float(cash_total_delta) + buy_amount - sell_amount - income_amount
    imported_internal = float(imported.get("imported_internal_ars") or 0.0)
    imported_external = float(imported.get("imported_external_ars") or 0.0)
    external_adjusted = external_raw - float(fx_revaluation_ars) - imported_internal - order_fee_internal_ars
    external_final = imported_external if abs(imported_external) > 1e-9 else external_adjusted

    manual_adjustment_ars = float(shared_db.manual_cashflow_sum(conn, base_snap.snapshot_date, end_snap.snapshot_date) or 0.0)
    residual_after_manual = float(external_final) - float(manual_adjustment_ars)
    traded_gross = abs(buy_amount) + abs(sell_amount) + abs(income_amount) + abs(fee_amount)
    residual_ratio = (abs(external_final) / traded_gross) if traded_gross > 0 else None
    imported_rows_count = int(imported.get("rows_count") or 0)
    manual_rows_count = _manual_rows_count(conn, base_snap.snapshot_date, end_snap.snapshot_date)

    interval_key = f"{base_snap.snapshot_date}>{end_snap.snapshot_date}"
    proposal = _proposal_for_interval(
        {
            "quality_warnings": warnings,
            "residual_ratio": residual_ratio,
            "buy_amount_ars": buy_amount,
            "sell_amount_ars": sell_amount,
            "residual_after_manual_ars": residual_after_manual,
            "imported_rows_count": imported_rows_count,
            "traded_gross_ars": traded_gross,
        }
    )

    state = None
    if imported_rows_count > 0 and manual_rows_count > 0 and abs(residual_after_manual) < INTERVAL_THRESHOLD_ARS:
        state = "resolved_mixed"
    elif imported_rows_count > 0 and abs(residual_after_manual) < INTERVAL_THRESHOLD_ARS:
        state = "resolved_imported"
    elif manual_rows_count > 0 and abs(residual_after_manual) < INTERVAL_THRESHOLD_ARS:
        state = "resolved_manual"
    elif abs(external_final) < INTERVAL_THRESHOLD_ARS and not warnings:
        state = "resolved_mixed"
    if state is None:
        state, _ = _quality_state_from_interval({"quality_warnings": warnings})

    resolution = None
    if proposal is not None:
        resolution = resolution_map.get((interval_key, str(proposal.get("issue_code") or "")))
        if resolution and str(resolution.get("status") or "") == "applied" and str(resolution.get("resolution_type") or "") == "ignore_internal":
            state = "resolved_mixed"
            proposal = None

    impact = "none"
    if state in ("missing_data", "orders_incomplete", "needs_review"):
        impact = "blocking"
    elif state.startswith("resolved_"):
        impact = "cleared"

    return {
        "interval_key": interval_key,
        "base_snapshot_date": base_snap.snapshot_date,
        "end_snapshot_date": end_snap.snapshot_date,
        "state": state,
        "issue_code": str(proposal.get("issue_code")) if proposal else None,
        "impact_on_inference": impact,
        "quality_warnings": list(dict.fromkeys(warnings)),
        "cash_delta_ars": float(cash_total_delta),
        "cash_ars_delta": cash_ars_delta,
        "cash_usd_delta": cash_usd_delta,
        "fx_revaluation_ars": float(fx_revaluation_ars),
        "buy_amount_ars": buy_amount,
        "sell_amount_ars": sell_amount,
        "income_amount_ars": income_amount,
        "fee_amount_ars": fee_amount,
        "external_raw_ars": float(external_raw),
        "external_adjusted_ars": float(external_adjusted),
        "external_final_ars": float(external_final),
        "manual_adjustment_ars": float(manual_adjustment_ars),
        "residual_after_manual_ars": float(residual_after_manual),
        "residual_ratio": residual_ratio,
        "imported_rows_count": imported_rows_count,
        "imported_external_ars": float(imported_external),
        "imported_internal_ars": float(imported_internal),
        "manual_rows_count": int(manual_rows_count),
        "orders_total": int(stats.get("total", 0) or 0),
        "orders_stats": stats,
        "traded_gross_ars": float(traded_gross),
        "proposal": proposal,
        "resolution": resolution,
        "source_basis": _source_basis(
            {
                "orders_total": stats.get("total", 0),
                "imported_rows_count": imported_rows_count,
                "manual_rows_count": manual_rows_count,
            }
        ),
    }


def _snapshot_dates_for_range(
    conn: sqlite3.Connection,
    *,
    as_of: Optional[str],
    days: int,
    date_from: Optional[str],
    date_to: Optional[str],
) -> Tuple[List[str], Optional[str], Optional[str], Optional[str]]:
    latest_snapshot = _latest_snapshot_date(conn)
    if not latest_snapshot:
        return [], None, None, None
    as_of_v = str(as_of or latest_snapshot)
    to_date = min(as_of_v, latest_snapshot)
    try:
        to_d = date.fromisoformat(to_date)
    except Exception:
        return [], None, None, latest_snapshot
    if date_from:
        from_date = date_from
    else:
        from_date = to_d.fromordinal(to_d.toordinal() - max(int(days), 1)).isoformat()
    if date_to:
        to_date = date_to
    rows = conn.execute(
        """
        SELECT snapshot_date
        FROM portfolio_snapshots
        WHERE snapshot_date >= ? AND snapshot_date <= ?
        ORDER BY snapshot_date ASC
        """,
        (from_date, to_date),
    ).fetchall()
    snap_dates = [str(r["snapshot_date"]) for r in rows or []]
    return snap_dates, from_date, to_date, latest_snapshot


def _build_summary(intervals: List[Dict[str, Any]], latest_snapshot_date: Optional[str], conn: sqlite3.Connection) -> Dict[str, Any]:
    counts = {
        "resolved_imported": 0,
        "resolved_manual": 0,
        "resolved_mixed": 0,
        "needs_review": 0,
        "missing_data": 0,
        "orders_incomplete": 0,
    }
    open_intervals = 0
    suppressed_intervals = 0
    imported_resolved = 0
    manual_resolved = 0
    mixed_resolved = 0
    for row in intervals:
        state = str(row.get("state") or "")
        if state in counts:
            counts[state] += 1
        proposal = row.get("proposal")
        resolution = row.get("resolution")
        if state == "resolved_imported":
            imported_resolved += 1
        elif state == "resolved_manual":
            manual_resolved += 1
        elif state == "resolved_mixed":
            mixed_resolved += 1
        if proposal:
            if resolution and str(resolution.get("status") or "") in ("dismissed", "applied", "acknowledged"):
                suppressed_intervals += 1
            else:
                open_intervals += 1
    coverage_mode = "none"
    if imported_resolved and manual_resolved:
        coverage_mode = "mixed"
    elif imported_resolved:
        coverage_mode = "imported"
    elif manual_resolved:
        coverage_mode = "manual"
    elif mixed_resolved:
        coverage_mode = "mixed"
    import_stats = _recent_import_stats(conn, latest_snapshot_date)
    manual_stats = _recent_manual_stats(conn, latest_snapshot_date)
    if coverage_mode == "none":
        if import_stats.get("recent_rows"):
            coverage_mode = "imported"
        elif manual_stats.get("recent_rows"):
            coverage_mode = "manual"
    headline = "No hay intervalos con bloqueo activo."
    if open_intervals > 0:
        headline = f"Hay {open_intervals} intervalos para revisar antes de confiar en el retorno real."
    elif suppressed_intervals > 0:
        headline = "Hay intervalos ya revisados; el warning queda auditado hasta que cambien los datos."
    return {
        "coverage_mode": coverage_mode,
        "open_intervals": int(open_intervals),
        "suppressed_intervals": int(suppressed_intervals),
        "counts": counts,
        "headline": headline,
        "import_stats": import_stats,
        "manual_stats": manual_stats,
    }


def _insert_run(conn: sqlite3.Connection, *, as_of: str, date_from: str, date_to: str, days: int, summary: Dict[str, Any]) -> int:
    cur = conn.execute(
        """
        INSERT INTO reconciliation_runs(
          created_at_utc, as_of, date_from, date_to, days, status, summary_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            _utc_now_iso(),
            str(as_of),
            str(date_from),
            str(date_to),
            int(days),
            "ok",
            json.dumps(summary, ensure_ascii=True),
        ),
    )
    conn.commit()
    return int(cur.lastrowid)


def _insert_interval(conn: sqlite3.Connection, run_id: int, row: Dict[str, Any]) -> int:
    cur = conn.execute(
        """
        INSERT INTO reconciliation_intervals(
          run_id, interval_key, base_snapshot_date, end_snapshot_date, state, issue_code,
          confidence, impact_on_inference, analysis_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            int(run_id),
            str(row.get("interval_key") or ""),
            str(row.get("base_snapshot_date") or ""),
            str(row.get("end_snapshot_date") or ""),
            str(row.get("state") or ""),
            row.get("issue_code"),
            (row.get("proposal") or {}).get("confidence"),
            str(row.get("impact_on_inference") or "none"),
            json.dumps(
                {
                    k: v
                    for k, v in row.items()
                    if k not in ("proposal", "resolution")
                },
                ensure_ascii=True,
            ),
        ),
    )
    conn.commit()
    return int(cur.lastrowid)


def _insert_proposal(conn: sqlite3.Connection, run_id: int, interval_id: int, interval: Dict[str, Any], proposal: Dict[str, Any]) -> int:
    cur = conn.execute(
        """
        INSERT INTO reconciliation_proposals(
          run_id, interval_id, interval_key, issue_code, resolution_type, suggested_kind,
          suggested_amount_ars, confidence, confidence_score, reason, source_basis,
          impact_on_inference, status, created_at_utc
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            int(run_id),
            int(interval_id),
            str(interval.get("interval_key") or ""),
            str(proposal.get("issue_code") or ""),
            str(proposal.get("resolution_type") or ""),
            proposal.get("suggested_kind"),
            proposal.get("suggested_amount_ars"),
            str(proposal.get("confidence") or "low"),
            float(proposal.get("confidence_score") or 0.0),
            str(proposal.get("reason") or ""),
            json.dumps(interval.get("source_basis") or [], ensure_ascii=True),
            str(proposal.get("impact_on_inference") or "keeps_warning"),
            "open",
            _utc_now_iso(),
        ),
    )
    conn.commit()
    return int(cur.lastrowid)


def _proposal_row_to_dict(row: sqlite3.Row) -> Dict[str, Any]:
    out = dict(row)
    out["source_basis"] = _loads_json(out.get("source_basis"), [])
    return out


def _interval_row_to_dict(row: sqlite3.Row) -> Dict[str, Any]:
    out = dict(row)
    out["analysis"] = _loads_json(out.get("analysis_json"), {})
    out.pop("analysis_json", None)
    return out


def _run_row_to_dict(row: sqlite3.Row, conn: sqlite3.Connection) -> Dict[str, Any]:
    out = dict(row)
    out["summary"] = _loads_json(out.get("summary_json"), {})
    out.pop("summary_json", None)
    intervals = conn.execute(
        """
        SELECT *
        FROM reconciliation_intervals
        WHERE run_id = ?
        ORDER BY end_snapshot_date DESC, id DESC
        """,
        (int(row["id"]),),
    ).fetchall()
    proposals = conn.execute(
        """
        SELECT *
        FROM reconciliation_proposals
        WHERE run_id = ?
        ORDER BY confidence_score DESC, id DESC
        """,
        (int(row["id"]),),
    ).fetchall()
    out["intervals"] = [_interval_row_to_dict(r) for r in intervals]
    out["proposals"] = [_proposal_row_to_dict(r) for r in proposals]
    return out


def get_latest_payload(conn: sqlite3.Connection, *, as_of: Optional[str] = None, ensure: bool = True) -> Dict[str, Any]:
    _ensure_reconciliation_schema(conn)
    row = None
    if _table_exists(conn, "reconciliation_runs"):
        if as_of:
            row = conn.execute(
                """
                SELECT *
                FROM reconciliation_runs
                WHERE as_of = ?
                ORDER BY id DESC
                LIMIT 1
                """,
                (str(as_of),),
            ).fetchone()
        if row is None:
            row = conn.execute(
                """
                SELECT *
                FROM reconciliation_runs
                ORDER BY id DESC
                LIMIT 1
                """
            ).fetchone()
    if row is None and ensure:
        return ensure_latest_run(conn, as_of=as_of)
    return _run_row_to_dict(row, conn) if row is not None else {"summary": {}, "intervals": [], "proposals": []}


def _open_proposals_from_run(run_payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    out = []
    intervals_by_id = {int(row["id"]): row for row in (run_payload.get("intervals") or []) if row.get("id") is not None}
    for proposal in run_payload.get("proposals") or []:
        status = str(proposal.get("status") or "open")
        if status not in PROPOSAL_OPEN_STATUSES:
            continue
        interval = intervals_by_id.get(int(proposal.get("interval_id") or 0))
        merged = dict(proposal)
        if interval:
            merged["interval"] = interval
        out.append(merged)
    return out


def get_open_payload(conn: sqlite3.Connection, *, as_of: Optional[str] = None, ensure: bool = True) -> Dict[str, Any]:
    _ensure_reconciliation_schema(conn)
    run_payload = get_latest_payload(conn, as_of=as_of, ensure=ensure)
    return {
        "run": {
            "id": run_payload.get("id"),
            "as_of": run_payload.get("as_of"),
            "created_at_utc": run_payload.get("created_at_utc"),
            "status": run_payload.get("status"),
            "summary": run_payload.get("summary") or {},
        },
        "rows": _open_proposals_from_run(run_payload),
    }


def run_reconciliation(
    conn: sqlite3.Connection,
    *,
    as_of: Optional[str] = None,
    days: int = 30,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    force: bool = False,
) -> Dict[str, Any]:
    _ensure_reconciliation_schema(conn)
    if not _table_exists(conn, "portfolio_snapshots"):
        return {"summary": {"headline": "Sin snapshots para conciliar."}, "intervals": [], "proposals": []}
    snap_dates, from_date, to_date, latest_snapshot = _snapshot_dates_for_range(
        conn,
        as_of=as_of,
        days=days,
        date_from=date_from,
        date_to=date_to,
    )
    if not snap_dates:
        return {"summary": {"headline": "Sin snapshots para conciliar."}, "intervals": [], "proposals": []}
    as_of_v = str(as_of or to_date or latest_snapshot or "")
    if not force and _table_exists(conn, "reconciliation_runs"):
        existing = conn.execute(
            """
            SELECT *
            FROM reconciliation_runs
            WHERE as_of = ? AND date_from = ? AND date_to = ? AND days = ?
            ORDER BY id DESC
            LIMIT 1
            """,
            (str(as_of_v), str(from_date), str(to_date), int(days)),
        ).fetchone()
        if existing:
            return _run_row_to_dict(existing, conn)

    resolution_map = _reconciliation_resolution_map(conn)
    intervals: List[Dict[str, Any]] = []
    for idx in range(1, len(snap_dates)):
        base_snap = shared_db.snapshot_on_or_before(conn, snap_dates[idx - 1])
        end_snap = shared_db.snapshot_on_or_before(conn, snap_dates[idx])
        if not base_snap or not end_snap or base_snap.snapshot_date == end_snap.snapshot_date:
            continue
        intervals.append(_build_interval(conn, base_snap, end_snap, resolution_map))

    summary = _build_summary(intervals, latest_snapshot, conn)
    run_id = _insert_run(conn, as_of=as_of_v, date_from=str(from_date), date_to=str(to_date), days=int(days), summary=summary)
    for interval in intervals:
        interval_id = _insert_interval(conn, run_id, interval)
        interval["id"] = interval_id
        proposal = interval.get("proposal")
        resolution = interval.get("resolution")
        if proposal and not (resolution and str(resolution.get("status") or "") in ("dismissed", "applied", "acknowledged")):
            proposal_id = _insert_proposal(conn, run_id, interval_id, interval, proposal)
            proposal["id"] = proposal_id
        elif proposal and resolution:
            proposal["status"] = str(resolution.get("status") or "dismissed")
    row = conn.execute("SELECT * FROM reconciliation_runs WHERE id = ?", (int(run_id),)).fetchone()
    return _run_row_to_dict(row, conn) if row else {"summary": summary, "intervals": intervals, "proposals": []}


def ensure_latest_run(conn: sqlite3.Connection, *, as_of: Optional[str] = None, days: int = 30) -> Dict[str, Any]:
    _ensure_reconciliation_schema(conn)
    latest_snapshot = _latest_snapshot_date(conn)
    as_of_v = str(as_of or latest_snapshot or "")
    row = None
    if _table_exists(conn, "reconciliation_runs") and as_of_v:
        row = conn.execute(
            """
            SELECT *
            FROM reconciliation_runs
            WHERE as_of = ?
            ORDER BY id DESC
            LIMIT 1
            """,
            (as_of_v,),
        ).fetchone()
    if row is not None:
        return _run_row_to_dict(row, conn)
    return run_reconciliation(conn, as_of=as_of_v or None, days=days, force=True)


def _log_reconciliation_event(
    conn: sqlite3.Connection,
    *,
    event_type: str,
    title: str,
    description: str,
    snapshot_date: Optional[str],
    payload: Dict[str, Any],
) -> None:
    if not _table_exists(conn, "advisor_events"):
        return
    conn.execute(
        """
        INSERT INTO advisor_events(created_at, event_type, title, description, symbol, snapshot_date, alert_id, payload_json)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            _utc_now_iso(),
            str(event_type),
            str(title),
            str(description),
            None,
            snapshot_date,
            None,
            json.dumps(payload, ensure_ascii=True),
        ),
    )
    conn.commit()


def _proposal_by_id(conn: sqlite3.Connection, proposal_id: int) -> Optional[sqlite3.Row]:
    if not _table_exists(conn, "reconciliation_proposals"):
        return None
    return conn.execute(
        """
        SELECT *
        FROM reconciliation_proposals
        WHERE id = ?
        """,
        (int(proposal_id),),
    ).fetchone()


def _insert_resolution(
    conn: sqlite3.Connection,
    *,
    proposal_row: sqlite3.Row,
    status: str,
    note: Optional[str],
    manual_cashflow_id: Optional[int],
) -> int:
    cur = conn.execute(
        """
        INSERT INTO reconciliation_resolutions(
          proposal_id, interval_id, interval_key, issue_code, resolution_type, status,
          manual_cashflow_id, note, created_at_utc
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            int(proposal_row["id"]),
            int(proposal_row["interval_id"]),
            str(proposal_row["interval_key"] or ""),
            str(proposal_row["issue_code"] or ""),
            str(proposal_row["resolution_type"] or ""),
            str(status),
            manual_cashflow_id,
            note,
            _utc_now_iso(),
        ),
    )
    conn.commit()
    return int(cur.lastrowid)


def apply_proposal(conn: sqlite3.Connection, proposal_id: int, *, note: Optional[str] = None) -> Dict[str, Any]:
    _ensure_reconciliation_schema(conn)
    row = _proposal_by_id(conn, int(proposal_id))
    if row is None:
        raise ValueError("proposal not found")
    if str(row["status"] or "") not in PROPOSAL_OPEN_STATUSES:
        raise ValueError("proposal is not open")
    resolution_type = str(row["resolution_type"] or "")
    manual_cashflow_id = None
    applied_status = "applied"
    summary_note = note
    interval_row = conn.execute(
        "SELECT end_snapshot_date FROM reconciliation_intervals WHERE id = ?",
        (int(row["interval_id"]),),
    ).fetchone()
    snapshot_date = str(interval_row["end_snapshot_date"]) if interval_row and interval_row["end_snapshot_date"] else None
    if resolution_type == "manual_adjustment":
        amount = float(row["suggested_amount_ars"] or 0.0)
        if amount <= 0:
            raise ValueError("manual adjustment proposal has invalid amount")
        cashflow = shared_db.add_manual_cashflow_adjustment(
            conn,
            snapshot_date or _latest_snapshot_date(conn) or date.today().isoformat(),
            str(row["suggested_kind"] or "correction"),
            float(amount),
            summary_note or f"Ajuste sugerido por conciliacion #{int(row['id'])}",
        )
        manual_cashflow_id = int(cashflow["id"])
    elif resolution_type in ("import", "review_orders"):
        applied_status = "acknowledged"
    elif resolution_type != "ignore_internal":
        raise ValueError("unsupported resolution type")

    conn.execute(
        "UPDATE reconciliation_proposals SET status = ?, applied_at_utc = ? WHERE id = ?",
        (applied_status, _utc_now_iso(), int(row["id"])),
    )
    conn.commit()
    resolution_id = _insert_resolution(
        conn,
        proposal_row=row,
        status=applied_status,
        note=summary_note,
        manual_cashflow_id=manual_cashflow_id,
    )
    _log_reconciliation_event(
        conn,
        event_type="reconciliation",
        title=f"Conciliacion aplicada #{int(row['id'])}",
        description=f"Se aplico la propuesta {resolution_type} para {row['interval_key']}.",
        snapshot_date=snapshot_date,
        payload={
            "proposal_id": int(row["id"]),
            "resolution_id": int(resolution_id),
            "resolution_type": resolution_type,
            "status": applied_status,
            "manual_cashflow_id": manual_cashflow_id,
        },
    )
    latest = _latest_snapshot_date(conn)
    fresh = run_reconciliation(conn, as_of=latest, days=30, force=True)
    return {
        "ok": True,
        "proposal_id": int(row["id"]),
        "resolution_id": int(resolution_id),
        "status": applied_status,
        "manual_cashflow_id": manual_cashflow_id,
        "latest_run_id": fresh.get("id"),
    }


def dismiss_proposal(conn: sqlite3.Connection, proposal_id: int, *, reason: str) -> Dict[str, Any]:
    _ensure_reconciliation_schema(conn)
    row = _proposal_by_id(conn, int(proposal_id))
    if row is None:
        raise ValueError("proposal not found")
    if str(row["status"] or "") not in PROPOSAL_OPEN_STATUSES:
        raise ValueError("proposal is not open")
    reason_v = str(reason or "").strip()
    if not reason_v:
        raise ValueError("reason is required")
    interval_row = conn.execute(
        "SELECT end_snapshot_date FROM reconciliation_intervals WHERE id = ?",
        (int(row["interval_id"]),),
    ).fetchone()
    snapshot_date = str(interval_row["end_snapshot_date"]) if interval_row and interval_row["end_snapshot_date"] else None
    conn.execute(
        "UPDATE reconciliation_proposals SET status = ?, applied_at_utc = ? WHERE id = ?",
        ("dismissed", _utc_now_iso(), int(row["id"])),
    )
    conn.commit()
    resolution_id = _insert_resolution(
        conn,
        proposal_row=row,
        status="dismissed",
        note=reason_v,
        manual_cashflow_id=None,
    )
    _log_reconciliation_event(
        conn,
        event_type="reconciliation",
        title=f"Conciliacion descartada #{int(row['id'])}",
        description=f"Se descarto la propuesta {row['resolution_type']} para {row['interval_key']}.",
        snapshot_date=snapshot_date,
        payload={
            "proposal_id": int(row["id"]),
            "resolution_id": int(resolution_id),
            "status": "dismissed",
            "reason": reason_v,
        },
    )
    return {"ok": True, "proposal_id": int(row["id"]), "resolution_id": int(resolution_id), "status": "dismissed"}


def explain_interval(conn: sqlite3.Connection, interval_id: int) -> Dict[str, Any]:
    _ensure_reconciliation_schema(conn)
    if not _table_exists(conn, "reconciliation_intervals"):
        raise ValueError("reconciliation data not found")
    row = conn.execute(
        """
        SELECT *
        FROM reconciliation_intervals
        WHERE id = ?
        """,
        (int(interval_id),),
    ).fetchone()
    if row is None:
        raise ValueError("interval not found")
    out = _interval_row_to_dict(row)
    proposals = conn.execute(
        """
        SELECT *
        FROM reconciliation_proposals
        WHERE interval_id = ?
        ORDER BY id DESC
        """,
        (int(interval_id),),
    ).fetchall()
    resolutions = []
    if _table_exists(conn, "reconciliation_resolutions"):
        resolutions = conn.execute(
            """
            SELECT *
            FROM reconciliation_resolutions
            WHERE interval_id = ?
            ORDER BY id DESC
            """,
            (int(interval_id),),
        ).fetchall()
    out["proposals"] = [_proposal_row_to_dict(r) for r in proposals]
    out["resolutions"] = [dict(r) for r in resolutions]
    return out
