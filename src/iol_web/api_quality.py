from __future__ import annotations

import json
from datetime import date
from typing import Any, Callable, Dict, List, Optional, Tuple

from fastapi import APIRouter

from iol_reconciliation.service import ensure_latest_run as ensure_latest_reconciliation_run

from . import db as dbmod
from .metrics import target_date


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


def _movements_coverage_stats(conn, as_of: str) -> Dict[str, Any]:
    cols = {r[1] for r in conn.execute("PRAGMA table_info(account_cash_movements)").fetchall()}
    if not cols:
        return {"income_total": 0, "income_with_symbol": 0, "coverage_pct": 0.0}
    symbol_col = "symbol" if "symbol" in cols else "NULL"
    cutoff = target_date(as_of, 90)
    row = conn.execute(
        f"""
        SELECT
            COUNT(*) AS income_total,
            SUM(CASE WHEN {symbol_col} IS NOT NULL AND {symbol_col} != '' THEN 1 ELSE 0 END) AS income_with_symbol
        FROM account_cash_movements
        WHERE kind IN ('dividend_income', 'coupon_income', 'bond_amortization_income')
        AND movement_date >= ?
        """,
        (cutoff,),
    ).fetchone()
    total = int(row["income_total"] or 0) if row else 0
    with_sym = int(row["income_with_symbol"] or 0) if row else 0
    pct = (with_sym / total * 100.0) if total > 0 else 0.0
    return {"income_total": total, "income_with_symbol": with_sym, "coverage_pct": round(pct, 1)}


def _fee_linkage_stats(conn, as_of: str) -> Dict[str, Any]:
    cols_fees = {r[1] for r in conn.execute("PRAGMA table_info(order_fees)").fetchall()}
    if not cols_fees:
        return {"fees_linked": 0, "fees_total": 0, "linkage_pct": 0.0}
    cols_mv = {r[1] for r in conn.execute("PRAGMA table_info(account_cash_movements)").fetchall()}
    cutoff = target_date(as_of, 90)
    fees_linked = int(conn.execute(
        "SELECT COUNT(*) FROM order_fees WHERE occurred_at >= ?", (cutoff,)
    ).fetchone()[0] or 0)
    fees_total = 0
    if cols_mv:
        fees_total = int(conn.execute(
            "SELECT COUNT(*) FROM account_cash_movements WHERE kind = 'operational_fee_or_tax' AND movement_date >= ?",
            (cutoff,),
        ).fetchone()[0] or 0)
    pct = (fees_linked / fees_total * 100.0) if fees_total > 0 else (100.0 if fees_linked == 0 else 0.0)
    return {"fees_linked": fees_linked, "fees_total": fees_total, "linkage_pct": round(pct, 1)}


def _fee_alerts_count(conn) -> int:
    cols = {r[1] for r in conn.execute("PRAGMA table_info(advisor_alerts)").fetchall()}
    if not cols or "kind" not in cols:
        return 0
    status_filter = "AND (status IS NULL OR status = 'open')" if "status" in cols else ""
    row = conn.execute(
        f"SELECT COUNT(*) FROM advisor_alerts WHERE kind = 'fee_discrepancy' {status_filter}"
    ).fetchone()
    return int(row[0] or 0)


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


def build_quality_router(
    *,
    returns_fn: Callable[[], Dict[str, Any]],
    monthly_kpi_fn: Callable[[], Dict[str, Any]],
    snapshot_data_freshness_fn: Callable[[Any], Dict[str, Any]],
) -> Tuple[APIRouter, Callable[[], Dict[str, Any]]]:
    router = APIRouter()

    @router.get("/quality")
    def quality():
        ret = returns_fn()
        monthly_kpi = monthly_kpi_fn()
        try:
            conn = dbmod.get_conn_rw()
        except FileNotFoundError:
            return {"rows": []}

        try:
            latest = dbmod.latest_snapshot(conn)
            latest_snapshot_date = latest.snapshot_date if latest else date.today().isoformat()
            period_blocks = [
                {"label": "Dia", "block": ret.get("daily") or {}},
                {"label": "Semana", "block": ret.get("weekly") or {}},
                {"label": "Mes", "block": ret.get("monthly") or {}},
                {"label": "Ano", "block": ret.get("yearly") or {}},
                {"label": "Desde inicio", "block": ret.get("inception") or {}},
            ]

            warn_set = set()
            warns_by_source: List[str] = []
            coverage_count = 0
            for item in period_blocks:
                block = item["block"]
                has_valid = bool(block.get("from")) and bool(block.get("to")) and str(block.get("from")) != str(block.get("to"))
                if has_valid:
                    coverage_count += 1
                warns = [str(w) for w in (block.get("quality_warnings") or [])]
                if warns:
                    warns_by_source.append(f"{item['label']}: {', '.join(warns)}")
                for warning in warns:
                    warn_set.add(warning)
            monthly_warns = [str(w) for w in (monthly_kpi.get("quality_warnings") or [])]
            if monthly_warns:
                warns_by_source.append(f"KPI mensual: {', '.join(monthly_warns)}")
            for warning in monthly_warns:
                warn_set.add(warning)

            reconciliation_summary = _reconciliation_quality_summary(conn, latest_snapshot_date)
            reconciliation_kind = _reconciliation_kind(reconciliation_summary)
            reconciliation_sources = [
                f"Cobertura: {str(reconciliation_summary.get('coverage_mode') or 'none')}",
                f"Abiertos: {int(reconciliation_summary.get('open_intervals') or 0)}",
                f"Importados 30d: {int(((reconciliation_summary.get('import_stats') or {}).get('recent_rows') or 0))}",
                f"Manuales 30d: {int(((reconciliation_summary.get('manual_stats') or {}).get('recent_rows') or 0))}",
            ]

            critical_warns = ["CASH_MISSING", "ORDERS_INCOMPLETE", "INFERENCE_PARTIAL"]
            critical_count = len([warning for warning in critical_warns if warning in warn_set])
            inference_kind = "ok"
            inference_value = "OK"
            inference_detail = "No se detectan senales criticas de inferencia."
            if reconciliation_kind == "warn":
                inference_kind = "warn"
                inference_value = f"Revisar ({int(reconciliation_summary.get('open_intervals') or 0)})"
                inference_detail = str(reconciliation_summary.get("headline") or "Hay intervalos pendientes de conciliacion.")
            elif reconciliation_kind == "ok":
                coverage_mode = str(reconciliation_summary.get("coverage_mode") or "manual")
                inference_kind = "ok"
                inference_value = {
                    "imported": "Importado",
                    "manual": "Manual OK",
                    "mixed": "Mixto",
                }.get(coverage_mode, "OK")
                inference_detail = str(reconciliation_summary.get("headline") or "La inferencia quedo conciliada.")
            elif critical_count > 0:
                inference_kind = "warn"
                inference_value = f"Revisar ({critical_count})"
                inference_detail = "El retorno real sigue dependiendo de inferencias parciales o cobertura incompleta."
            elif warn_set:
                inference_kind = "info"
                inference_value = "Estimado"
                inference_detail = "Hay senales informativas; el calculo es usable pero no totalmente confirmado."

            freshness = snapshot_data_freshness_fn(latest)
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
                "La conciliacion usa movimientos importados confirmados."
                if coverage_mode == "imported"
                else (
                    "La conciliacion quedo resuelta con ajustes manuales auditados."
                    if coverage_mode == "manual"
                    else (
                        "La conciliacion combina movimientos importados y ajustes manuales."
                        if coverage_mode == "mixed"
                        else "No hay cobertura suficiente de cashflows; falta importar movimientos o confirmar ajustes."
                    )
                )
            )

            mov_cov = _movements_coverage_stats(conn, latest_snapshot_date)
            mov_cov_total = int(mov_cov.get("income_total") or 0)
            mov_cov_pct = float(mov_cov.get("coverage_pct") or 0.0)
            mov_cov_kind = "ok" if mov_cov_pct >= 80 else ("info" if mov_cov_pct >= 40 else ("warn" if mov_cov_total > 0 else "info"))
            mov_cov_value = f"{mov_cov_pct:.0f}% con símbolo" if mov_cov_total > 0 else "Sin ingresos"
            mov_cov_detail = (
                "Los movimientos de ingresos (dividendos, cupones, amortizaciones) tienen símbolo identificado."
                if mov_cov_pct >= 80
                else (
                    "Parte de los ingresos no tienen símbolo identificado — correr 'movements sync' para completar."
                    if mov_cov_total > 0
                    else "No hay movimientos de ingresos registrados en los últimos 90 días."
                )
            )

            fee_lnk = _fee_linkage_stats(conn, latest_snapshot_date)
            fee_lnk_linked = int(fee_lnk.get("fees_linked") or 0)
            fee_lnk_total = int(fee_lnk.get("fees_total") or 0)
            fee_lnk_pct = float(fee_lnk.get("linkage_pct") or 0.0)
            fee_lnk_kind = "ok" if fee_lnk_pct >= 80 else ("info" if fee_lnk_total == 0 else "warn")
            fee_lnk_value = f"{fee_lnk_linked} vinculadas" if fee_lnk_total > 0 else "Sin comisiones"
            fee_lnk_detail = (
                "Las comisiones están vinculadas a operaciones de compra/venta — verificación activa."
                if fee_lnk_pct >= 80
                else (
                    "Correr 'movements link-fees' para vincular comisiones a sus operaciones."
                    if fee_lnk_total > 0
                    else "No hay comisiones registradas en los últimos 90 días."
                )
            )

            fee_alert_count = _fee_alerts_count(conn)
            fee_alert_kind = "ok" if fee_alert_count == 0 else ("warn" if fee_alert_count <= 3 else "warn")
            fee_alert_value = "Sin alertas" if fee_alert_count == 0 else f"{fee_alert_count} alertas"
            fee_alert_detail = (
                "No se detectaron discrepancias en comisiones respecto a la tarifa del tier."
                if fee_alert_count == 0
                else f"Hay {fee_alert_count} alerta(s) de comisión fuera de rango — revisar con 'movements check-fees'."
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
                    evidence_detail = "La evidencia reciente esta dentro de la ventana operativa."
                elif age_days is not None and age_days <= 21:
                    evidence_kind = "info"
                    evidence_value = f"{age_days}d"
                    evidence_detail = "La evidencia empieza a perder frescura para oportunidades nuevas."
                else:
                    evidence_value = "Vieja"
                    evidence_detail = "La evidencia disponible esta vieja para operar con confianza."

            run_health = _latest_run_quality(conn)
            metrics = dict(run_health.get("run_metrics") or {})
            dispersion = float(metrics.get("score_dispersion") or 0.0)
            fresh_ratio = float(metrics.get("fresh_evidence_ratio") or 0.0)
            scoring_kind = "warn"
            scoring_value = "Sin run"
            scoring_detail = "Todavia no hay una corrida reciente de oportunidades con metricas registradas."
            if metrics:
                if dispersion >= 10.0 and fresh_ratio >= 0.30:
                    scoring_kind = "ok"
                    scoring_value = f"Disp. {dispersion:.1f}"
                    scoring_detail = "El scoring muestra dispersion suficiente y evidencia fresca razonable."
                elif dispersion >= 5.0:
                    scoring_kind = "info"
                    scoring_value = f"Disp. {dispersion:.1f}"
                    scoring_detail = "El scoring discrimina algo, pero todavia hay margen para mayor senal."
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
                ipc_detail = "El IPC del mes actual se proyecta con informacion parcial."
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
                ipc_detail = "Faltan snapshots para comparar contra inflacion."

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
                    "Ultima foto de cartera y retrieval operativo.",
                    sources=[
                        f"Snapshot: {freshness.get('snapshot_date') or '-'}",
                        f"Retrieval: {freshness.get('retrieved_at') or '-'}",
                        f"Dias stale: {freshness.get('days_stale')}",
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
                        f"Ultimo movimiento: {cashflow_stats.get('latest_movement_date') or '-'}",
                    ],
                    codes=[
                        "CASHFLOW_IMPORTED" if imported_recent > 0 else "CASHFLOW_IMPORT_MISSING",
                        f"COVERAGE_{coverage_mode.upper()}",
                    ],
                ),
                _quality_row(
                    "reconciliation_queue",
                    "Cola de conciliacion",
                    f"{int(reconciliation_summary.get('open_intervals') or 0)} abiertas",
                    reconciliation_kind,
                    str(reconciliation_summary.get("headline") or "Sin datos de conciliacion."),
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
                        f"Ultima evidencia: {latest_evidence or '-'}",
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
                    "Cuantas ventanas de retorno tienen base historica usable.",
                    sources=[f"{item['label']}: {item['block'].get('from')} -> {item['block'].get('to')}" for item in period_blocks],
                ),
                _quality_row(
                    "movements_coverage",
                    "Cobertura de ingresos",
                    mov_cov_value,
                    mov_cov_kind,
                    mov_cov_detail,
                    sources=[
                        f"Ingresos 90d: {mov_cov_total}",
                        f"Con símbolo: {int(mov_cov.get('income_with_symbol') or 0)}",
                        f"Cobertura: {mov_cov_pct:.1f}%",
                    ],
                    codes=["INCOME_SYMBOL_OK" if mov_cov_pct >= 80 else "INCOME_SYMBOL_PARTIAL"],
                ),
                _quality_row(
                    "fee_linkage",
                    "Vinculación de comisiones",
                    fee_lnk_value,
                    fee_lnk_kind,
                    fee_lnk_detail,
                    sources=[
                        f"Comisiones 90d: {fee_lnk_total}",
                        f"Vinculadas: {fee_lnk_linked}",
                        f"Cobertura: {fee_lnk_pct:.1f}%",
                    ],
                    codes=["FEE_LINKED_OK" if fee_lnk_pct >= 80 else ("FEE_UNLINKED" if fee_lnk_total > 0 else "FEE_NONE")],
                ),
                _quality_row(
                    "fee_alerts",
                    "Alertas de comisiones",
                    fee_alert_value,
                    fee_alert_kind,
                    fee_alert_detail,
                    sources=[f"Alertas abiertas: {fee_alert_count}"],
                    codes=["FEE_ALERTS_CLEAN" if fee_alert_count == 0 else f"FEE_ALERTS_{fee_alert_count}"],
                ),
            ]
            return {"rows": rows, "meta": {"snapshot_date": latest_snapshot_date}}
        finally:
            conn.close()

    return router, quality
