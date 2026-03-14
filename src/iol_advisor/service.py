from __future__ import annotations

import json
import os
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any, Dict, Iterable, List, Optional

from iol_cli.advisor_context import build_advisor_context_from_db_path
from iol_cli.db import connect, ensure_columns, resolve_db_path


DEFAULT_SOURCE_POLICY = "strict_official_reuters"
BRIEFING_CADENCES = {"daily", "weekly"}
BRIEFING_STATUSES = {"ok", "warn", "blocked", "error"}


@dataclass
class BriefingBundle:
    briefing: Dict[str, Any]
    reused: bool = False


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _loads_json(v: Any, default: Any) -> Any:
    if v is None:
        return default
    if isinstance(v, (dict, list)):
        return v
    try:
        obj = json.loads(str(v))
    except Exception:
        return default
    return obj if isinstance(obj, type(default)) else default


def _ensure_advisor_schema(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS advisor_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at TEXT NOT NULL,
            snapshot_date TEXT,
            prompt TEXT NOT NULL,
            response TEXT NOT NULL,
            env TEXT,
            base_url TEXT
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS advisor_opportunity_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at_utc TEXT NOT NULL,
            as_of TEXT NOT NULL,
            mode TEXT NOT NULL,
            universe TEXT NOT NULL,
            budget_ars REAL NOT NULL,
            top_n INTEGER NOT NULL,
            variant_id INTEGER,
            score_version TEXT,
            status TEXT NOT NULL,
            error_message TEXT,
            config_json TEXT NOT NULL,
            pipeline_warnings_json TEXT,
            run_metrics_json TEXT
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS advisor_opportunity_candidates (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id INTEGER NOT NULL,
            symbol TEXT NOT NULL,
            candidate_type TEXT NOT NULL,
            signal_side TEXT,
            signal_family TEXT,
            score_version TEXT,
            score_total REAL NOT NULL,
            score_risk REAL NOT NULL,
            score_value REAL NOT NULL,
            score_momentum REAL NOT NULL,
            score_catalyst REAL NOT NULL,
            entry_low REAL,
            entry_high REAL,
            suggested_weight_pct REAL,
            suggested_amount_ars REAL,
            reason_summary TEXT NOT NULL,
            risk_flags_json TEXT,
            filters_passed INTEGER NOT NULL,
            expert_signal_score REAL,
            trusted_refs_count INTEGER,
            consensus_state TEXT,
            decision_gate TEXT,
            candidate_status TEXT,
            evidence_summary_json TEXT,
            liquidity_score REAL,
            sector_bucket TEXT,
            is_crypto_proxy INTEGER,
            holding_context_json TEXT,
            score_features_json TEXT,
            FOREIGN KEY(run_id) REFERENCES advisor_opportunity_runs(id)
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS advisor_briefings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at_utc TEXT NOT NULL,
            as_of TEXT NOT NULL,
            cadence TEXT NOT NULL,
            status TEXT NOT NULL,
            source_policy TEXT NOT NULL,
            title TEXT,
            summary_md TEXT NOT NULL,
            recommendations_json TEXT NOT NULL,
            watchlist_json TEXT NOT NULL,
            quality_json TEXT NOT NULL,
            market_notes_json TEXT NOT NULL,
            links_json TEXT,
            opportunity_run_id INTEGER,
            advisor_log_id INTEGER,
            FOREIGN KEY(opportunity_run_id) REFERENCES advisor_opportunity_runs(id),
            FOREIGN KEY(advisor_log_id) REFERENCES advisor_logs(id)
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS advisor_model_variants (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            status TEXT NOT NULL,
            created_at_utc TEXT NOT NULL,
            config_json TEXT NOT NULL,
            objective TEXT NOT NULL,
            promoted_from_variant_id INTEGER,
            promoted_at_utc TEXT
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS advisor_run_regressions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id INTEGER NOT NULL,
            cadence TEXT NOT NULL,
            variant_id INTEGER NOT NULL,
            baseline_variant_id INTEGER NOT NULL,
            window_days INTEGER NOT NULL,
            scorecard_json TEXT NOT NULL,
            gate_status TEXT NOT NULL,
            regression_flags_json TEXT,
            FOREIGN KEY(run_id) REFERENCES advisor_opportunity_runs(id),
            FOREIGN KEY(variant_id) REFERENCES advisor_model_variants(id),
            FOREIGN KEY(baseline_variant_id) REFERENCES advisor_model_variants(id)
        )
        """
    )
    conn.commit()
    ensure_columns(
        conn,
        "advisor_opportunity_runs",
        {
            "variant_id": "INTEGER",
            "score_version": "TEXT",
            "pipeline_warnings_json": "TEXT",
            "run_metrics_json": "TEXT",
        },
    )
    ensure_columns(
        conn,
        "advisor_opportunity_candidates",
        {
            "expert_signal_score": "REAL",
            "trusted_refs_count": "INTEGER",
            "consensus_state": "TEXT",
            "decision_gate": "TEXT",
            "candidate_status": "TEXT",
            "evidence_summary_json": "TEXT",
            "liquidity_score": "REAL",
            "sector_bucket": "TEXT",
            "is_crypto_proxy": "INTEGER",
            "signal_side": "TEXT",
            "signal_family": "TEXT",
            "score_version": "TEXT",
            "holding_context_json": "TEXT",
            "score_features_json": "TEXT",
        },
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_advisor_logs_created ON advisor_logs(created_at)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_opp_runs_asof ON advisor_opportunity_runs(as_of)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_advisor_briefings_asof ON advisor_briefings(as_of, cadence, created_at_utc DESC)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_advisor_briefings_status ON advisor_briefings(status, cadence)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_opp_candidates_run_score ON advisor_opportunity_candidates(run_id, score_total DESC)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_opp_candidates_signal ON advisor_opportunity_candidates(run_id, signal_side, signal_family, score_total DESC)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_model_variants_status ON advisor_model_variants(status)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_run_regressions_variant_cadence ON advisor_run_regressions(variant_id, cadence, id DESC)")
    conn.commit()


@contextmanager
def _override_db_env(db_path: str):
    prev = os.environ.get("IOL_DB_PATH")
    os.environ["IOL_DB_PATH"] = db_path
    try:
        yield
    finally:
        if prev is None:
            os.environ.pop("IOL_DB_PATH", None)
        else:
            os.environ["IOL_DB_PATH"] = prev


def _quality_kind_rank(kind: str) -> int:
    mapping = {"error": 0, "warn": 1, "info": 2, "ok": 3}
    return mapping.get(str(kind or "").strip().lower(), 2)


def _find_quality_row(rows: Iterable[Dict[str, Any]], row_id: str) -> Dict[str, Any]:
    for row in rows or []:
        if str((row or {}).get("id") or "") == row_id:
            return dict(row)
    return {}


def _latest_snapshot_date(conn: sqlite3.Connection) -> Optional[str]:
    row = conn.execute(
        "SELECT snapshot_date FROM portfolio_snapshots ORDER BY snapshot_date DESC LIMIT 1"
    ).fetchone()
    return str(row["snapshot_date"]) if row and row["snapshot_date"] else None


def get_latest_briefing(conn: sqlite3.Connection, cadence: str) -> Optional[Dict[str, Any]]:
    row = conn.execute(
        """
        SELECT *
        FROM advisor_briefings
        WHERE cadence = ?
        ORDER BY id DESC
        LIMIT 1
        """,
        (str(cadence),),
    ).fetchone()
    return _briefing_row_to_dict(row) if row else None


def get_briefing_for_as_of(conn: sqlite3.Connection, cadence: str, as_of: str) -> Optional[Dict[str, Any]]:
    row = conn.execute(
        """
        SELECT *
        FROM advisor_briefings
        WHERE cadence = ? AND as_of = ?
        ORDER BY id DESC
        LIMIT 1
        """,
        (str(cadence), str(as_of)),
    ).fetchone()
    return _briefing_row_to_dict(row) if row else None


def list_briefings(conn: sqlite3.Connection, cadence: Optional[str] = None, limit: int = 20) -> List[Dict[str, Any]]:
    if cadence:
        rows = conn.execute(
            """
            SELECT *
            FROM advisor_briefings
            WHERE cadence = ?
            ORDER BY id DESC
            LIMIT ?
            """,
            (str(cadence), int(limit)),
        ).fetchall()
    else:
        rows = conn.execute(
            """
            SELECT *
            FROM advisor_briefings
            ORDER BY id DESC
            LIMIT ?
            """,
            (int(limit),),
        ).fetchall()
    return [_briefing_row_to_dict(r) for r in rows]


def _briefing_row_to_dict(row: sqlite3.Row) -> Dict[str, Any]:
    if row is None:
        return {}
    out = dict(row)
    for k in ("recommendations_json", "watchlist_json", "quality_json", "market_notes_json", "links_json"):
        default = [] if k in ("recommendations_json", "watchlist_json") else {}
        out[k.replace("_json", "")] = _loads_json(out.get(k), default)
    out.pop("recommendations_json", None)
    out.pop("watchlist_json", None)
    out.pop("quality_json", None)
    out.pop("market_notes_json", None)
    out.pop("links_json", None)
    return out


def _load_candidates_for_run(conn: sqlite3.Connection, run_id: int) -> List[Dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT *
        FROM advisor_opportunity_candidates
        WHERE run_id = ?
        ORDER BY score_total DESC, symbol ASC
        """,
        (int(run_id),),
    ).fetchall()
    return [dict(r) for r in rows]


def get_latest_opportunity_run(conn: sqlite3.Connection, *, ok_only: bool = True) -> Optional[Dict[str, Any]]:
    sql = "SELECT * FROM advisor_opportunity_runs"
    if ok_only:
        sql += " WHERE status = 'ok'"
    sql += " ORDER BY id DESC LIMIT 1"
    row = conn.execute(sql).fetchone()
    if not row:
        return None
    return _opportunity_run_row_to_dict(row, conn)


def find_reusable_opportunity_run(
    conn: sqlite3.Connection,
    *,
    as_of: str,
    mode: str,
    universe: str,
    budget_ars: float,
    top_n: int,
    variant_id: Optional[int] = None,
) -> Optional[Dict[str, Any]]:
    sql = """
        SELECT *
        FROM advisor_opportunity_runs
        WHERE status = 'ok'
          AND as_of = ?
          AND mode = ?
          AND universe = ?
          AND budget_ars = ?
          AND top_n = ?
    """
    params: List[Any] = [str(as_of), str(mode), str(universe), float(budget_ars), int(top_n)]
    if variant_id is not None:
        sql += " AND COALESCE(variant_id, 0) = ?"
        params.append(int(variant_id))
    sql += " ORDER BY id DESC LIMIT 1"
    row = conn.execute(sql, tuple(params)).fetchone()
    if not row:
        return None
    return _opportunity_run_row_to_dict(row, conn)


def _opportunity_run_row_to_dict(row: sqlite3.Row, conn: sqlite3.Connection) -> Dict[str, Any]:
    out = dict(row)
    out["pipeline_warnings"] = _loads_json(out.pop("pipeline_warnings_json", None), [])
    out["run_metrics"] = _loads_json(out.pop("run_metrics_json", None), {})
    out["candidates"] = _load_candidates_for_run(conn, int(out["id"]))
    out["top_operable"] = [
        c for c in out["candidates"] if str(c.get("candidate_status") or "") == "operable"
    ][: int(out.get("top_n") or 10)]
    out["watchlist"] = [
        c for c in out["candidates"] if str(c.get("candidate_status") or "") in ("watchlist", "manual_review")
    ][: int(out.get("top_n") or 10)]
    return out


def build_unified_context(
    db_path: str,
    *,
    as_of: Optional[str] = None,
    limit: int = 10,
    history_days: int = 365,
    include_cash: bool = True,
    include_orders: bool = False,
    orders_limit: int = 20,
) -> Dict[str, Any]:
    from iol_web import routes_api

    base = build_advisor_context_from_db_path(
        db_path=db_path,
        as_of=as_of,
        limit=limit,
        history_days=history_days,
        include_cash=include_cash,
        include_orders=include_orders,
        orders_limit=orders_limit,
    )
    if not os.path.exists(db_path):
        base["quality"] = {"rows": []}
        base["monthly_kpi"] = {"status": "insufficient_snapshots"}
        return base
    try:
        conn = connect(db_path)
        _ensure_advisor_schema(conn)
        latest_snapshot = _latest_snapshot_date(conn)
    finally:
        conn.close()

    if not latest_snapshot:
        base["quality"] = {"rows": []}
        base["monthly_kpi"] = {"status": "insufficient_snapshots"}
        return base

    as_of_v = str(base.get("as_of") or latest_snapshot)
    if as_of_v != latest_snapshot:
        base.setdefault("notes", {})
        base["notes"]["returns_definition"] = "flow-aware only on latest snapshot; historical as_of keeps snapshot deltas"
        return base

    with _override_db_env(db_path):
        latest_payload = routes_api.latest()
        returns_payload = routes_api.returns()
        quality_payload = routes_api.quality()
        monthly_kpi = routes_api.kpi_monthly_vs_inflation()

    warnings = [str(w) for w in (base.get("warnings") or []) if str(w) != "RETURNS_IGNORE_CASHFLOWS"]
    inference = _find_quality_row(quality_payload.get("rows") or [], "quality_inference")
    if inference and str(inference.get("kind") or "") in ("warn", "error"):
        warnings.append("RETURNS_FLOW_WARNING")
    base["snapshot"] = dict(base.get("snapshot") or {})
    latest_snapshot_payload = (latest_payload or {}).get("snapshot") or {}
    if latest_snapshot_payload:
        base["snapshot"]["cash_total_ars"] = latest_snapshot_payload.get("cash_total_ars")
        base["snapshot"]["close_time"] = latest_snapshot_payload.get("close_time")
    base["returns"] = returns_payload
    base["quality"] = quality_payload
    base["monthly_kpi"] = monthly_kpi
    base["warnings"] = list(dict.fromkeys(warnings))
    return base


def _candidate_evidence_count(row: Dict[str, Any]) -> int:
    summary = _loads_json(row.get("evidence_summary_json"), {})
    try:
        return int(summary.get("fresh_trusted_refs_count") or row.get("trusted_refs_count") or 0)
    except Exception:
        return 0


def _candidate_quality_flags(row: Dict[str, Any], quality_rows: List[Dict[str, Any]]) -> List[str]:
    flags = _loads_json(row.get("risk_flags_json"), [])
    out = [str(f) for f in flags if str(f).strip()]
    inference = _find_quality_row(quality_rows, "quality_inference")
    cashflows = _find_quality_row(quality_rows, "cashflow_imports")
    if str(inference.get("kind") or "") == "warn":
        out.append("QUALITY_INFERENCE_WARN")
    if str(cashflows.get("kind") or "") == "warn":
        out.append("CASHFLOW_IMPORT_MISSING")
    return list(dict.fromkeys(out))


def _candidate_status_for_briefing(row: Dict[str, Any], quality_rows: List[Dict[str, Any]]) -> str:
    candidate_status = str(row.get("candidate_status") or "")
    evidence_count = _candidate_evidence_count(row)
    flags = _candidate_quality_flags(row, quality_rows)
    if candidate_status in ("watchlist", "manual_review"):
        return "watchlist"
    if candidate_status != "operable":
        return "blocked"
    if evidence_count >= 2 and not any(f.startswith("EVIDENCE_") or f.startswith("QUALITY_") for f in flags):
        return "actionable"
    return "conditional"


def _recommendation_action_bucket(row: Dict[str, Any], status: str) -> str:
    symbol = str(row.get("symbol") or "").strip()
    signal_side = str(row.get("signal_side") or "").strip().lower()
    if not symbol:
        return "review"
    if status == "watchlist":
        return "wait"
    if signal_side == "sell":
        return "sell"
    if signal_side == "buy":
        return "buy"
    return "review"


def _recommendation_short_reason(row: Dict[str, Any], status: str, flags: List[str]) -> str:
    symbol = str(row.get("symbol") or "").strip()
    signal_side = str(row.get("signal_side") or "").strip().lower()
    signal_family = str(row.get("signal_family") or row.get("candidate_type") or "").strip().lower()
    evidence_count = _candidate_evidence_count(row)
    raw_reason = str(row.get("reason_summary") or row.get("reason") or row.get("detail") or "").strip()

    if not symbol:
        return raw_reason or "Hay una validacion pendiente antes de operar."
    if status == "watchlist":
        if evidence_count <= 0:
            return "Seguir en observacion hasta tener mas evidencia."
        return "Idea en seguimiento; todavia no esta lista para operar."
    if signal_side == "sell":
        if signal_family == "exit":
            return "Salida sugerida por deterioro de la tesis o del riesgo."
        return "Reducir posicion para bajar riesgo o concentracion."
    if signal_family == "rebuy":
        return "Reforzar posicion en un retroceso controlado."
    if any(str(flag).startswith("EVIDENCE_") for flag in flags):
        return "Compra posible, pero hay evidencia que todavia debe validarse."
    if status == "actionable":
        return "Compra lista para validar con tu plan."
    return "Compra posible, pero requiere validacion antes de operar."


def _recommendation_is_blocking(row: Dict[str, Any], status: str) -> bool:
    return status == "blocked" or not str(row.get("symbol") or "").strip()


def _quality_summary(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    worst_kind = "ok"
    labels: List[str] = []
    for row in rows or []:
        kind = str((row or {}).get("kind") or "info")
        if _quality_kind_rank(kind) < _quality_kind_rank(worst_kind):
            worst_kind = kind
        if kind in ("warn", "error"):
            labels.append(str((row or {}).get("label") or "warning"))
    if not rows:
        worst_kind = "warn"
        labels.append("Sin calidad disponible")
    return {"kind": worst_kind if worst_kind in ("ok", "warn", "error") else "warn", "labels": labels[:5]}


def _build_market_notes(
    context: Dict[str, Any],
    latest_run: Optional[Dict[str, Any]],
    regression: Optional[Dict[str, Any]] = None,
    active_variant: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    returns_block = (context or {}).get("returns") or {}
    snapshot = (context or {}).get("snapshot") or {}
    quality_rows = ((context or {}).get("quality") or {}).get("rows") or []
    quality_summary = _quality_summary(quality_rows)
    monthly_kpi = (context or {}).get("monthly_kpi") or {}
    return {
        "snapshot_date": snapshot.get("snapshot_date"),
        "total_value_ars": snapshot.get("total_value_ars"),
        "cash_ars": snapshot.get("cash_disponible_ars"),
        "cash_total_ars": snapshot.get("cash_total_ars"),
        "daily_real_pct": (returns_block.get("daily") or {}).get("real_pct"),
        "weekly_real_pct": (returns_block.get("weekly") or {}).get("real_pct"),
        "monthly_real_pct": (returns_block.get("monthly") or {}).get("real_pct"),
        "monthly_vs_inflation_pct": monthly_kpi.get("real_vs_inflation_pct"),
        "quality_summary": quality_summary,
        "latest_weekly_run_as_of": (latest_run or {}).get("as_of"),
        "latest_weekly_run_operable": len((latest_run or {}).get("top_operable") or []),
        "active_variant_name": (active_variant or {}).get("name"),
        "active_variant_status": (active_variant or {}).get("status"),
        "benchmark_gate_status": (regression or {}).get("gate_status"),
        "benchmark_flags": list((regression or {}).get("regression_flags") or []),
        "benchmark_scorecard": dict((regression or {}).get("scorecard") or {}),
    }


def _build_summary_md(
    cadence: str,
    context: Dict[str, Any],
    latest_run: Optional[Dict[str, Any]],
    status: str,
    regression: Optional[Dict[str, Any]] = None,
    active_variant: Optional[Dict[str, Any]] = None,
) -> str:
    snapshot = (context or {}).get("snapshot") or {}
    returns_block = (context or {}).get("returns") or {}
    monthly_kpi = (context or {}).get("monthly_kpi") or {}
    quality_rows = ((context or {}).get("quality") or {}).get("rows") or []
    quality_summary = _quality_summary(quality_rows)
    lines = [
        f"# Briefing {cadence}",
        "",
        f"- `as_of`: {snapshot.get('snapshot_date') or '-'}",
        f"- `status`: {status}",
        f"- `total_value_ars`: {snapshot.get('total_value_ars') or '-'}",
        f"- `daily_real_pct`: {(returns_block.get('daily') or {}).get('real_pct')}",
        f"- `weekly_real_pct`: {(returns_block.get('weekly') or {}).get('real_pct')}",
        f"- `monthly_real_pct`: {(returns_block.get('monthly') or {}).get('real_pct')}",
        f"- `real_vs_inflation_pct`: {monthly_kpi.get('real_vs_inflation_pct')}",
        f"- `quality`: {quality_summary.get('kind')}",
    ]
    if latest_run:
        lines.append(f"- `latest_weekly_run_as_of`: {latest_run.get('as_of')}")
        lines.append(f"- `latest_weekly_operable`: {len(latest_run.get('top_operable') or [])}")
        if latest_run.get("variant_name"):
            lines.append(f"- `variant`: {latest_run.get('variant_name')}")
        if latest_run.get("score_version"):
            lines.append(f"- `score_version`: {latest_run.get('score_version')}")
    if active_variant:
        lines.append(f"- `active_variant_status`: {active_variant.get('status')}")
    if regression:
        scorecard = dict(regression.get("scorecard") or {})
        lines.append(f"- `benchmark_gate_status`: {regression.get('gate_status')}")
        lines.append(f"- `benchmark_composite_score`: {scorecard.get('composite_score')}")
        flags = list(regression.get("regression_flags") or [])
        if flags:
            lines.append(f"- `benchmark_flags`: {','.join(str(x) for x in flags)}")
    if quality_summary.get("labels"):
        lines.append("")
        lines.append("## Warnings")
        for label in quality_summary["labels"]:
            lines.append(f"- {label}")
    return "\n".join(lines) + "\n"


def _build_recommendations(
    cadence: str,
    context: Dict[str, Any],
    latest_run: Optional[Dict[str, Any]],
) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]], str]:
    quality_rows = ((context or {}).get("quality") or {}).get("rows") or []
    recommendations: List[Dict[str, Any]] = []
    watchlist: List[Dict[str, Any]] = []
    worst_status = "ok"

    def _merge_status(cur: str, new: str) -> str:
        order = {"error": 0, "blocked": 1, "warn": 2, "ok": 3}
        normalized = {
            "actionable": "ok",
            "conditional": "warn",
            "watchlist": "warn",
            "blocked": "blocked",
            "error": "error",
            "ok": "ok",
            "warn": "warn",
        }
        cur_v = normalized.get(str(cur or "").strip().lower(), "warn")
        new_v = normalized.get(str(new or "").strip().lower(), "warn")
        return new_v if order[new_v] < order[cur_v] else cur_v

    if latest_run:
        for row in (latest_run.get("top_operable") or []):
            status = _candidate_status_for_briefing(row, quality_rows)
            quality_flags = _candidate_quality_flags(row, quality_rows)
            action_bucket = _recommendation_action_bucket(row, status)
            item = {
                "status": status,
                "symbol": row.get("symbol"),
                "title": str(row.get("symbol") or row.get("title") or "").strip() or f"{row.get('symbol')} ({row.get('candidate_type')})",
                "reason": row.get("reason_summary"),
                "short_reason": _recommendation_short_reason(row, status, quality_flags),
                "action_bucket": action_bucket,
                "is_blocking": _recommendation_is_blocking(row, status),
                "evidence_count": _candidate_evidence_count(row),
                "quality_flags": quality_flags,
                "next_step": "Validar ventana de entrada y simular antes de operar.",
                "entry_low": row.get("entry_low"),
                "entry_high": row.get("entry_high"),
                "suggested_amount_ars": row.get("suggested_amount_ars"),
                "score_total": row.get("score_total"),
                "candidate_type": row.get("candidate_type"),
                "signal_side": row.get("signal_side"),
                "signal_family": row.get("signal_family"),
                "score_version": row.get("score_version"),
            }
            if str(row.get("signal_side") or "buy").strip().lower() == "sell":
                item["next_step"] = "Validar salida parcial/total y simular venta antes de operar."
            if status == "watchlist":
                watchlist.append(item)
            else:
                recommendations.append(item)
            worst_status = _merge_status(worst_status, status)

        for row in (latest_run.get("watchlist") or []):
            quality_flags = _candidate_quality_flags(row, quality_rows)
            status = "watchlist"
            watchlist.append(
                {
                    "status": status,
                    "symbol": row.get("symbol"),
                    "title": str(row.get("symbol") or row.get("title") or "").strip() or f"{row.get('symbol')} ({row.get('candidate_type')})",
                    "reason": row.get("reason_summary"),
                    "short_reason": _recommendation_short_reason(row, status, quality_flags),
                    "action_bucket": _recommendation_action_bucket(row, status),
                    "is_blocking": _recommendation_is_blocking(row, status),
                    "evidence_count": _candidate_evidence_count(row),
                    "quality_flags": quality_flags,
                    "next_step": "Esperar más evidencia fresca u hoja de ruta semanal.",
                    "score_total": row.get("score_total"),
                    "candidate_type": row.get("candidate_type"),
                    "signal_side": row.get("signal_side"),
                    "signal_family": row.get("signal_family"),
                    "score_version": row.get("score_version"),
                }
            )
    else:
        recommendations.append(
            {
                "status": "blocked",
                "symbol": None,
                "title": "Weekly deep no disponible",
                "reason": "No hay una corrida semanal reciente para transformar el ranking en recomendaciones operativas.",
                "short_reason": "No hay corrida semanal reciente para sugerir acciones operativas.",
                "action_bucket": "review",
                "is_blocking": True,
                "evidence_count": 0,
                "quality_flags": ["NO_WEEKLY_RUN"],
                "next_step": "Ejecutar `iol advisor autopilot run --cadence weekly`.",
            }
        )
        worst_status = _merge_status(worst_status, "blocked")

    if cadence == "daily":
        quality_row = _find_quality_row(quality_rows, "quality_inference")
        cashflows_row = _find_quality_row(quality_rows, "cashflow_imports")
        if str(quality_row.get("kind") or "") == "warn":
            recommendations.insert(
                0,
                {
                    "status": "conditional",
                    "symbol": None,
                    "title": "Revisar calidad de inferencia",
                    "reason": quality_row.get("detail") or "La calidad de inferencia necesita revisión manual.",
                    "short_reason": "Los datos todavia no permiten confiar plenamente en la recomendacion.",
                    "action_bucket": "review",
                    "is_blocking": True,
                    "evidence_count": 0,
                    "quality_flags": [str(c) for c in (quality_row.get("codes") or [])],
                    "next_step": "Corregir cashflows manuales o completar cobertura antes de usar el retorno real como señal fuerte.",
                },
            )
            worst_status = _merge_status(worst_status, "warn")
        if str(cashflows_row.get("kind") or "") == "warn":
            watchlist.insert(
                0,
                {
                    "status": "watchlist",
                    "symbol": None,
                    "title": "Cobertura de cashflows incompleta",
                    "reason": cashflows_row.get("detail") or "No hay movimientos importados recientes.",
                    "short_reason": "Faltan movimientos para medir bien el rendimiento real.",
                    "action_bucket": "review",
                    "is_blocking": True,
                    "evidence_count": 0,
                    "quality_flags": [str(c) for c in (cashflows_row.get("codes") or [])],
                    "next_step": "Importar movimientos o cargar ajustes manuales antes del próximo cierre.",
                },
            )

    if not recommendations and watchlist:
        worst_status = "warn"
    elif not recommendations and not watchlist:
        worst_status = "blocked"

    normalized_status = worst_status
    if normalized_status not in BRIEFING_STATUSES:
        normalized_status = "warn"
    for idx, item in enumerate(recommendations, start=1):
        item["priority_rank"] = idx
    for idx, item in enumerate(watchlist, start=1):
        item["priority_rank"] = idx
    return recommendations, watchlist, normalized_status


def insert_advisor_briefing(
    conn: sqlite3.Connection,
    briefing: Dict[str, Any],
) -> int:
    cur = conn.execute(
        """
        INSERT INTO advisor_briefings(
            created_at_utc, as_of, cadence, status, source_policy, title,
            summary_md, recommendations_json, watchlist_json, quality_json,
            market_notes_json, links_json, opportunity_run_id, advisor_log_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            str(briefing.get("created_at_utc")),
            str(briefing.get("as_of")),
            str(briefing.get("cadence")),
            str(briefing.get("status")),
            str(briefing.get("source_policy") or DEFAULT_SOURCE_POLICY),
            briefing.get("title"),
            str(briefing.get("summary_md") or ""),
            json.dumps(briefing.get("recommendations") or [], ensure_ascii=True, sort_keys=True),
            json.dumps(briefing.get("watchlist") or [], ensure_ascii=True, sort_keys=True),
            json.dumps(briefing.get("quality") or {}, ensure_ascii=True, sort_keys=True),
            json.dumps(briefing.get("market_notes") or {}, ensure_ascii=True, sort_keys=True),
            json.dumps(briefing.get("links") or {}, ensure_ascii=True, sort_keys=True),
            briefing.get("opportunity_run_id"),
            briefing.get("advisor_log_id"),
        ),
    )
    return int(cur.lastrowid)


def log_briefing(
    conn: sqlite3.Connection,
    *,
    cadence: str,
    as_of: str,
    status: str,
    summary_md: str,
    env: str,
    base_url: str,
) -> int:
    cur = conn.execute(
        """
        INSERT INTO advisor_logs(created_at, snapshot_date, prompt, response, env, base_url)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            _utc_now_iso(),
            str(as_of),
            f"autopilot {cadence}",
            summary_md,
            env,
            base_url,
        ),
    )
    return int(cur.lastrowid)


def build_briefing_payload(
    *,
    cadence: str,
    context: Dict[str, Any],
    latest_run: Optional[Dict[str, Any]],
    regression: Optional[Dict[str, Any]] = None,
    active_variant: Optional[Dict[str, Any]] = None,
    source_policy: str = DEFAULT_SOURCE_POLICY,
) -> Dict[str, Any]:
    cadence_v = str(cadence or "").strip().lower()
    if cadence_v not in BRIEFING_CADENCES:
        raise ValueError("cadence must be daily|weekly")
    recommendations, watchlist, status = _build_recommendations(cadence_v, context, latest_run)
    quality_rows = ((context or {}).get("quality") or {}).get("rows") or []
    quality_summary = _quality_summary(quality_rows)
    if quality_summary.get("kind") == "warn" and status == "ok":
        status = "warn"
    if regression:
        gate_status = str(regression.get("gate_status") or "").strip().lower()
        if gate_status == "warn" and status == "ok":
            status = "warn"
        if gate_status == "blocked":
            status = "blocked"
    summary_md = _build_summary_md(cadence_v, context, latest_run, status, regression, active_variant)
    snapshot = (context or {}).get("snapshot") or {}
    return {
        "created_at_utc": _utc_now_iso(),
        "as_of": str(snapshot.get("snapshot_date") or date.today().isoformat()),
        "cadence": cadence_v,
        "status": status,
        "source_policy": source_policy,
        "title": f"Asesor {cadence_v}",
        "summary_md": summary_md,
        "recommendations": recommendations,
        "watchlist": watchlist,
        "quality": {"rows": quality_rows, "summary": quality_summary},
        "market_notes": _build_market_notes(context, latest_run, regression, active_variant),
        "links": {
            "latest_reports": {
                "analysis": "reports/latest/AnalisisPortafolio.md",
                "macro": "reports/latest/Macro.md",
                "opportunities": "reports/latest/Oportunidades.md",
                "followup": "reports/latest/Seguimiento.md",
            },
            "opportunity_run_id": (latest_run or {}).get("id"),
            "regression_gate_status": (regression or {}).get("gate_status"),
        },
        "opportunity_run_id": (latest_run or {}).get("id"),
        "advisor_log_id": None,
    }


def persist_briefing_bundle(
    *,
    db_path: str,
    cadence: str,
    env: str,
    base_url: str,
    context: Dict[str, Any],
    latest_run: Optional[Dict[str, Any]],
    regression: Optional[Dict[str, Any]] = None,
    active_variant: Optional[Dict[str, Any]] = None,
    source_policy: str = DEFAULT_SOURCE_POLICY,
    force: bool = False,
) -> BriefingBundle:
    conn = connect(db_path)
    _ensure_advisor_schema(conn)
    try:
        as_of = str(((context or {}).get("snapshot") or {}).get("snapshot_date") or date.today().isoformat())
        if not force:
            existing = get_briefing_for_as_of(conn, cadence, as_of)
            if existing:
                return BriefingBundle(existing, reused=True)
        briefing = build_briefing_payload(
            cadence=cadence,
            context=context,
            latest_run=latest_run,
            regression=regression,
            active_variant=active_variant,
            source_policy=source_policy,
        )
        log_id = log_briefing(
            conn,
            cadence=cadence,
            as_of=as_of,
            status=str(briefing.get("status") or "warn"),
            summary_md=str(briefing.get("summary_md") or ""),
            env=env,
            base_url=base_url,
        )
        briefing["advisor_log_id"] = log_id
        briefing_id = insert_advisor_briefing(conn, briefing)
        conn.commit()
        created = get_briefing_for_as_of(conn, cadence, as_of) or {}
        if not created:
            created = dict(briefing)
            created["id"] = briefing_id
        return BriefingBundle(created, reused=False)
    finally:
        conn.close()


def load_latest_briefing_payload(db_path: str, cadence: str) -> Optional[Dict[str, Any]]:
    if not os.path.exists(db_path):
        return None
    conn = connect(db_path)
    _ensure_advisor_schema(conn)
    try:
        return get_latest_briefing(conn, cadence)
    finally:
        conn.close()


def load_briefing_history_payload(db_path: str, cadence: Optional[str], limit: int) -> List[Dict[str, Any]]:
    if not os.path.exists(db_path):
        return []
    conn = connect(db_path)
    _ensure_advisor_schema(conn)
    try:
        return list_briefings(conn, cadence=cadence, limit=limit)
    finally:
        conn.close()


def load_latest_opportunity_payload(db_path: str) -> Optional[Dict[str, Any]]:
    if not os.path.exists(db_path):
        return None
    conn = connect(db_path)
    _ensure_advisor_schema(conn)
    try:
        return get_latest_opportunity_run(conn, ok_only=True)
    finally:
        conn.close()
