from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple


HORIZONS = (1, 5, 20)
DEFAULT_WINDOW_DAYS = 90
DEFAULT_OBJECTIVE = "risk_adjusted_excess_return"


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _loads_json(value: Any, default: Any) -> Any:
    if value is None:
        return default
    if isinstance(value, (dict, list)):
        return value
    try:
        parsed = json.loads(str(value))
    except Exception:
        return default
    return parsed if isinstance(parsed, type(default)) else default


def _safe_float(value: Any, default: Optional[float] = None) -> Optional[float]:
    if value is None:
        return default
    try:
        return float(value)
    except Exception:
        return default


def _safe_int(value: Any, default: Optional[int] = None) -> Optional[int]:
    if value is None:
        return default
    try:
        return int(value)
    except Exception:
        return default


def _pct(value: float, base: float) -> Optional[float]:
    if base <= 0:
        return None
    return (value / base - 1.0) * 100.0


def _candidate_score(row: Dict[str, Any]) -> float:
    excess = _safe_float(row.get("excess_return_pct"), 0.0) or 0.0
    mae = _safe_float(row.get("max_adverse_excursion_pct"), 0.0) or 0.0
    penalty = _safe_float(row.get("liquidity_penalty"), 0.0) or 0.0
    return float(excess) - max(0.0, -float(mae)) * 0.5 - float(penalty)


def _signal_label(signal_side: str, signal_family: str) -> str:
    return f"{str(signal_side or '').strip().lower()}:{str(signal_family or '').strip().lower()}"


@dataclass
class ModelVariant:
    id: int
    name: str
    status: str
    created_at_utc: str
    config: Dict[str, Any]
    objective: str
    promoted_from_variant_id: Optional[int]
    promoted_at_utc: Optional[str]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "status": self.status,
            "created_at_utc": self.created_at_utc,
            "config": dict(self.config),
            "objective": self.objective,
            "promoted_from_variant_id": self.promoted_from_variant_id,
            "promoted_at_utc": self.promoted_at_utc,
        }


def default_variant_specs() -> List[Dict[str, Any]]:
    return [
        {
            "name": "baseline_v1",
            "status": "active",
            "objective": DEFAULT_OBJECTIVE,
            "config": {
                "score_version": "baseline_v1",
                "weights": {"risk": 0.35, "value": 0.20, "momentum": 0.35, "catalyst": 0.10},
                "thresholds": {
                    "spread_pct_max": 2.5,
                    "concentration_pct_max": 15.0,
                    "new_asset_initial_cap_pct": 8.0,
                    "drawdown_exclusion_pct": -25.0,
                    "rebuy_dip_threshold_pct": -8.0,
                    "trim_weight_pct": 12.0,
                    "exit_weight_pct": 15.0,
                    "sell_momentum_max": 35.0,
                    "exit_momentum_max": 20.0,
                    "sell_conflict_exit": True,
                    "liquidity_floor": 40.0,
                },
            },
        },
        {
            "name": "challenger_v1",
            "status": "challenger",
            "objective": DEFAULT_OBJECTIVE,
            "config": {
                "score_version": "challenger_v1",
                "weights": {"risk": 0.30, "value": 0.15, "momentum": 0.30, "catalyst": 0.25},
                "thresholds": {
                    "spread_pct_max": 2.2,
                    "concentration_pct_max": 14.0,
                    "new_asset_initial_cap_pct": 7.0,
                    "drawdown_exclusion_pct": -22.0,
                    "rebuy_dip_threshold_pct": -6.0,
                    "trim_weight_pct": 11.0,
                    "exit_weight_pct": 14.0,
                    "sell_momentum_max": 40.0,
                    "exit_momentum_max": 18.0,
                    "sell_conflict_exit": True,
                    "liquidity_floor": 45.0,
                },
            },
        },
    ]


def ensure_default_model_variants(conn: sqlite3.Connection) -> None:
    rows = conn.execute(
        """
        SELECT id, name, status, created_at_utc, config_json, objective, promoted_from_variant_id, promoted_at_utc
        FROM advisor_model_variants
        ORDER BY id ASC
        """
    ).fetchall()
    existing = {str(r["name"]): dict(r) for r in rows}
    now = _utc_now_iso()
    for spec in default_variant_specs():
        if spec["name"] in existing:
            continue
        conn.execute(
            """
            INSERT INTO advisor_model_variants(
                name, status, created_at_utc, config_json, objective, promoted_from_variant_id, promoted_at_utc
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                spec["name"],
                spec["status"],
                now,
                json.dumps(spec["config"], ensure_ascii=True, sort_keys=True),
                spec["objective"],
                None,
                None,
            ),
        )
    conn.commit()


def list_model_variants(conn: sqlite3.Connection) -> List[ModelVariant]:
    ensure_default_model_variants(conn)
    rows = conn.execute(
        """
        SELECT id, name, status, created_at_utc, config_json, objective, promoted_from_variant_id, promoted_at_utc
        FROM advisor_model_variants
        ORDER BY CASE status WHEN 'active' THEN 0 WHEN 'challenger' THEN 1 ELSE 2 END, id ASC
        """
    ).fetchall()
    out: List[ModelVariant] = []
    for row in rows:
        out.append(
            ModelVariant(
                id=int(row["id"]),
                name=str(row["name"]),
                status=str(row["status"]),
                created_at_utc=str(row["created_at_utc"]),
                config=_loads_json(row["config_json"], {}),
                objective=str(row["objective"] or DEFAULT_OBJECTIVE),
                promoted_from_variant_id=_safe_int(row["promoted_from_variant_id"]),
                promoted_at_utc=str(row["promoted_at_utc"]) if row["promoted_at_utc"] else None,
            )
        )
    return out


def resolve_variant_selection(conn: sqlite3.Connection, selector: str) -> List[ModelVariant]:
    selector_v = str(selector or "active").strip().lower()
    variants = list_model_variants(conn)
    if selector_v == "both":
        return [v for v in variants if v.status in ("active", "challenger")]
    if selector_v in ("active", "challenger"):
        return [v for v in variants if v.status == selector_v]
    try:
        variant_id = int(selector_v)
    except Exception:
        return []
    return [v for v in variants if v.id == variant_id]


def active_variant(conn: sqlite3.Connection) -> Optional[ModelVariant]:
    rows = resolve_variant_selection(conn, "active")
    return rows[0] if rows else None


def challenger_variant(conn: sqlite3.Connection) -> Optional[ModelVariant]:
    rows = resolve_variant_selection(conn, "challenger")
    return rows[0] if rows else None


def _latest_snapshot_date(conn: sqlite3.Connection) -> Optional[str]:
    latest: Optional[str] = None
    for table in ("market_symbol_snapshots", "portfolio_snapshots"):
        row = conn.execute(
            f"SELECT snapshot_date FROM {table} ORDER BY snapshot_date DESC LIMIT 1"
        ).fetchone()
        snap = str(row["snapshot_date"]) if row and row["snapshot_date"] else None
        if snap and (latest is None or snap > latest):
            latest = snap
    return latest


def _load_price_series(conn: sqlite3.Connection, as_of: str) -> Dict[str, List[Tuple[str, float]]]:
    rows = conn.execute(
        """
        SELECT snapshot_date, symbol, last_price
        FROM market_symbol_snapshots
        WHERE snapshot_date <= ?
        ORDER BY symbol ASC, snapshot_date ASC
        """,
        (str(as_of),),
    ).fetchall()
    out: Dict[str, List[Tuple[str, float]]] = {}
    for row in rows:
        price = _safe_float(row["last_price"])
        symbol = str(row["symbol"] or "").strip().upper()
        snap = str(row["snapshot_date"] or "")
        if not symbol or not snap or price is None or price <= 0:
            continue
        out.setdefault(symbol, []).append((snap, float(price)))
    return out


def _load_benchmark_series(conn: sqlite3.Connection, as_of: str) -> Dict[str, Dict[str, float]]:
    rows = conn.execute(
        """
        SELECT snapshot_date, symbol, last_price
        FROM market_symbol_snapshots
        WHERE snapshot_date <= ?
        ORDER BY snapshot_date ASC, symbol ASC
        """,
        (str(as_of),),
    ).fetchall()
    by_date: Dict[str, List[float]] = {}
    for row in rows:
        snap = str(row["snapshot_date"] or "")
        price = _safe_float(row["last_price"])
        if not snap or price is None or price <= 0:
            continue
        by_date.setdefault(snap, []).append(float(price))
    out: Dict[str, Dict[str, float]] = {}
    for snap, prices in by_date.items():
        if not prices:
            continue
        out[snap] = {"mean_price": sum(prices) / float(len(prices)), "count": float(len(prices))}
    return out


def _price_on_or_before(series: Sequence[Tuple[str, float]], target: str) -> Optional[float]:
    latest: Optional[float] = None
    for snap, price in series:
        if snap <= target:
            latest = float(price)
        else:
            break
    return latest


def _series_window(series: Sequence[Tuple[str, float]], start: str, end: str) -> List[Tuple[str, float]]:
    out: List[Tuple[str, float]] = []
    for snap, price in series:
        if snap < start:
            continue
        if snap > end:
            break
        out.append((snap, float(price)))
    return out


def _signed_path_returns(
    path: Sequence[Tuple[str, float]],
    entry_price: float,
    signal_side: str,
) -> List[float]:
    side = str(signal_side or "buy").strip().lower()
    out: List[float] = []
    if entry_price <= 0:
        return out
    for _, price in path:
        raw_ret = _pct(float(price), float(entry_price))
        if raw_ret is None:
            continue
        signed = raw_ret if side == "buy" else -raw_ret
        out.append(float(signed))
    return out


def _manual_feedback_for_symbol(conn: sqlite3.Connection, symbol: str, as_of: str) -> Dict[str, Any]:
    rows = conn.execute(
        """
        SELECT event_type, title, description, snapshot_date
        FROM advisor_events
        WHERE symbol = ?
          AND (snapshot_date IS NULL OR snapshot_date >= ?)
        ORDER BY id DESC
        LIMIT 10
        """,
        (str(symbol), str(as_of)),
    ).fetchall()
    labels: List[str] = []
    for row in rows:
        event_type = str(row["event_type"] or "").strip().lower()
        title = str(row["title"] or "").strip().lower()
        if event_type:
            labels.append(event_type)
        if "execut" in title:
            labels.append("executed")
        if "dismiss" in title:
            labels.append("dismissed")
        if "late" in title:
            labels.append("late")
        if "false" in title:
            labels.append("false_positive")
        if "thesis" in title:
            labels.append("thesis_broken")
    return {"manual_event_labels": sorted(set(labels))}


def evaluate_signal_outcomes(
    conn: sqlite3.Connection,
    *,
    as_of: Optional[str] = None,
    horizons: Sequence[int] = HORIZONS,
) -> Dict[str, Any]:
    ensure_default_model_variants(conn)
    as_of_v = str(as_of or _latest_snapshot_date(conn) or date.today().isoformat())
    series_by_symbol = _load_price_series(conn, as_of_v)
    benchmark_by_date = _load_benchmark_series(conn, as_of_v)
    candidates = conn.execute(
        """
        SELECT c.id, c.run_id, c.symbol, c.candidate_status, c.signal_side, c.signal_family, c.liquidity_score,
               c.entry_low, c.entry_high, c.score_version, c.holding_context_json, c.score_features_json,
               r.variant_id, r.as_of
        FROM advisor_opportunity_candidates c
        JOIN advisor_opportunity_runs r ON r.id = c.run_id
        WHERE r.status = 'ok'
          AND r.as_of <= ?
          AND c.candidate_status = 'operable'
        ORDER BY c.id ASC
        """,
        (as_of_v,),
    ).fetchall()
    inserted = 0
    skipped = 0
    for row in candidates:
        symbol = str(row["symbol"] or "").strip().upper()
        signal_side = str(row["signal_side"] or "buy").strip().lower()
        signal_family = str(row["signal_family"] or row["candidate_status"] or "new").strip().lower()
        run_as_of = str(row["as_of"] or "")
        series = series_by_symbol.get(symbol) or []
        if not run_as_of or not series:
            skipped += 1
            continue
        start_price = _price_on_or_before(series, run_as_of)
        if start_price is None or start_price <= 0:
            skipped += 1
            continue
        benchmark_start = (benchmark_by_date.get(run_as_of) or {}).get("mean_price")
        for horizon in horizons:
            existing = conn.execute(
                """
                SELECT id
                FROM advisor_signal_outcomes
                WHERE candidate_id = ? AND horizon = ?
                LIMIT 1
                """,
                (int(row["id"]), int(horizon)),
            ).fetchone()
            if existing:
                continue
            target_date = (date.fromisoformat(run_as_of) + timedelta(days=int(horizon))).isoformat()
            latest_known = _latest_snapshot_date(conn)
            if latest_known is None or latest_known < target_date:
                skipped += 1
                continue
            end_price = _price_on_or_before(series, target_date)
            benchmark_end = (benchmark_by_date.get(target_date) or {}).get("mean_price")
            if end_price is None or benchmark_start is None or benchmark_end is None:
                conn.execute(
                    """
                    INSERT INTO advisor_signal_outcomes(
                        candidate_id, run_id, variant_id, signal_side, signal_family, symbol, as_of, horizon,
                        eval_status, forward_return_pct, excess_return_pct, max_adverse_excursion_pct,
                        max_favorable_excursion_pct, liquidity_penalty, hit, notes_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        int(row["id"]),
                        int(row["run_id"]),
                        _safe_int(row["variant_id"]),
                        signal_side,
                        signal_family,
                        symbol,
                        run_as_of,
                        int(horizon),
                        "missing_prices",
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        json.dumps({"score_version": row["score_version"]}, ensure_ascii=True, sort_keys=True),
                    ),
                )
                inserted += 1
                continue
            raw_forward = _pct(float(end_price), float(start_price))
            bench_forward = _pct(float(benchmark_end), float(benchmark_start))
            if raw_forward is None or bench_forward is None:
                skipped += 1
                continue
            signed_forward = raw_forward if signal_side == "buy" else -raw_forward
            excess = (raw_forward - bench_forward) if signal_side == "buy" else (bench_forward - raw_forward)
            path = _series_window(series, run_as_of, target_date)
            signed_path = _signed_path_returns(path, float(start_price), signal_side)
            mae = min(signed_path) if signed_path else signed_forward
            mfe = max(signed_path) if signed_path else signed_forward
            liq_score = _safe_float(row["liquidity_score"], 50.0) or 50.0
            liquidity_penalty = max(0.0, (60.0 - float(liq_score)) / 60.0 * 2.0)
            hit = 1 if signed_forward > 0.0 and excess > 0.0 else 0
            notes = {
                "benchmark_forward_pct": bench_forward,
                "entry_price": start_price,
                "end_price": end_price,
                "score_version": row["score_version"],
                "holding_context": _loads_json(row["holding_context_json"], {}),
                "score_features": _loads_json(row["score_features_json"], {}),
            }
            notes.update(_manual_feedback_for_symbol(conn, symbol, run_as_of))
            conn.execute(
                """
                INSERT INTO advisor_signal_outcomes(
                    candidate_id, run_id, variant_id, signal_side, signal_family, symbol, as_of, horizon,
                    eval_status, forward_return_pct, excess_return_pct, max_adverse_excursion_pct,
                    max_favorable_excursion_pct, liquidity_penalty, hit, notes_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    int(row["id"]),
                    int(row["run_id"]),
                    _safe_int(row["variant_id"]),
                    signal_side,
                    signal_family,
                    symbol,
                    run_as_of,
                    int(horizon),
                    "ok",
                    float(signed_forward),
                    float(excess),
                    float(mae),
                    float(mfe),
                    float(liquidity_penalty),
                    int(hit),
                    json.dumps(notes, ensure_ascii=True, sort_keys=True),
                ),
            )
            inserted += 1
    conn.commit()
    return {"as_of": as_of_v, "inserted": inserted, "skipped": skipped, "horizons": list(horizons)}


def _scorecard_rows(
    conn: sqlite3.Connection,
    *,
    variant_id: int,
    as_of: str,
    window_days: int,
) -> List[Dict[str, Any]]:
    cutoff = (date.fromisoformat(as_of) - timedelta(days=int(window_days))).isoformat()
    rows = conn.execute(
        """
        SELECT o.*, c.score_total, c.signal_side AS candidate_signal_side, c.signal_family AS candidate_signal_family,
               c.candidate_status, r.created_at_utc
        FROM advisor_signal_outcomes o
        JOIN advisor_opportunity_candidates c ON c.id = o.candidate_id
        JOIN advisor_opportunity_runs r ON r.id = o.run_id
        WHERE o.variant_id = ?
          AND o.eval_status = 'ok'
          AND o.as_of >= ?
          AND o.as_of <= ?
        ORDER BY o.as_of ASC, o.run_id ASC, c.score_total DESC
        """,
        (int(variant_id), cutoff, as_of),
    ).fetchall()
    return [dict(r) for r in rows]


def _precision_by_topn(rows: Sequence[Dict[str, Any]], side: str, top_n: int) -> Optional[float]:
    side_v = str(side).strip().lower()
    grouped: Dict[int, List[Dict[str, Any]]] = {}
    for row in rows:
        if int(row.get("horizon") or 0) != 5:
            continue
        if str(row.get("signal_side") or row.get("candidate_signal_side") or "").strip().lower() != side_v:
            continue
        grouped.setdefault(int(row["run_id"]), []).append(row)
    scored: List[float] = []
    for _, group in grouped.items():
        ranked = sorted(group, key=lambda item: -float(item.get("score_total") or 0.0))[: int(top_n)]
        if not ranked:
            continue
        hits = [int(r.get("hit") or 0) for r in ranked]
        scored.append(sum(hits) / float(len(hits)))
    if not scored:
        return None
    return sum(scored) / float(len(scored))


def _operable_ratio(
    conn: sqlite3.Connection,
    *,
    variant_id: int,
    as_of: str,
    window_days: int,
) -> Optional[float]:
    cutoff = (date.fromisoformat(as_of) - timedelta(days=int(window_days))).isoformat()
    rows = conn.execute(
        """
        SELECT c.candidate_status
        FROM advisor_opportunity_candidates c
        JOIN advisor_opportunity_runs r ON r.id = c.run_id
        WHERE r.variant_id = ?
          AND r.as_of >= ?
          AND r.as_of <= ?
          AND r.status = 'ok'
        """,
        (int(variant_id), cutoff, as_of),
    ).fetchall()
    if not rows:
        return None
    total = len(rows)
    operable = sum(1 for row in rows if str(row["candidate_status"] or "") == "operable")
    return operable / float(total) if total else None


def build_variant_scorecard(
    conn: sqlite3.Connection,
    *,
    variant_id: int,
    as_of: Optional[str] = None,
    window_days: int = DEFAULT_WINDOW_DAYS,
) -> Dict[str, Any]:
    as_of_v = str(as_of or _latest_snapshot_date(conn) or date.today().isoformat())
    rows = _scorecard_rows(conn, variant_id=int(variant_id), as_of=as_of_v, window_days=int(window_days))
    if not rows:
        return {
            "variant_id": int(variant_id),
            "as_of": as_of_v,
            "window_days": int(window_days),
            "sample_count": 0,
            "daily_sample_count": 0,
            "weekly_sample_count": 0,
            "composite_score": None,
            "mean_forward_return_pct": None,
            "mean_excess_return_pct": None,
            "mae_mean_pct": None,
            "mfe_mean_pct": None,
            "operable_ratio": _operable_ratio(conn, variant_id=int(variant_id), as_of=as_of_v, window_days=int(window_days)),
            "top3_precision_buy": None,
            "top5_precision_buy": None,
            "top3_precision_sell": None,
            "top5_precision_sell": None,
            "false_positive_sell_ratio": None,
            "status": "insufficient",
        }
    scores = [_candidate_score(r) for r in rows]
    forward = [float(r.get("forward_return_pct") or 0.0) for r in rows]
    excess = [float(r.get("excess_return_pct") or 0.0) for r in rows]
    maes = [float(r.get("max_adverse_excursion_pct") or 0.0) for r in rows]
    mfes = [float(r.get("max_favorable_excursion_pct") or 0.0) for r in rows]
    daily_rows = [r for r in rows if int(r.get("horizon") or 0) == 1]
    weekly_rows = [r for r in rows if int(r.get("horizon") or 0) == 5]
    sell_rows = [r for r in weekly_rows if str(r.get("signal_side") or "").strip().lower() == "sell"]
    false_positive = sum(1 for r in sell_rows if int(r.get("hit") or 0) == 0)
    status = "ok"
    if len(rows) < 10:
        status = "insufficient"
    elif (sum(scores) / float(len(scores))) < 0.0:
        status = "warn"
    return {
        "variant_id": int(variant_id),
        "as_of": as_of_v,
        "window_days": int(window_days),
        "sample_count": len(rows),
        "daily_sample_count": len(daily_rows),
        "weekly_sample_count": len(weekly_rows),
        "composite_score": sum(scores) / float(len(scores)),
        "mean_forward_return_pct": sum(forward) / float(len(forward)),
        "mean_excess_return_pct": sum(excess) / float(len(excess)),
        "mae_mean_pct": sum(maes) / float(len(maes)),
        "mfe_mean_pct": sum(mfes) / float(len(mfes)),
        "operable_ratio": _operable_ratio(conn, variant_id=int(variant_id), as_of=as_of_v, window_days=int(window_days)),
        "top3_precision_buy": _precision_by_topn(rows, "buy", 3),
        "top5_precision_buy": _precision_by_topn(rows, "buy", 5),
        "top3_precision_sell": _precision_by_topn(rows, "sell", 3),
        "top5_precision_sell": _precision_by_topn(rows, "sell", 5),
        "false_positive_sell_ratio": (false_positive / float(len(sell_rows))) if sell_rows else None,
        "status": status,
    }


def compare_scorecards(
    active_scorecard: Optional[Dict[str, Any]],
    challenger_scorecard: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    active = dict(active_scorecard or {})
    challenger = dict(challenger_scorecard or {})
    flags: List[str] = []
    gate_status = "ok"
    active_score = _safe_float(active.get("composite_score"))
    if active.get("status") == "insufficient":
        flags.append("SAMPLE_INSUFFICIENT")
        gate_status = "warn"
    if active_score is not None and active_score < 0.0:
        flags.append("ACTIVE_COMPOSITE_NEGATIVE")
        gate_status = "warn"
    if active_score is not None and active_score < -1.0:
        flags.append("ACTIVE_COMPOSITE_BLOCKED")
        gate_status = "blocked"
    sell_fp = _safe_float(active.get("false_positive_sell_ratio"))
    if sell_fp is not None and sell_fp > 0.55:
        flags.append("SELL_FALSE_POSITIVE_HIGH")
        gate_status = "warn" if gate_status != "blocked" else gate_status
    top5_buy = _safe_float(active.get("top5_precision_buy"))
    if top5_buy is not None and top5_buy < 0.40:
        flags.append("BUY_PRECISION_LOW")
        gate_status = "warn" if gate_status != "blocked" else gate_status
    promoted = False
    if active and challenger:
        active_comp = _safe_float(active.get("composite_score"))
        challenger_comp = _safe_float(challenger.get("composite_score"))
        if active_comp is not None and challenger_comp is not None:
            improve = challenger_comp - active_comp
            flags.append(f"CHALLENGER_DELTA={improve:.4f}")
    return {
        "gate_status": gate_status,
        "regression_flags": list(dict.fromkeys(flags)),
        "active_scorecard": active,
        "challenger_scorecard": challenger,
        "promoted": promoted,
    }


def _weekly_regression_count(conn: sqlite3.Connection, variant_id: int) -> int:
    row = conn.execute(
        """
        SELECT COUNT(*) AS n
        FROM advisor_run_regressions
        WHERE variant_id = ? AND cadence = 'weekly'
        """,
        (int(variant_id),),
    ).fetchone()
    return int(row["n"] or 0) if row else 0


def maybe_promote_challenger(
    conn: sqlite3.Connection,
    *,
    active_variant_id: int,
    challenger_variant_id: int,
    active_scorecard: Dict[str, Any],
    challenger_scorecard: Dict[str, Any],
) -> Dict[str, Any]:
    flags: List[str] = []
    promoted = False
    reason = "insufficient"
    active_comp = _safe_float(active_scorecard.get("composite_score"))
    challenger_comp = _safe_float(challenger_scorecard.get("composite_score"))
    if active_comp is None or challenger_comp is None:
        flags.append("PROMOTION_NO_SCORE")
        return {"promoted": False, "reason": reason, "flags": flags}
    if int(challenger_scorecard.get("daily_sample_count") or 0) < 30:
        flags.append("PROMOTION_DAILY_SAMPLE_LT_30")
        return {"promoted": False, "reason": reason, "flags": flags}
    if _weekly_regression_count(conn, int(challenger_variant_id)) < 8:
        flags.append("PROMOTION_WEEKLY_SCORECARDS_LT_8")
        return {"promoted": False, "reason": reason, "flags": flags}
    if active_comp == 0:
        improvement_ratio = 999.0 if challenger_comp > 0 else 0.0
    else:
        improvement_ratio = (challenger_comp - active_comp) / abs(active_comp)
    if improvement_ratio < 0.05:
        flags.append("PROMOTION_COMPOSITE_LT_5PCT")
        return {"promoted": False, "reason": reason, "flags": flags}
    active_buy = _safe_float(active_scorecard.get("top5_precision_buy"))
    challenger_buy = _safe_float(challenger_scorecard.get("top5_precision_buy"))
    if active_buy is not None and challenger_buy is not None and (challenger_buy - active_buy) < -0.02:
        flags.append("PROMOTION_BUY_PRECISION_REGRESSION")
        return {"promoted": False, "reason": reason, "flags": flags}
    active_sell = _safe_float(active_scorecard.get("top5_precision_sell"))
    challenger_sell = _safe_float(challenger_scorecard.get("top5_precision_sell"))
    if active_sell is not None and challenger_sell is not None and (challenger_sell - active_sell) < -0.02:
        flags.append("PROMOTION_SELL_PRECISION_REGRESSION")
        return {"promoted": False, "reason": reason, "flags": flags}
    active_mae = _safe_float(active_scorecard.get("mae_mean_pct"))
    challenger_mae = _safe_float(challenger_scorecard.get("mae_mean_pct"))
    if active_mae is not None and challenger_mae is not None and challenger_mae < active_mae:
        flags.append("PROMOTION_MAE_REGRESSION")
        return {"promoted": False, "reason": reason, "flags": flags}
    now = _utc_now_iso()
    conn.execute(
        "UPDATE advisor_model_variants SET status='retired' WHERE id = ?",
        (int(active_variant_id),),
    )
    conn.execute(
        """
        UPDATE advisor_model_variants
        SET status='active', promoted_from_variant_id=?, promoted_at_utc=?
        WHERE id = ?
        """,
        (int(active_variant_id), now, int(challenger_variant_id)),
    )
    conn.commit()
    promoted = True
    reason = "promoted"
    flags.append("CHALLENGER_PROMOTED")
    return {"promoted": promoted, "reason": reason, "flags": flags}


def insert_run_regression(
    conn: sqlite3.Connection,
    *,
    run_id: int,
    cadence: str,
    variant_id: int,
    baseline_variant_id: int,
    window_days: int,
    scorecard: Dict[str, Any],
    gate_status: str,
    regression_flags: Sequence[str],
) -> int:
    cur = conn.execute(
        """
        INSERT INTO advisor_run_regressions(
            run_id, cadence, variant_id, baseline_variant_id, window_days, scorecard_json, gate_status, regression_flags_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            int(run_id),
            str(cadence),
            int(variant_id),
            int(baseline_variant_id),
            int(window_days),
            json.dumps(scorecard, ensure_ascii=True, sort_keys=True),
            str(gate_status),
            json.dumps(list(regression_flags or []), ensure_ascii=True, sort_keys=True),
        ),
    )
    conn.commit()
    return int(cur.lastrowid)


def latest_regression_for_run(conn: sqlite3.Connection, run_id: int) -> Optional[Dict[str, Any]]:
    row = conn.execute(
        """
        SELECT id, run_id, cadence, variant_id, baseline_variant_id, window_days, scorecard_json, gate_status, regression_flags_json
        FROM advisor_run_regressions
        WHERE run_id = ?
        ORDER BY id DESC
        LIMIT 1
        """,
        (int(run_id),),
    ).fetchone()
    if not row:
        return None
    out = dict(row)
    out["scorecard"] = _loads_json(out.pop("scorecard_json", None), {})
    out["regression_flags"] = _loads_json(out.pop("regression_flags_json", None), [])
    return out
