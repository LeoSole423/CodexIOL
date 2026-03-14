import json
from datetime import date
from typing import Any, Callable, Dict, List, Optional

import typer

from .advisor_context import build_advisor_context_from_db_path
from .advisor_opportunity_support import (
    CONFLICT_MODES,
    OPP_MODES,
    OPP_UNIVERSES,
    SOURCE_POLICIES,
    VARIANT_SELECTORS,
    latest_snapshot_date,
    load_evidence_rows_grouped,
    load_holdings_context_from_db,
    load_holdings_map_from_context,
    load_market_snapshot_rows,
    normalize_enum,
    pick_symbols_for_web_link,
)
from .db import connect, init_db, resolve_db_path
from .opportunities import (
    build_candidates,
    latest_metrics_by_symbol,
    panel_rows,
    parse_iso_date,
    price_series_by_symbol,
    snapshot_row_from_panel,
    snapshot_row_from_quote,
    summarize_run_metrics,
)
from .util import normalize_country, normalize_market
from iol_advisor.continuous import active_variant, ensure_default_model_variants, resolve_variant_selection
from iol_advisor.service import find_reusable_opportunity_run


def snapshot_universe_impl(
    cli_ctx: Any,
    *,
    as_of: Optional[str],
    universe: str,
    get_client_fn: Callable[[Any], Any],
) -> Dict[str, Any]:
    universe_v = normalize_enum(universe, "--universe", OPP_UNIVERSES)
    db_path = resolve_db_path(cli_ctx.config.db_path)
    conn = connect(db_path)
    init_db(conn)
    try:
        latest_snap = latest_snapshot_date(conn)
    finally:
        conn.close()
    as_of_v = parse_iso_date(as_of, default=latest_snap or date.today().isoformat())

    ctx_payload = build_advisor_context_from_db_path(db_path=db_path, as_of=as_of_v, limit=200, history_days=3650)
    holdings_map = load_holdings_map_from_context(ctx_payload)
    symbols = set(holdings_map.keys())

    client = get_client_fn(cli_ctx)
    panel_data: List[Dict[str, Any]] = []
    if universe_v == "bcba_cedears":
        try:
            panel_payload = client.get_panel_quotes("Acciones", "CEDEARs", normalize_country("argentina"))
            panel_data = panel_rows(panel_payload)
        except Exception:
            panel_data = []

    rows_to_upsert: List[Dict[str, Any]] = []
    for r in panel_data:
        pr = snapshot_row_from_panel(as_of_v, r, market="bcba")
        if pr is None:
            continue
        rows_to_upsert.append(pr)
        symbols.add(str(pr["symbol"]))

    quote_errors: List[Dict[str, Any]] = []
    for sym in sorted(symbols):
        try:
            quote = client.get_quote(normalize_market("bcba"), sym)
            rows_to_upsert.append(snapshot_row_from_quote(as_of_v, sym, quote, market="bcba"))
        except Exception as exc:
            quote_errors.append({"symbol": sym, "error": str(exc)})

    conn = connect(db_path)
    init_db(conn)
    try:
        for r in rows_to_upsert:
            conn.execute(
                """
                INSERT INTO market_symbol_snapshots (
                    snapshot_date, symbol, market, last_price, bid, ask, spread_pct,
                    daily_var_pct, operations_count, volume_amount, source
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(snapshot_date, symbol, source) DO UPDATE SET
                    market=excluded.market,
                    last_price=excluded.last_price,
                    bid=excluded.bid,
                    ask=excluded.ask,
                    spread_pct=excluded.spread_pct,
                    daily_var_pct=excluded.daily_var_pct,
                    operations_count=excluded.operations_count,
                    volume_amount=excluded.volume_amount
                """,
                (
                    r["snapshot_date"],
                    r["symbol"],
                    r["market"],
                    r["last_price"],
                    r["bid"],
                    r["ask"],
                    r["spread_pct"],
                    r["daily_var_pct"],
                    r["operations_count"],
                    r["volume_amount"],
                    r["source"],
                ),
            )
        conn.commit()
    finally:
        conn.close()

    return {
        "as_of": as_of_v,
        "universe": universe_v,
        "rows_upserted": len(rows_to_upsert),
        "symbols_considered": len(symbols),
        "panel_rows": len(panel_data),
        "quote_errors": quote_errors,
    }


def run_opportunity_pipeline_impl(
    cli_ctx: Any,
    *,
    budget_ars: float,
    mode: str,
    as_of: Optional[str],
    top: int,
    universe: str,
    fetch_evidence: bool,
    evidence_max_symbols: int,
    evidence_per_source_limit: int,
    evidence_news: bool,
    evidence_sec: bool,
    evidence_timeout_sec: int,
    web_link: bool,
    web_top_k: int,
    web_source_policy: str,
    web_lookback_days: int,
    web_min_trusted_refs: int,
    web_conflict_mode: str,
    web_reuters: bool,
    web_official: bool,
    exclude_crypto_new: bool,
    min_volume_amount: float,
    min_operations: int,
    liquidity_priority: bool,
    diversify_sectors: bool,
    max_per_sector: int,
    variant: str = "active",
    cadence: Optional[str] = None,
    reuse_existing: bool = False,
    utc_now_iso_fn: Callable[[], str] = None,
    collect_symbol_evidence_fn: Callable[..., Any] = None,
    store_evidence_rows_fn: Callable[[Any, List[Dict[str, Any]]], int] = None,
) -> Dict[str, Any]:
    if float(budget_ars) <= 0:
        raise typer.BadParameter("--budget-ars must be > 0")
    mode_v = normalize_enum(mode, "--mode", OPP_MODES)
    universe_v = normalize_enum(universe, "--universe", OPP_UNIVERSES)
    web_source_policy_v = normalize_enum(web_source_policy, "--web-source-policy", SOURCE_POLICIES)
    web_conflict_mode_v = normalize_enum(web_conflict_mode, "--web-conflict-mode", CONFLICT_MODES)

    db_path = resolve_db_path(cli_ctx.config.db_path)
    conn = connect(db_path)
    init_db(conn)
    try:
        ensure_default_model_variants(conn)
        latest_snap = latest_snapshot_date(conn)
        as_of_v = parse_iso_date(as_of, default=latest_snap or date.today().isoformat())
        if variant in VARIANT_SELECTORS and variant == "both":
            selected = resolve_variant_selection(conn, "both")
            if not selected:
                raise typer.BadParameter("--variant both requires active/challenger variants")
            results: List[Dict[str, Any]] = []
            for row in selected:
                results.append(
                    run_opportunity_pipeline_impl(
                        cli_ctx,
                        budget_ars=float(budget_ars),
                        mode=mode_v,
                        as_of=as_of_v,
                        top=int(top),
                        universe=universe_v,
                        fetch_evidence=bool(fetch_evidence),
                        evidence_max_symbols=int(evidence_max_symbols),
                        evidence_per_source_limit=int(evidence_per_source_limit),
                        evidence_news=bool(evidence_news),
                        evidence_sec=bool(evidence_sec),
                        evidence_timeout_sec=int(evidence_timeout_sec),
                        web_link=bool(web_link),
                        web_top_k=int(web_top_k),
                        web_source_policy=web_source_policy_v,
                        web_lookback_days=int(web_lookback_days),
                        web_min_trusted_refs=int(web_min_trusted_refs),
                        web_conflict_mode=web_conflict_mode_v,
                        web_reuters=bool(web_reuters),
                        web_official=bool(web_official),
                        exclude_crypto_new=bool(exclude_crypto_new),
                        min_volume_amount=float(min_volume_amount),
                        min_operations=int(min_operations),
                        liquidity_priority=bool(liquidity_priority),
                        diversify_sectors=bool(diversify_sectors),
                        max_per_sector=int(max_per_sector),
                        variant=str(row.id),
                        cadence=cadence,
                        reuse_existing=bool(reuse_existing),
                        utc_now_iso_fn=utc_now_iso_fn,
                        collect_symbol_evidence_fn=collect_symbol_evidence_fn,
                        store_evidence_rows_fn=store_evidence_rows_fn,
                    )
                )
            current_active = active_variant(conn)
            active_row = None
            if current_active is not None:
                for res in results:
                    if int(res.get("variant_id") or 0) == int(current_active.id):
                        active_row = res
                        break
            if active_row is None:
                active_row = results[0]
            return {
                "variant": "both",
                "variant_runs": results,
                "active_variant_id": active_row.get("variant_id"),
                "run_id": active_row.get("run_id"),
                "as_of": as_of_v,
                "mode": mode_v,
                "universe": universe_v,
                "budget_ars": float(budget_ars),
                "top_n": int(top),
                "pipeline_warnings": list(active_row.get("pipeline_warnings") or []),
                "run_metrics": dict(active_row.get("run_metrics") or {}),
                "candidates_total": int(active_row.get("candidates_total") or 0),
                "top_operable": list(active_row.get("top_operable") or []),
                "watchlist": list(active_row.get("watchlist") or []),
                "manual_review": list(active_row.get("manual_review") or []),
                "reused": bool(all(bool(r.get("reused")) for r in results)),
            }
        selected = resolve_variant_selection(conn, variant)
        if len(selected) != 1:
            raise typer.BadParameter("--variant must be active|challenger|both or a valid variant id")
        variant_row = selected[0]
        variant_cfg = dict(variant_row.config or {})
        score_version = str(variant_cfg.get("score_version") or variant_row.name)
        if reuse_existing:
            existing = find_reusable_opportunity_run(
                conn,
                as_of=as_of_v,
                mode=mode_v,
                universe=universe_v,
                budget_ars=float(budget_ars),
                top_n=int(top),
                variant_id=int(variant_row.id),
            )
            if existing:
                return {
                    "run_id": int(existing["id"]),
                    "variant_id": int(variant_row.id),
                    "variant_name": variant_row.name,
                    "score_version": score_version,
                    "as_of": as_of_v,
                    "mode": mode_v,
                    "universe": universe_v,
                    "budget_ars": float(budget_ars),
                    "top_n": int(top),
                    "evidence_fetch": {
                        "enabled": False,
                        "symbols": [],
                        "fetched_rows": 0,
                        "inserted": 0,
                        "errors": [],
                        "source_policy": web_source_policy_v,
                    },
                    "pipeline_warnings": list(existing.get("pipeline_warnings") or []),
                    "run_metrics": dict(existing.get("run_metrics") or {}),
                    "candidates_total": len(existing.get("candidates") or []),
                    "top_operable": list(existing.get("top_operable") or []),
                    "watchlist": list(existing.get("watchlist") or []),
                    "manual_review": [
                        c for c in (existing.get("candidates") or [])
                        if str(c.get("candidate_status") or "").strip().lower() == "manual_review"
                    ][: int(top)],
                    "reused": True,
                }
    finally:
        conn.close()

    cfg = {
        "weights": dict(variant_cfg.get("weights") or {"risk": 0.35, "value": 0.20, "momentum": 0.35, "catalyst": 0.10}),
        "thresholds": {
            "spread_pct_max": 2.5,
            "concentration_pct_max": 15.0,
            "new_asset_initial_cap_pct": 8.0,
            "drawdown_exclusion_pct": -25.0,
            "rebuy_dip_threshold_pct": -8.0,
            "exclude_crypto_new": bool(exclude_crypto_new),
            "min_volume_amount": float(min_volume_amount),
            "min_operations": int(min_operations),
            "liquidity_priority": bool(liquidity_priority),
            "diversify_sectors": bool(diversify_sectors),
            "max_per_sector": int(max_per_sector) if bool(diversify_sectors) else 0,
            "trim_weight_pct": float(((variant_cfg.get("thresholds") or {}).get("trim_weight_pct") or 12.0)),
            "exit_weight_pct": float(((variant_cfg.get("thresholds") or {}).get("exit_weight_pct") or 15.0)),
            "sell_momentum_max": float(((variant_cfg.get("thresholds") or {}).get("sell_momentum_max") or 35.0)),
            "exit_momentum_max": float(((variant_cfg.get("thresholds") or {}).get("exit_momentum_max") or 20.0)),
            "sell_conflict_exit": bool((variant_cfg.get("thresholds") or {}).get("sell_conflict_exit", True)),
            "liquidity_floor": float(((variant_cfg.get("thresholds") or {}).get("liquidity_floor") or 40.0)),
        },
        "variant": variant_row.to_dict(),
        "web_link": {
            "enabled": bool(web_link),
            "top_k": int(web_top_k),
            "source_policy": web_source_policy_v,
            "lookback_days": int(web_lookback_days),
            "min_trusted_refs": int(web_min_trusted_refs),
            "conflict_mode": web_conflict_mode_v,
            "reuters": bool(web_reuters),
            "official": bool(web_official),
        },
    }

    now = utc_now_iso_fn()
    run_id = None
    conn = connect(db_path)
    init_db(conn)
    try:
        cur = conn.execute(
            """
            INSERT INTO advisor_opportunity_runs (
                created_at_utc, as_of, mode, universe, budget_ars, top_n, variant_id, score_version, status, error_message, config_json, pipeline_warnings_json, run_metrics_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                now,
                as_of_v,
                mode_v,
                universe_v,
                float(budget_ars),
                int(top),
                int(variant_row.id),
                score_version,
                "running",
                None,
                json.dumps(cfg, ensure_ascii=True),
                None,
                None,
            ),
        )
        run_id = int(cur.lastrowid)
        conn.commit()
    finally:
        conn.close()

    try:
        ctx_payload = build_advisor_context_from_db_path(db_path=db_path, as_of=as_of_v, limit=500, history_days=3650)
        portfolio_total = float(((ctx_payload or {}).get("snapshot") or {}).get("total_value_ars") or 0.0)
        holdings_map = load_holdings_map_from_context(ctx_payload)

        conn = connect(db_path)
        init_db(conn)
        try:
            market_rows = load_market_snapshot_rows(conn, as_of_v)
            evidence_map = load_evidence_rows_grouped(conn, as_of_v, lookback_days=int(web_lookback_days))
            holdings_context = load_holdings_context_from_db(conn, as_of_v)
        finally:
            conn.close()

        latest_metrics = latest_metrics_by_symbol(market_rows, as_of_v)
        if not latest_metrics:
            raise RuntimeError("NO_MARKET_SNAPSHOTS: run 'iol advisor opportunities snapshot-universe' first")

        series_by_symbol = price_series_by_symbol(market_rows, as_of_v)
        prelim_candidates = build_candidates(
            as_of=as_of_v,
            mode=mode_v,
            budget_ars=float(budget_ars),
            top_n=int(top),
            portfolio_total_ars=portfolio_total,
            holdings_value_by_symbol=holdings_map,
            latest_metrics=latest_metrics,
            series_by_symbol=series_by_symbol,
            evidence_by_symbol=evidence_map,
            holdings_context_by_symbol=holdings_context,
            min_trusted_refs=0,
            apply_expert_overlay=False,
            conflict_mode=web_conflict_mode_v,
            exclude_crypto_new=bool(exclude_crypto_new),
            min_volume_amount=float(min_volume_amount),
            min_operations=int(min_operations),
            liquidity_priority=bool(liquidity_priority),
            max_per_sector=0,
            weights=dict(cfg.get("weights") or {}),
            thresholds=dict(cfg.get("thresholds") or {}),
            score_version=score_version,
        )

        pipeline_warnings: List[str] = []
        web_link_enabled = bool(web_link and fetch_evidence)
        evidence_fetch_summary: Dict[str, Any] = {
            "enabled": bool(web_link_enabled),
            "symbols": [],
            "fetched_rows": 0,
            "inserted": 0,
            "errors": [],
            "source_policy": web_source_policy_v,
        }
        if web_link_enabled:
            auto_symbols = pick_symbols_for_web_link(
                holdings_map=holdings_map,
                prelim_candidates=[c.to_dict() for c in prelim_candidates],
                top_k=int(web_top_k),
            )
            auto_symbols = auto_symbols[: int(evidence_max_symbols)]
            evidence_fetch_summary["symbols"] = auto_symbols
            collected: List[Dict[str, Any]] = []
            fetch_errors: List[Dict[str, Any]] = []
            for sym in auto_symbols:
                rows, errs = collect_symbol_evidence_fn(
                    symbol=sym,
                    per_source_limit=int(evidence_per_source_limit),
                    include_news=bool(evidence_news),
                    include_sec=bool(evidence_sec),
                    timeout_sec=int(evidence_timeout_sec),
                    source_policy=web_source_policy_v,
                    include_reuters=bool(web_reuters),
                    include_official=bool(web_official),
                    run_stage="rerank",
                )
                collected.extend(rows)
                for e in errs:
                    fetch_errors.append({"symbol": sym, "error": e})

            conn = connect(db_path)
            init_db(conn)
            try:
                inserted_rows = store_evidence_rows_fn(conn, collected)
                conn.commit()
            finally:
                conn.close()
            evidence_fetch_summary["fetched_rows"] = len(collected)
            evidence_fetch_summary["inserted"] = int(inserted_rows)
            evidence_fetch_summary["errors"] = fetch_errors
            if fetch_errors:
                pipeline_warnings.append("WEB_FETCH_PARTIAL_ERRORS")

            conn = connect(db_path)
            init_db(conn)
            try:
                evidence_map = load_evidence_rows_grouped(conn, as_of_v, lookback_days=int(web_lookback_days))
            finally:
                conn.close()

        has_recent_evidence = any(bool(v) for v in evidence_map.values())
        apply_web_overlay = bool(
            web_link_enabled
            and (int(evidence_fetch_summary.get("inserted") or 0) > 0 or has_recent_evidence)
        )
        min_refs_final = int(web_min_trusted_refs) if apply_web_overlay else 0
        if web_link_enabled and not apply_web_overlay:
            pipeline_warnings.append("WEB_FETCH_EMPTY_FALLBACK_TO_QUANT")

        rerank_symbols = set(
            pick_symbols_for_web_link(
                holdings_map=holdings_map,
                prelim_candidates=[c.to_dict() for c in prelim_candidates],
                top_k=int(web_top_k),
            )
        )
        latest_metrics_final = {sym: row for sym, row in latest_metrics.items() if sym in rerank_symbols}
        series_by_symbol_final = {sym: row for sym, row in series_by_symbol.items() if sym in rerank_symbols}
        evidence_map_final = {sym: evidence_map.get(sym, []) for sym in rerank_symbols}

        final_candidates = build_candidates(
            as_of=as_of_v,
            mode=mode_v,
            budget_ars=float(budget_ars),
            top_n=int(top),
            portfolio_total_ars=portfolio_total,
            holdings_value_by_symbol=holdings_map,
            latest_metrics=latest_metrics_final,
            series_by_symbol=series_by_symbol_final,
            evidence_by_symbol=evidence_map_final,
            holdings_context_by_symbol=holdings_context,
            min_trusted_refs=min_refs_final,
            apply_expert_overlay=apply_web_overlay,
            conflict_mode=web_conflict_mode_v,
            exclude_crypto_new=bool(exclude_crypto_new),
            min_volume_amount=float(min_volume_amount),
            min_operations=int(min_operations),
            liquidity_priority=bool(liquidity_priority),
            max_per_sector=int(max_per_sector) if bool(diversify_sectors) else 0,
            weights=dict(cfg.get("weights") or {}),
            thresholds=dict(cfg.get("thresholds") or {}),
            score_version=score_version,
        )
        final_symbols = {str(c.symbol).strip().upper() for c in final_candidates}
        prelim_non_operable = [
            c
            for c in prelim_candidates
            if str(c.symbol).strip().upper() not in final_symbols and str(c.candidate_status) != "operable"
        ]
        candidates = list(final_candidates) + prelim_non_operable
        run_metrics = summarize_run_metrics(candidates)

        conn = connect(db_path)
        init_db(conn)
        try:
            conn.execute("DELETE FROM advisor_opportunity_candidates WHERE run_id = ?", (int(run_id),))
            for c in candidates:
                d = c.to_dict()
                conn.execute(
                    """
                    INSERT INTO advisor_opportunity_candidates (
                        run_id, symbol, candidate_type, signal_side, signal_family, score_version, score_total, score_risk, score_value, score_momentum,
                        score_catalyst, entry_low, entry_high, suggested_weight_pct, suggested_amount_ars,
                        reason_summary, risk_flags_json, filters_passed, expert_signal_score,
                        trusted_refs_count, consensus_state, decision_gate, candidate_status, evidence_summary_json, liquidity_score, sector_bucket, is_crypto_proxy,
                        holding_context_json, score_features_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        int(run_id),
                        d["symbol"],
                        d["candidate_type"],
                        d.get("signal_side"),
                        d.get("signal_family"),
                        d.get("score_version"),
                        float(d["score_total"]),
                        float(d["score_risk"]),
                        float(d["score_value"]),
                        float(d["score_momentum"]),
                        float(d["score_catalyst"]),
                        d["entry_low"],
                        d["entry_high"],
                        d["suggested_weight_pct"],
                        d["suggested_amount_ars"],
                        d["reason_summary"],
                        d["risk_flags_json"],
                        int(d["filters_passed"]),
                        float(d.get("expert_signal_score") or 0.0),
                        int(d.get("trusted_refs_count") or 0),
                        str(d.get("consensus_state") or "insufficient"),
                        str(d.get("decision_gate") or "auto"),
                        str(d.get("candidate_status") or "watchlist"),
                        str(d.get("evidence_summary_json") or "{}"),
                        float(d.get("liquidity_score") or 0.0),
                        str(d.get("sector_bucket") or "unknown"),
                        int(d.get("is_crypto_proxy") or 0),
                        str(d.get("holding_context_json") or "{}"),
                        str(d.get("score_features_json") or "{}"),
                    ),
                )
            warnings_json = json.dumps(pipeline_warnings, ensure_ascii=True) if pipeline_warnings else None
            conn.execute(
                "UPDATE advisor_opportunity_runs SET status='ok', error_message=NULL, pipeline_warnings_json=?, run_metrics_json=? WHERE id = ?",
                (warnings_json, json.dumps(run_metrics, ensure_ascii=True, sort_keys=True), int(run_id)),
            )
            conn.commit()
        finally:
            conn.close()

        operable_rows = [c.to_dict() for c in candidates if str(c.candidate_status) == "operable"][: int(top)]
        manual_rows = [
            c.to_dict()
            for c in candidates
            if str(c.candidate_status).strip().lower() == "manual_review"
        ][: int(top)]
        watchlist_rows = [
            c.to_dict()
            for c in candidates
            if str(c.candidate_status).strip().lower() == "watchlist"
        ][: int(top)]
        return {
            "run_id": int(run_id),
            "variant_id": int(variant_row.id),
            "variant_name": variant_row.name,
            "score_version": score_version,
            "as_of": as_of_v,
            "mode": mode_v,
            "universe": universe_v,
            "budget_ars": float(budget_ars),
            "top_n": int(top),
            "evidence_fetch": evidence_fetch_summary,
            "pipeline_warnings": pipeline_warnings,
            "run_metrics": run_metrics,
            "candidates_total": len(candidates),
            "top_operable": operable_rows,
            "watchlist": watchlist_rows,
            "manual_review": manual_rows,
            "reused": False,
        }
    except Exception as exc:
        conn = connect(db_path)
        init_db(conn)
        try:
            conn.execute(
                "UPDATE advisor_opportunity_runs SET status='error', error_message=?, pipeline_warnings_json=?, run_metrics_json=? WHERE id = ?",
                (str(exc), json.dumps(["RUN_ERROR"], ensure_ascii=True), None, int(run_id)),
            )
            conn.commit()
        finally:
            conn.close()
        raise
