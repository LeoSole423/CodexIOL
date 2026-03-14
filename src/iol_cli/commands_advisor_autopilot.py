from datetime import date
from typing import Any, Callable, Dict, Optional

import typer

from .db import connect, init_db, resolve_db_path
from .opportunities import report_markdown
from iol_advisor.continuous import (
    DEFAULT_WINDOW_DAYS,
    active_variant,
    build_variant_scorecard,
    challenger_variant,
    compare_scorecards,
    ensure_default_model_variants,
    evaluate_signal_outcomes,
    insert_run_regression,
    maybe_promote_challenger,
)
from iol_advisor.service import (
    DEFAULT_SOURCE_POLICY,
    build_unified_context,
    load_latest_opportunity_payload,
    persist_briefing_bundle,
)


def register_advisor_autopilot_commands(
    advisor_autopilot_app: typer.Typer,
    *,
    print_json: Callable[[Any], None],
    normalize_enum: Callable[[str, str, set], str],
    source_policies: set,
    snapshot_universe_impl: Callable[..., Dict[str, Any]],
    run_opportunity_pipeline_impl: Callable[..., Dict[str, Any]],
) -> None:
    @advisor_autopilot_app.command("run")
    def advisor_autopilot_run(
        ctx: typer.Context,
        cadence: str = typer.Option(..., "--cadence", help="daily|weekly"),
        as_of: Optional[str] = typer.Option(None, "--as-of", help="Optional YYYY-MM-DD"),
        budget_ars: float = typer.Option(100000.0, "--budget-ars"),
        top: int = typer.Option(10, "--top", min=1, max=100),
        mode: str = typer.Option("both", "--mode", help="new|rebuy|both"),
        universe: str = typer.Option("bcba_cedears", "--universe", help="bcba_cedears"),
        source_policy: str = typer.Option(DEFAULT_SOURCE_POLICY, "--source-policy", help="strict_official_reuters"),
        out: Optional[str] = typer.Option(None, "--out", help="Optional markdown output file"),
        opportunity_report_out: Optional[str] = typer.Option(None, "--opportunity-report-out", help="Optional weekly opportunities markdown"),
        force: bool = typer.Option(False, "--force", help="Persist a new briefing even if same cadence+as_of already exists"),
    ):
        cadence_v = cadence.strip().lower()
        if cadence_v not in ("daily", "weekly"):
            raise typer.BadParameter("--cadence must be daily|weekly")
        source_policy_v = normalize_enum(source_policy, "--source-policy", source_policies)
        db_path = resolve_db_path(ctx.obj.config.db_path)
        conn = connect(db_path)
        init_db(conn)
        try:
            ensure_default_model_variants(conn)
            eval_summary = evaluate_signal_outcomes(conn, as_of=as_of)
            active_before = active_variant(conn)
            challenger_before = challenger_variant(conn)
            active_score_before = build_variant_scorecard(conn, variant_id=int(active_before.id), as_of=as_of, window_days=DEFAULT_WINDOW_DAYS) if active_before else {}
            challenger_score_before = build_variant_scorecard(conn, variant_id=int(challenger_before.id), as_of=as_of, window_days=DEFAULT_WINDOW_DAYS) if challenger_before else {}
            comparison_before = compare_scorecards(active_score_before, challenger_score_before)
        finally:
            conn.close()

        latest_run = None
        run_payload: Dict[str, Any] = {}
        if cadence_v == "weekly":
            snapshot_universe_impl(ctx.obj, as_of=as_of, universe=universe)
            run_payload = run_opportunity_pipeline_impl(
                ctx.obj,
                budget_ars=float(budget_ars),
                mode=mode,
                as_of=as_of,
                top=int(top),
                universe=universe,
                fetch_evidence=True,
                evidence_max_symbols=15,
                evidence_per_source_limit=2,
                evidence_news=True,
                evidence_sec=True,
                evidence_timeout_sec=10,
                web_link=True,
                web_top_k=15,
                web_source_policy=source_policy_v,
                web_lookback_days=120,
                web_min_trusted_refs=2,
                web_conflict_mode="manual_review",
                web_reuters=True,
                web_official=True,
                exclude_crypto_new=True,
                min_volume_amount=50000.0,
                min_operations=5,
                liquidity_priority=True,
                diversify_sectors=True,
                max_per_sector=2,
                variant="both",
                cadence=cadence_v,
                reuse_existing=True,
            )
            if opportunity_report_out and run_payload:
                active_rows = list(run_payload.get("variant_runs") or [])
                active_report = next((row for row in active_rows if int(row.get("variant_id") or 0) == int(run_payload.get("active_variant_id") or 0)), None) or run_payload
                conn = connect(db_path)
                init_db(conn)
                try:
                    latest_run = load_latest_opportunity_payload(db_path) or active_report
                finally:
                    conn.close()
                md = report_markdown(latest_run, latest_run.get("candidates") or [])
                with open(opportunity_report_out, "w", encoding="utf-8") as f:
                    f.write(md)
        else:
            run_payload = run_opportunity_pipeline_impl(
                ctx.obj,
                budget_ars=float(budget_ars),
                mode=mode,
                as_of=as_of,
                top=int(top),
                universe=universe,
                fetch_evidence=True,
                evidence_max_symbols=15,
                evidence_per_source_limit=2,
                evidence_news=True,
                evidence_sec=True,
                evidence_timeout_sec=10,
                web_link=True,
                web_top_k=15,
                web_source_policy=source_policy_v,
                web_lookback_days=120,
                web_min_trusted_refs=2,
                web_conflict_mode="manual_review",
                web_reuters=True,
                web_official=True,
                exclude_crypto_new=True,
                min_volume_amount=50000.0,
                min_operations=5,
                liquidity_priority=True,
                diversify_sectors=True,
                max_per_sector=2,
                variant="both",
                cadence=cadence_v,
                reuse_existing=True,
            )

        conn = connect(db_path)
        init_db(conn)
        try:
            ensure_default_model_variants(conn)
            active_current = active_variant(conn)
            challenger_current = challenger_variant(conn)
            active_score_after = build_variant_scorecard(conn, variant_id=int(active_current.id), as_of=as_of, window_days=DEFAULT_WINDOW_DAYS) if active_current else {}
            challenger_score_after = build_variant_scorecard(conn, variant_id=int(challenger_current.id), as_of=as_of, window_days=DEFAULT_WINDOW_DAYS) if challenger_current else {}
            comparison_after = compare_scorecards(active_score_after, challenger_score_after)
            promotion = {"promoted": False, "reason": "not_weekly", "flags": []}
            if cadence_v == "weekly" and active_before and challenger_before and active_score_after and challenger_score_after:
                promotion = maybe_promote_challenger(
                    conn,
                    active_variant_id=int(active_before.id),
                    challenger_variant_id=int(challenger_before.id),
                    active_scorecard=active_score_after,
                    challenger_scorecard=challenger_score_after,
                )
                if promotion.get("promoted"):
                    active_current = active_variant(conn)
                    challenger_current = challenger_variant(conn)
                    active_score_after = build_variant_scorecard(conn, variant_id=int(active_current.id), as_of=as_of, window_days=DEFAULT_WINDOW_DAYS) if active_current else {}
                    challenger_score_after = build_variant_scorecard(conn, variant_id=int(challenger_current.id), as_of=as_of, window_days=DEFAULT_WINDOW_DAYS) if challenger_current else {}
                    comparison_after = compare_scorecards(active_score_after, challenger_score_after)
                    comparison_after["regression_flags"] = list(
                        dict.fromkeys(list(comparison_after.get("regression_flags") or []) + list(promotion.get("flags") or []))
                    )

            for row in (run_payload.get("variant_runs") or []):
                variant_id = int(row.get("variant_id") or 0)
                scorecard = active_score_after if active_current and variant_id == int(active_current.id) else challenger_score_after
                gate_status = comparison_after.get("gate_status") if active_current and variant_id == int(active_current.id) else str((scorecard or {}).get("status") or "ok")
                regression_flags = comparison_after.get("regression_flags") if active_current and variant_id == int(active_current.id) else list(promotion.get("flags") or [])
                baseline_id = int(active_before.id if active_before else variant_id)
                if row.get("run_id"):
                    insert_run_regression(
                        conn,
                        run_id=int(row["run_id"]),
                        cadence=cadence_v,
                        variant_id=variant_id,
                        baseline_variant_id=baseline_id,
                        window_days=DEFAULT_WINDOW_DAYS,
                        scorecard=dict(scorecard or {}),
                        gate_status=str(gate_status or "ok"),
                        regression_flags=list(regression_flags or []),
                    )

            active_runs = list(run_payload.get("variant_runs") or [])
            latest_run = next((row for row in active_runs if active_current and int(row.get("variant_id") or 0) == int(active_current.id)), None)
            if latest_run is None:
                latest_run = load_latest_opportunity_payload(db_path)
            regression_payload = {
                "gate_status": comparison_after.get("gate_status"),
                "regression_flags": list(comparison_after.get("regression_flags") or []),
                "scorecard": dict(active_score_after or {}),
                "comparison": comparison_after,
                "promotion": promotion,
            }
            active_variant_payload = active_current.to_dict() if active_current else None
        finally:
            conn.close()

        context = build_unified_context(
            db_path,
            as_of=as_of,
            limit=10,
            history_days=365,
            include_cash=True,
            include_orders=False,
        )

        if cadence_v == "daily" and latest_run:
            try:
                snapshot_date = str(((context or {}).get("snapshot") or {}).get("snapshot_date") or "")
                if snapshot_date and latest_run.get("as_of"):
                    age_days = (date.fromisoformat(snapshot_date) - date.fromisoformat(str(latest_run.get("as_of")))).days
                    if age_days > 7:
                        latest_run = None
            except Exception:
                pass

        bundle = persist_briefing_bundle(
            db_path=db_path,
            cadence=cadence_v,
            env=ctx.obj.env,
            base_url=ctx.obj.base_url,
            context=context,
            latest_run=latest_run,
            regression=regression_payload,
            active_variant=active_variant_payload,
            source_policy=source_policy_v,
            force=bool(force),
        )
        briefing = bundle.briefing
        if out and briefing:
            with open(out, "w", encoding="utf-8") as f:
                f.write(str(briefing.get("summary_md") or ""))
        print_json(
            {
                "briefing": briefing,
                "reused": bool(bundle.reused),
                "weekly_run_id": (latest_run or {}).get("id"),
                "evaluation": eval_summary,
                "comparison_before": comparison_before,
                "comparison_after": regression_payload,
                "active_variant": active_variant_payload,
                "out": out,
                "opportunity_report_out": opportunity_report_out,
            }
        )
