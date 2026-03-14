from typing import Any, Callable, Optional

import typer

from .db import connect, init_db, resolve_db_path
from .opportunities import report_markdown
from iol_advisor.continuous import (
    DEFAULT_WINDOW_DAYS,
    active_variant,
    build_variant_scorecard,
    challenger_variant,
    compare_scorecards,
    list_model_variants,
    evaluate_signal_outcomes,
)


def register_advisor_opportunity_commands(
    advisor_opp_app: typer.Typer,
    advisor_opp_variants_app: typer.Typer,
    *,
    print_json: Callable[[Any], None],
    console: Any,
    run_opportunity_pipeline_impl: Callable[..., Any],
    snapshot_universe_impl: Callable[..., Any],
) -> None:
    @advisor_opp_app.command("snapshot-universe")
    def advisor_opportunities_snapshot_universe(
        ctx: typer.Context,
        as_of: Optional[str] = typer.Option(None, "--as-of", help="Optional YYYY-MM-DD"),
        universe: str = typer.Option("bcba_cedears", "--universe", help="bcba_cedears"),
    ):
        print_json(snapshot_universe_impl(ctx.obj, as_of=as_of, universe=universe))

    @advisor_opp_app.command("run")
    def advisor_opportunities_run(
        ctx: typer.Context,
        budget_ars: float = typer.Option(..., "--budget-ars"),
        mode: str = typer.Option("both", "--mode", help="new|rebuy|both"),
        variant: str = typer.Option("active", "--variant", help="active|challenger|both"),
        as_of: Optional[str] = typer.Option(None, "--as-of", help="Optional YYYY-MM-DD"),
        top: int = typer.Option(10, "--top", min=1, max=100),
        universe: str = typer.Option("bcba_cedears", "--universe", help="bcba_cedears"),
        fetch_evidence: bool = typer.Option(True, "--fetch-evidence/--no-fetch-evidence"),
        evidence_max_symbols: int = typer.Option(15, "--evidence-max-symbols", min=1, max=200),
        evidence_per_source_limit: int = typer.Option(2, "--evidence-per-source-limit", min=1, max=10),
        evidence_news: bool = typer.Option(True, "--evidence-news/--no-evidence-news"),
        evidence_sec: bool = typer.Option(True, "--evidence-sec/--no-evidence-sec"),
        evidence_timeout_sec: int = typer.Option(10, "--evidence-timeout-sec", min=1, max=60),
        web_link: bool = typer.Option(True, "--web-link/--no-web-link"),
        web_top_k: int = typer.Option(15, "--web-top-k", min=1, max=200),
        web_source_policy: str = typer.Option("strict_official_reuters", "--web-source-policy", help="strict_official_reuters"),
        web_lookback_days: int = typer.Option(120, "--web-lookback-days", min=1, max=3650),
        web_min_trusted_refs: int = typer.Option(2, "--web-min-trusted-refs", min=0, max=20),
        web_conflict_mode: str = typer.Option("manual_review", "--web-conflict-mode", help="manual_review"),
        web_reuters: bool = typer.Option(True, "--web-reuters/--no-web-reuters"),
        web_official: bool = typer.Option(True, "--web-official/--no-web-official"),
        exclude_crypto_new: bool = typer.Option(True, "--exclude-crypto-new/--include-crypto-new"),
        min_volume_amount: float = typer.Option(50000.0, "--min-volume-amount", min=0.0),
        min_operations: int = typer.Option(5, "--min-operations", min=0, max=1000000),
        liquidity_priority: bool = typer.Option(True, "--liquidity-priority/--no-liquidity-priority"),
        diversify_sectors: bool = typer.Option(True, "--diversify-sectors/--no-diversify-sectors"),
        max_per_sector: int = typer.Option(2, "--max-per-sector", min=1, max=20),
    ):
        try:
            payload = run_opportunity_pipeline_impl(
                ctx.obj,
                budget_ars=float(budget_ars),
                mode=mode,
                as_of=as_of,
                top=int(top),
                universe=universe,
                fetch_evidence=bool(fetch_evidence),
                evidence_max_symbols=int(evidence_max_symbols),
                evidence_per_source_limit=int(evidence_per_source_limit),
                evidence_news=bool(evidence_news),
                evidence_sec=bool(evidence_sec),
                evidence_timeout_sec=int(evidence_timeout_sec),
                web_link=bool(web_link),
                web_top_k=int(web_top_k),
                web_source_policy=web_source_policy,
                web_lookback_days=int(web_lookback_days),
                web_min_trusted_refs=int(web_min_trusted_refs),
                web_conflict_mode=web_conflict_mode,
                web_reuters=bool(web_reuters),
                web_official=bool(web_official),
                exclude_crypto_new=bool(exclude_crypto_new),
                min_volume_amount=float(min_volume_amount),
                min_operations=int(min_operations),
                liquidity_priority=bool(liquidity_priority),
                diversify_sectors=bool(diversify_sectors),
                max_per_sector=int(max_per_sector),
                variant=variant,
            )
            print_json(payload)
        except Exception as exc:
            console.print(f"Error: {exc}")
            raise typer.Exit(code=1)

    @advisor_opp_app.command("report")
    def advisor_opportunities_report(
        ctx: typer.Context,
        run_id: int = typer.Option(..., "--run-id", min=1),
        out: Optional[str] = typer.Option(None, "--out", help="Optional markdown output file"),
    ):
        db_path = resolve_db_path(ctx.obj.config.db_path)
        conn = connect(db_path)
        init_db(conn)
        try:
            run = conn.execute(
                """
                SELECT id, created_at_utc, as_of, mode, universe, budget_ars, top_n, status, error_message, pipeline_warnings_json, run_metrics_json
                FROM advisor_opportunity_runs
                WHERE id = ?
                """,
                (int(run_id),),
            ).fetchone()
            if not run:
                console.print("Run ID not found.")
                raise typer.Exit(code=1)
            rows = conn.execute(
                """
                SELECT symbol, candidate_type, score_total, score_risk, score_value, score_momentum, score_catalyst,
                       entry_low, entry_high, suggested_weight_pct, suggested_amount_ars, reason_summary, risk_flags_json,
                       filters_passed, expert_signal_score, trusted_refs_count, consensus_state, decision_gate, candidate_status,
                       evidence_summary_json
                FROM advisor_opportunity_candidates
                WHERE run_id = ?
                ORDER BY score_total DESC, symbol ASC
                """,
                (int(run_id),),
            ).fetchall()
        finally:
            conn.close()

        md = report_markdown(dict(run), [dict(r) for r in rows])
        if out:
            with open(out, "w", encoding="utf-8") as f:
                f.write(md)
            print_json({"run_id": int(run_id), "out": out})
            return
        console.print(md)

    @advisor_opp_app.command("list-runs")
    def advisor_opportunities_list_runs(
        ctx: typer.Context,
        limit: int = typer.Option(20, "--limit", min=1, max=500),
        status: Optional[str] = typer.Option(None, "--status", help="ok|error|running"),
    ):
        status_v = status.strip().lower() if status and status.strip() else None
        if status_v is not None and status_v not in ("ok", "error", "running"):
            raise typer.BadParameter("--status must be ok|error|running")

        db_path = resolve_db_path(ctx.obj.config.db_path)
        conn = connect(db_path)
        init_db(conn)
        try:
            if status_v is None:
                rows = conn.execute(
                    """
                    SELECT id, created_at_utc, as_of, mode, universe, budget_ars, top_n, status, error_message, pipeline_warnings_json
                    FROM advisor_opportunity_runs
                    ORDER BY id DESC
                    LIMIT ?
                    """,
                    (int(limit),),
                ).fetchall()
            else:
                rows = conn.execute(
                    """
                    SELECT id, created_at_utc, as_of, mode, universe, budget_ars, top_n, status, error_message, pipeline_warnings_json
                    FROM advisor_opportunity_runs
                    WHERE status = ?
                    ORDER BY id DESC
                    LIMIT ?
                    """,
                    (status_v, int(limit)),
                ).fetchall()
            print_json([dict(r) for r in rows])
        finally:
            conn.close()

    @advisor_opp_variants_app.command("list")
    def advisor_opportunities_variants_list(
        ctx: typer.Context,
    ):
        db_path = resolve_db_path(ctx.obj.config.db_path)
        conn = connect(db_path)
        init_db(conn)
        try:
            rows = [row.to_dict() for row in list_model_variants(conn)]
            print_json(rows)
        finally:
            conn.close()

    @advisor_opp_app.command("evaluate")
    def advisor_opportunities_evaluate(
        ctx: typer.Context,
        as_of: Optional[str] = typer.Option(None, "--as-of", help="Optional YYYY-MM-DD"),
    ):
        db_path = resolve_db_path(ctx.obj.config.db_path)
        conn = connect(db_path)
        init_db(conn)
        try:
            payload = evaluate_signal_outcomes(conn, as_of=as_of)
            print_json(payload)
        finally:
            conn.close()

    @advisor_opp_app.command("scorecard")
    def advisor_opportunities_scorecard(
        ctx: typer.Context,
        as_of: Optional[str] = typer.Option(None, "--as-of", help="Optional YYYY-MM-DD"),
        window_days: int = typer.Option(DEFAULT_WINDOW_DAYS, "--window-days", min=7, max=3650),
    ):
        db_path = resolve_db_path(ctx.obj.config.db_path)
        conn = connect(db_path)
        init_db(conn)
        try:
            active_row = active_variant(conn)
            challenger_row = challenger_variant(conn)
            active_score = build_variant_scorecard(conn, variant_id=int(active_row.id), as_of=as_of, window_days=int(window_days)) if active_row else {}
            challenger_score = build_variant_scorecard(conn, variant_id=int(challenger_row.id), as_of=as_of, window_days=int(window_days)) if challenger_row else {}
            compare = compare_scorecards(active_score, challenger_score)
            print_json(
                {
                    "as_of": as_of,
                    "window_days": int(window_days),
                    "active_variant": active_row.to_dict() if active_row else None,
                    "challenger_variant": challenger_row.to_dict() if challenger_row else None,
                    "active_scorecard": active_score,
                    "challenger_scorecard": challenger_score,
                    "comparison": compare,
                }
            )
        finally:
            conn.close()
