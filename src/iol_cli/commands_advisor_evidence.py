from datetime import date, timedelta
from typing import Any, Callable, Dict, List, Optional

import typer

from .advisor_context import build_advisor_context_from_db_path
from .db import connect, init_db, resolve_db_path
from .opportunities import parse_iso_date


def register_advisor_evidence_commands(
    advisor_evidence_app: typer.Typer,
    *,
    print_json: Callable[[Any], None],
    normalize_enum: Callable[[str, str, set], str],
    confidence_levels: set,
    source_policies: set,
    parse_iso_date_optional: Callable[[Optional[str], str], Optional[str]],
    utc_now_iso: Callable[[], str],
    latest_snapshot_date: Callable[[Any], Optional[str]],
    load_holdings_map_from_context: Callable[[Dict[str, Any]], Dict[str, float]],
    store_evidence_rows: Callable[[Any, List[Dict[str, Any]]], int],
    collect_symbol_evidence_fn: Callable[..., Any],
) -> None:
    @advisor_evidence_app.command("add")
    def advisor_evidence_add(
        ctx: typer.Context,
        symbol: str = typer.Option(..., "--symbol"),
        query: str = typer.Option(..., "--query"),
        source_name: str = typer.Option(..., "--source-name"),
        source_url: str = typer.Option(..., "--source-url"),
        claim: str = typer.Option(..., "--claim"),
        confidence: str = typer.Option(..., "--confidence", help="low|medium|high"),
        date_confidence: str = typer.Option(..., "--date-confidence", help="low|medium|high"),
        published_date: Optional[str] = typer.Option(None, "--published-date", help="Optional YYYY-MM-DD"),
        notes: Optional[str] = typer.Option(None, "--notes"),
        conflict_key: Optional[str] = typer.Option(None, "--conflict-key"),
    ):
        sym = symbol.strip().upper()
        query_v = query.strip()
        source_name_v = source_name.strip()
        source_url_v = source_url.strip()
        claim_v = claim.strip()
        if not sym:
            raise typer.BadParameter("--symbol is required")
        if not query_v:
            raise typer.BadParameter("--query is required")
        if not source_name_v:
            raise typer.BadParameter("--source-name is required")
        if not source_url_v:
            raise typer.BadParameter("--source-url is required")
        if not claim_v:
            raise typer.BadParameter("--claim is required")
        conf_v = normalize_enum(confidence, "--confidence", confidence_levels)
        date_conf_v = normalize_enum(date_confidence, "--date-confidence", confidence_levels)
        pub_v = parse_iso_date_optional(published_date, "--published-date")
        notes_v = notes.strip() if notes and notes.strip() else None
        conflict_v = conflict_key.strip() if conflict_key and conflict_key.strip() else None
        now = utc_now_iso()

        db_path = resolve_db_path(ctx.obj.config.db_path)
        conn = connect(db_path)
        init_db(conn)
        try:
            cur = conn.execute(
                """
                INSERT INTO advisor_evidence (
                    created_at, symbol, query, source_name, source_url, published_date, retrieved_at_utc,
                    claim, confidence, date_confidence, notes, conflict_key
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    now,
                    sym,
                    query_v,
                    source_name_v,
                    source_url_v,
                    pub_v,
                    now,
                    claim_v,
                    conf_v,
                    date_conf_v,
                    notes_v,
                    conflict_v,
                ),
            )
            conn.commit()
            print_json(
                {
                    "id": cur.lastrowid,
                    "symbol": sym,
                    "confidence": conf_v,
                    "date_confidence": date_conf_v,
                    "retrieved_at_utc": now,
                }
            )
        finally:
            conn.close()

    @advisor_evidence_app.command("list")
    def advisor_evidence_list(
        ctx: typer.Context,
        symbol: Optional[str] = typer.Option(None, "--symbol"),
        days: int = typer.Option(60, "--days", min=1, max=3650),
        limit: int = typer.Option(200, "--limit", min=1, max=2000),
    ):
        sym = symbol.strip().upper() if symbol and symbol.strip() else None
        cutoff = (date.today() - timedelta(days=int(days))).isoformat() + "T00:00:00Z"
        where = ["retrieved_at_utc >= ?"]
        params: List[Any] = [cutoff]
        if sym is not None:
            where.append("symbol = ?")
            params.append(sym)
        params.append(int(limit))

        db_path = resolve_db_path(ctx.obj.config.db_path)
        conn = connect(db_path)
        init_db(conn)
        try:
            rows = conn.execute(
                f"""
                SELECT id, created_at, symbol, query, source_name, source_url, published_date, retrieved_at_utc,
                       claim, confidence, date_confidence, notes, conflict_key
                FROM advisor_evidence
                WHERE {" AND ".join(where)}
                ORDER BY retrieved_at_utc DESC, id DESC
                LIMIT ?
                """,
                tuple(params),
            ).fetchall()
            print_json([dict(r) for r in rows])
        finally:
            conn.close()

    @advisor_evidence_app.command("fetch")
    def advisor_evidence_fetch(
        ctx: typer.Context,
        symbols: Optional[str] = typer.Option(None, "--symbols", help="Comma-separated symbols"),
        from_context: bool = typer.Option(True, "--from-context/--no-from-context"),
        from_top_run_id: Optional[int] = typer.Option(None, "--from-top-run-id", min=1, help="Use top candidates from a previous run"),
        as_of: Optional[str] = typer.Option(None, "--as-of", help="Optional YYYY-MM-DD"),
        per_source_limit: int = typer.Option(2, "--per-source-limit", min=1, max=10),
        max_symbols: int = typer.Option(15, "--max-symbols", min=1, max=200),
        include_news: bool = typer.Option(True, "--news/--no-news"),
        include_sec: bool = typer.Option(True, "--sec/--no-sec"),
        source_policy: str = typer.Option("strict_official_reuters", "--source-policy", help="strict_official_reuters"),
        include_reuters: bool = typer.Option(True, "--reuters/--no-reuters"),
        include_official: bool = typer.Option(True, "--official/--no-official"),
        run_stage: str = typer.Option("prelim", "--run-stage", help="prelim|rerank"),
        timeout_sec: int = typer.Option(10, "--timeout-sec", min=1, max=60),
    ):
        source_policy_v = normalize_enum(source_policy, "--source-policy", source_policies)
        run_stage_v = (run_stage or "").strip().lower()
        if run_stage_v not in {"prelim", "rerank"}:
            raise typer.BadParameter("--run-stage must be prelim|rerank")
        db_path = resolve_db_path(ctx.obj.config.db_path)
        conn = connect(db_path)
        init_db(conn)
        try:
            latest_snap = latest_snapshot_date(conn)
        finally:
            conn.close()
        as_of_v = parse_iso_date(as_of, default=latest_snap or date.today().isoformat())

        picked: List[str] = []
        if symbols and symbols.strip():
            for raw in symbols.split(","):
                s = raw.strip().upper()
                if s and s not in picked:
                    picked.append(s)

        if from_context:
            ctx_payload = build_advisor_context_from_db_path(db_path=db_path, as_of=as_of_v, limit=200, history_days=3650)
            holdings_map = load_holdings_map_from_context(ctx_payload)
            for s in sorted(holdings_map.keys()):
                if s not in picked:
                    picked.append(s)
        if from_top_run_id is not None:
            conn = connect(db_path)
            init_db(conn)
            try:
                rows = conn.execute(
                    """
                    SELECT symbol
                    FROM advisor_opportunity_candidates
                    WHERE run_id = ? AND filters_passed = 1
                    ORDER BY score_total DESC, symbol ASC
                    LIMIT ?
                    """,
                    (int(from_top_run_id), int(max_symbols)),
                ).fetchall()
                for r in rows:
                    sym = str(r["symbol"] or "").strip().upper()
                    if sym and sym not in picked:
                        picked.append(sym)
            finally:
                conn.close()

        picked = picked[: int(max_symbols)]
        if not picked:
            print_json({"as_of": as_of_v, "symbols": [], "inserted": 0, "errors": []})
            return

        all_rows: List[Dict[str, Any]] = []
        errors: List[Dict[str, Any]] = []
        for sym in picked:
            rows, errs = collect_symbol_evidence_fn(
                symbol=sym,
                per_source_limit=int(per_source_limit),
                include_news=bool(include_news),
                include_sec=bool(include_sec),
                timeout_sec=int(timeout_sec),
                source_policy=source_policy_v,
                include_reuters=bool(include_reuters),
                include_official=bool(include_official),
                run_stage=run_stage_v,
            )
            all_rows.extend(rows)
            for e in errs:
                errors.append({"symbol": sym, "error": e})

        conn = connect(db_path)
        init_db(conn)
        try:
            inserted = store_evidence_rows(conn, all_rows)
            conn.commit()
        finally:
            conn.close()

        print_json(
            {
                "as_of": as_of_v,
                "symbols": picked,
                "inserted": inserted,
                "fetched_rows": len(all_rows),
                "source_policy": source_policy_v,
                "errors": errors,
            }
        )
