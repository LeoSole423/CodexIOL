from __future__ import annotations

import csv
import sys
from typing import Any, Callable, Optional

import typer
from rich.console import Console

from .batch import BatchError, plan_from_md, plan_template, run_batch
from .db import connect, init_db, resolve_db_path
from .iol_client import IOLAPIError
from .snapshot import backfill_orders_and_snapshot, catchup_snapshot, run_snapshot


console = Console()

ALLOWED_EXPORT_TABLES = (
    "portfolio_snapshots",
    "portfolio_assets",
    "account_balances",
    "orders",
    "manual_cashflow_adjustments",
    "account_cash_movements",
    "snapshot_runs",
    "advisor_logs",
    "advisor_alerts",
    "advisor_events",
    "advisor_evidence",
    "market_symbol_snapshots",
    "advisor_model_variants",
    "advisor_opportunity_runs",
    "advisor_opportunity_candidates",
    "advisor_signal_outcomes",
    "advisor_run_regressions",
    "reconciliation_runs",
    "reconciliation_intervals",
    "reconciliation_proposals",
    "reconciliation_resolutions",
    "batch_runs",
    "batch_ops",
)


def build_snapshot_app(
    *,
    get_client: Callable[[Any], Any],
    parse_date: Callable[[str], Any],
    print_json: Callable[[Any], None],
) -> typer.Typer:
    snapshot_app = typer.Typer(help="Snapshots")

    @snapshot_app.command("run")
    def snapshot_run(
        ctx: typer.Context,
        country: str = typer.Option("argentina", "--country"),
        source: str = typer.Option("manual", "--source"),
        force: bool = typer.Option(
            False,
            "--force",
            help="Overwrite an existing snapshot even if it is closer to the market close time.",
        ),
        mode: str = typer.Option(
            "close",
            "--mode",
            help="Snapshot mode: 'close' (default) keeps daily semantics; 'live' uses today's date intraday.",
        ),
        only_market_open: bool = typer.Option(
            False,
            "--only-market-open",
            help="If set, skips snapshot when the market is closed (uses IOL_MARKET_OPEN_TIME/IOL_MARKET_CLOSE_TIME).",
        ),
    ):
        client = get_client(ctx.obj)
        try:
            result = run_snapshot(
                client,
                ctx.obj.config,
                country,
                source=source,
                force=force,
                mode=mode,
                only_market_open=only_market_open,
            )
            print_json(result)
        except Exception as exc:
            console.print(f"Snapshot error: {exc}")
            raise typer.Exit(code=1)

    @snapshot_app.command("catchup")
    def snapshot_catchup(
        ctx: typer.Context,
        country: str = typer.Option("argentina", "--country"),
    ):
        client = get_client(ctx.obj)
        try:
            result = catchup_snapshot(client, ctx.obj.config, country)
            print_json(result)
        except Exception as exc:
            console.print(f"Catchup error: {exc}")
            raise typer.Exit(code=1)

    @snapshot_app.command("backfill")
    def snapshot_backfill(
        ctx: typer.Context,
        date_from: str = typer.Option(..., "--from"),
        date_to: str = typer.Option(..., "--to"),
        country: str = typer.Option("argentina", "--country"),
    ):
        client = get_client(ctx.obj)
        try:
            result = backfill_orders_and_snapshot(
                client,
                ctx.obj.config,
                country,
                parse_date(date_from),
                parse_date(date_to),
            )
            print_json(result)
        except Exception as exc:
            console.print(f"Backfill error: {exc}")
            raise typer.Exit(code=1)

    return snapshot_app


def build_batch_app(
    *,
    get_client: Callable[[Any], Any],
    print_json: Callable[[Any], None],
) -> typer.Typer:
    batch_app = typer.Typer(help="Batch execution from a JSON plan")

    @batch_app.command("template")
    def batch_template():
        """Print an example batch plan JSON."""
        print_json(plan_template())

    @batch_app.command("validate")
    def batch_validate(
        ctx: typer.Context,
        plan: str = typer.Option(..., "--plan", help="Path to plan JSON"),
        price_mode: Optional[str] = typer.Option(None, "--price-mode", help="fast|mid|last override"),
        default_market: str = typer.Option("bcba", "--default-market"),
        default_plazo: str = typer.Option("t1", "--default-plazo"),
    ):
        client = get_client(ctx.obj)
        try:
            result = run_batch(
                client=client,
                config=ctx.obj.config,
                plan_path=plan,
                dry_run=True,
                price_mode_override=price_mode,
                default_market=default_market,
                default_plazo=default_plazo,
                confirm_enabled=False,
            )
            print_json(result)
        except (BatchError, IOLAPIError) as exc:
            console.print(f"Error: {exc}")
            raise typer.Exit(code=1)

    @batch_app.command("run")
    def batch_run(
        ctx: typer.Context,
        plan: str = typer.Option(..., "--plan", help="Path to plan JSON"),
        dry_run: bool = typer.Option(False, "--dry-run", help="Validate only; do not execute"),
        price_mode: Optional[str] = typer.Option(None, "--price-mode", help="fast|mid|last override"),
        default_market: str = typer.Option("bcba", "--default-market"),
        default_plazo: str = typer.Option("t1", "--default-plazo"),
        confirm: Optional[str] = typer.Option(
            None,
            "--confirm",
            help="Execute orders by passing CONFIRMAR. Without this flag the command behaves as dry-run.",
        ),
    ):
        confirm_enabled = False
        if confirm is not None:
            if confirm.strip() != "CONFIRMAR":
                raise typer.BadParameter("--confirm must be exactly CONFIRMAR")
            confirm_enabled = True

        def preview_printer(ops: Any) -> None:
            console.print("Preview (prepared ops):")
            print_json({"ops": ops})

        client = get_client(ctx.obj)
        try:
            result = run_batch(
                client=client,
                config=ctx.obj.config,
                plan_path=plan,
                dry_run=bool(dry_run),
                price_mode_override=price_mode,
                default_market=default_market,
                default_plazo=default_plazo,
                confirm_enabled=confirm_enabled,
                on_preview=preview_printer if (confirm_enabled and not dry_run) else None,
            )
            print_json(result)
        except (BatchError, IOLAPIError) as exc:
            console.print(f"Error: {exc}")
            raise typer.Exit(code=1)

    @batch_app.command("from-md")
    def batch_from_md(
        md: str = typer.Option(..., "--md", help="Markdown file path"),
        out: str = typer.Option(..., "--out", help="Output JSON plan path"),
    ):
        """Parse a rebalance markdown and generate a plan JSON."""
        try:
            plan = plan_from_md(md, out)
            print_json({"out": out, "plan": plan})
        except BatchError as exc:
            console.print(f"Error: {exc}")
            raise typer.Exit(code=1)

    return batch_app


def build_data_app(*, print_json: Callable[[Any], None]) -> typer.Typer:
    data_app = typer.Typer(help="Local data access")

    @data_app.command("query")
    def data_query(
        ctx: typer.Context,
        sql: str = typer.Argument(..., help="SQL SELECT query"),
    ):
        query = sql.strip()
        if not query.lower().startswith("select"):
            console.print("Only SELECT queries are allowed.")
            raise typer.Exit(code=1)
        db_path = resolve_db_path(ctx.obj.config.db_path)
        conn = connect(db_path)
        init_db(conn)
        try:
            rows = conn.execute(query).fetchall()
            data = [dict(row) for row in rows]
            print_json(data)
        finally:
            conn.close()

    @data_app.command("detect-pivots")
    def detect_pivots_cmd(
        ctx: typer.Context,
        as_of: str = typer.Option(None, "--as-of", help="Date YYYY-MM-DD (default: today)"),
        strength: int = typer.Option(3, "--strength", help="Bars to confirm pivot"),
        lookback: int = typer.Option(60, "--lookback-days", help="Days of OHLCV to analyze"),
        symbol: str = typer.Option(None, "--symbol", help="Single symbol (default: all)"),
    ):
        """Detect and store pivot highs/lows from OHLCV data."""
        from datetime import date as _date
        from iol_engines.market_data_ohlcv import detect_pivots, detect_pivots_all_symbols
        from rich.table import Table

        target = as_of or _date.today().isoformat()

        db_path = resolve_db_path(ctx.obj.config.db_path)
        conn = connect(db_path)
        init_db(conn)

        if symbol:
            pivots = detect_pivots(conn, symbol, target, lookback_days=lookback, strength=strength)
            results = {"symbols_processed": 1, "pivots_detected": len(pivots), "by_symbol": {symbol: pivots} if pivots else {}}
        else:
            results = detect_pivots_all_symbols(conn, target, strength=strength, lookback_days=lookback)

        console.print(f"[bold]Pivot detection[/bold] as_of={target} strength={strength}")
        console.print(f"Symbols: {results['symbols_processed']}  Pivots detected: {results['pivots_detected']}")

        if results["by_symbol"]:
            table = Table("Symbol", "Date", "Type", "Price", "Strength")
            for sym, plist in results["by_symbol"].items():
                for p in plist:
                    color = "green" if p["pivot_type"] == "low" else "red"
                    table.add_row(sym, p["pivot_date"], f"[{color}]{p['pivot_type']}[/{color}]",
                                  str(p["price"]), str(p["strength"]))
            console.print(table)
        conn.close()

    @data_app.command("export")
    def data_export(
        ctx: typer.Context,
        table: str = typer.Option(..., "--table"),
        fmt: str = typer.Option("json", "--format"),
    ):
        table_name = table.strip()
        if table_name not in ALLOWED_EXPORT_TABLES:
            console.print("Invalid table name.")
            raise typer.Exit(code=1)
        db_path = resolve_db_path(ctx.obj.config.db_path)
        conn = connect(db_path)
        init_db(conn)
        try:
            rows = conn.execute(f"SELECT * FROM {table_name}").fetchall()
            data = [dict(row) for row in rows]
        finally:
            conn.close()
        fmt_norm = fmt.strip().lower()
        if fmt_norm == "json":
            print_json(data)
            return
        if fmt_norm == "csv":
            if not data:
                return
            writer = csv.DictWriter(sys.stdout, fieldnames=data[0].keys())
            writer.writeheader()
            for row in data:
                writer.writerow(row)
            return
        console.print("Unsupported format. Use json or csv.")
        raise typer.Exit(code=1)

    return data_app
