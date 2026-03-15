"""CLI commands for the paper-trading simulation framework.

Sub-app tree registered in cli.py:
  iol simulate run
  iol simulate list
  iol simulate show
  iol simulate compare
  iol simulate bots
"""
from __future__ import annotations

from typing import Any, Callable, List, Optional

import typer
from rich.console import Console

console = Console()


def build_simulate_app(
    *,
    print_json: Callable[[Any], None],
) -> typer.Typer:
    simulate_app = typer.Typer(help="Paper-trading simulation and backtesting")

    # ── bots ────────────────────────────────────────────────────────────────

    @simulate_app.command("bots")
    def bots():
        """List available bot presets (conservative, balanced, growth)."""
        from iol_engines.simulation.bot_config import PRESETS

        from rich.table import Table

        table = Table(title="Bot Presets", show_lines=False)
        table.add_column("Name", style="bold", width=14)
        table.add_column("Risk", justify="right", width=6)
        table.add_column("Value", justify="right", width=6)
        table.add_column("Mom.", justify="right", width=6)
        table.add_column("Cat.", justify="right", width=6)
        table.add_column("Regime Infl.", justify="right", width=13)
        table.add_column("Max Pos%", justify="right", width=9)
        table.add_column("Reserve%", justify="right", width=9)
        table.add_column("Min Score", justify="right", width=10)

        for name, cfg in PRESETS.items():
            w = cfg.weights
            table.add_row(
                name,
                f"{w['risk']:.0%}",
                f"{w['value']:.0%}",
                f"{w['momentum']:.0%}",
                f"{w['catalyst']:.0%}",
                f"{cfg.regime_influence:.0%}",
                f"{cfg.max_position_pct:.0%}",
                f"{cfg.cash_reserve_pct:.0%}",
                f"{cfg.min_score_threshold:.0f}",
            )

        console.print(table)
        console.print(
            "\nRun a backtest: [bold]iol simulate run --bot-config balanced "
            "--date-from 2024-01-01 --date-to 2024-12-31 --initial-cash-ars 1000000[/bold]"
        )

    # ── run ─────────────────────────────────────────────────────────────────

    @simulate_app.command("run")
    def run(
        ctx: typer.Context,
        bot_config: str = typer.Option(
            "balanced",
            "--bot-config",
            help=f"Bot preset: conservative | balanced | growth",
        ),
        date_from: str = typer.Option(..., "--date-from", help="Start date YYYY-MM-DD"),
        date_to: str = typer.Option(..., "--date-to", help="End date YYYY-MM-DD"),
        initial_cash_ars: float = typer.Option(
            1_000_000.0, "--initial-cash-ars", help="Starting ARS cash"
        ),
        json_out: bool = typer.Option(False, "--json"),
    ):
        """Run a backtest for the given date range using a named bot config."""
        from iol_cli.db import connect, init_db, resolve_db_path
        from iol_engines.simulation.bot_config import get_preset
        from iol_engines.simulation.report import load_run
        from iol_engines.simulation.runner import run_backtest

        db_path = resolve_db_path(ctx.obj.config.db_path)
        conn = connect(db_path)
        init_db(conn)

        try:
            config = get_preset(bot_config)
        except ValueError as exc:
            console.print(f"[red]{exc}[/red]")
            raise typer.Exit(code=1)

        run_id = run_backtest(
            conn,
            config,
            date_from,
            date_to,
            initial_cash_ars,
            verbose=not json_out,
        )

        result = load_run(conn, run_id)
        if result is None:
            console.print("[red]Could not load run result.[/red]")
            raise typer.Exit(code=1)

        if json_out:
            print_json(result)
        else:
            _print_run_summary(result)
            console.print(
                f"\nView trades:  [bold]iol simulate show --run-id {run_id} --trades[/bold]"
            )
            console.print(
                f"Compare runs: [bold]iol simulate compare --run-ids {run_id},...[/bold]"
            )

    # ── list ────────────────────────────────────────────────────────────────

    @simulate_app.command("list")
    def list_runs(
        ctx: typer.Context,
        bot: Optional[str] = typer.Option(None, "--bot", help="Filter by bot name"),
        limit: int = typer.Option(20, "--limit"),
        json_out: bool = typer.Option(False, "--json"),
    ):
        """List recent simulation runs."""
        from iol_cli.db import connect, init_db, resolve_db_path
        from iol_engines.simulation.report import list_runs as _list

        db_path = resolve_db_path(ctx.obj.config.db_path)
        conn = connect(db_path)
        init_db(conn)

        runs = _list(conn, limit=limit, bot_name=bot)

        if json_out:
            print_json(runs)
            return

        if not runs:
            console.print("[yellow]No simulation runs found.[/yellow]")
            return

        from rich.table import Table

        table = Table(title="Simulation Runs", show_lines=False)
        table.add_column("ID", justify="right", width=5)
        table.add_column("Bot", width=14)
        table.add_column("From", width=12)
        table.add_column("To", width=12)
        table.add_column("Status", width=8)
        table.add_column("Return%", justify="right", width=9)
        table.add_column("Sharpe", justify="right", width=7)
        table.add_column("MaxDD%", justify="right", width=8)
        table.add_column("Final ARS", justify="right", width=14)

        for r in runs:
            ret = r.get("total_return_pct")
            ret_color = "green" if (ret or 0) >= 0 else "red"
            ret_str = f"[{ret_color}]{ret:+.1f}%[/{ret_color}]" if ret is not None else "-"
            table.add_row(
                str(r["id"]),
                r["bot_name"],
                r["date_from"],
                r["date_to"],
                r["status"],
                ret_str,
                f"{r['sharpe_ratio']:.2f}" if r.get("sharpe_ratio") else "-",
                f"{r['max_drawdown_pct']:.1f}%" if r.get("max_drawdown_pct") else "-",
                f"{r['final_value_ars']:,.0f}" if r.get("final_value_ars") else "-",
            )

        console.print(table)

    # ── show ────────────────────────────────────────────────────────────────

    @simulate_app.command("show")
    def show(
        ctx: typer.Context,
        run_id: int = typer.Option(..., "--run-id"),
        trades: bool = typer.Option(False, "--trades", help="Also show paper trades"),
        json_out: bool = typer.Option(False, "--json"),
    ):
        """Show details for a single simulation run."""
        from iol_cli.db import connect, init_db, resolve_db_path
        from iol_engines.simulation.report import load_run, load_trades

        db_path = resolve_db_path(ctx.obj.config.db_path)
        conn = connect(db_path)
        init_db(conn)

        result = load_run(conn, run_id)
        if result is None:
            console.print(f"[red]Run #{run_id} not found.[/red]")
            raise typer.Exit(code=1)

        trade_rows = load_trades(conn, run_id) if trades else []

        if json_out:
            result["trades"] = trade_rows
            print_json(result)
            return

        _print_run_summary(result)

        if trades and trade_rows:
            _print_trades_table(trade_rows)

    # ── compare ─────────────────────────────────────────────────────────────

    @simulate_app.command("compare")
    def compare(
        ctx: typer.Context,
        run_ids: str = typer.Option(..., "--run-ids", help="Comma-separated run IDs, e.g. 1,2,3"),
        json_out: bool = typer.Option(False, "--json"),
    ):
        """Compare multiple simulation runs side-by-side."""
        from iol_cli.db import connect, init_db, resolve_db_path
        from iol_engines.simulation.report import compare_runs

        try:
            ids = [int(x.strip()) for x in run_ids.split(",")]
        except ValueError:
            console.print("[red]--run-ids must be comma-separated integers.[/red]")
            raise typer.Exit(code=1)

        db_path = resolve_db_path(ctx.obj.config.db_path)
        conn = connect(db_path)
        init_db(conn)

        result = compare_runs(conn, ids)

        if json_out:
            print_json(result)
            return

        if "error" in result:
            console.print(f"[red]{result['error']}[/red]")
            raise typer.Exit(code=1)

        console.rule("[bold]Simulation Comparison[/bold]")
        if result.get("winner"):
            console.print(f"  Winner: [bold green]{result['winner']}[/bold green]")
        console.print()

        from rich.table import Table

        table = Table(show_lines=True)
        table.add_column("Metric", style="dim", width=18)
        for r in result["runs"]:
            table.add_column(f"{r['bot_name']} (#{r['run_id']})", justify="right", width=18)

        def _row(label: str, key: str, fmt: str = "", suffix: str = "") -> None:
            cells = []
            for r in result["runs"]:
                val = r.get(key)
                if val is None:
                    cells.append("-")
                else:
                    cells.append(f"{val:{fmt}}{suffix}" if fmt else str(val))
            table.add_row(label, *cells)

        _row("Date from", "date_from")
        _row("Date to", "date_to")
        _row("Days", "n_days")
        _row("Initial ARS", "initial_value_ars", ",.0f")
        _row("Final ARS", "final_value_ars", ",.0f")
        _row("Total return", "total_return_pct", "+.1f", "%")
        _row("Sharpe ratio", "sharpe_ratio", ".3f")
        _row("Max drawdown", "max_drawdown_pct", ".1f", "%")
        _row("Win rate", "win_rate_pct", ".1f", "%")
        _row("Turnover", "turnover_pct", ".1f", "%")

        console.print(table)

    # ── live-step ────────────────────────────────────────────────────────────

    @simulate_app.command("live-step")
    def live_step(
        ctx: typer.Context,
        bots: str = typer.Option(
            "all", "--bots",
            help="Comma-separated bot names or 'all' (conservative,balanced,growth)",
        ),
        as_of: Optional[str] = typer.Option(None, "--as-of", help="Date YYYY-MM-DD (default: today)"),
        initial_cash_ars: float = typer.Option(
            1_000_000.0, "--initial-cash-ars",
            help="Starting ARS (only used when creating a new live run)",
        ),
        json_out: bool = typer.Option(False, "--json"),
    ):
        """Execute one live paper-trading step for each bot using today's engine signals.

        Designed to be called daily by the scheduler after market close.
        Each bot maintains a persistent monthly run in the DB.
        """
        from datetime import date as _date

        from iol_cli.db import connect, init_db, resolve_db_path
        from iol_engines.simulation.runner import run_live_step

        target_date = as_of or _date.today().isoformat()
        all_presets = ["conservative", "balanced", "growth"]
        bot_list = all_presets if bots.strip().lower() == "all" else [
            b.strip().lower() for b in bots.split(",")
        ]

        db_path = resolve_db_path(ctx.obj.config.db_path)
        conn = connect(db_path)
        init_db(conn)

        run_ids = run_live_step(
            conn, bot_list, target_date,
            initial_cash_ars=initial_cash_ars,
            verbose=not json_out,
        )

        if json_out:
            print_json({"date": target_date, "bots": bot_list, "run_ids": run_ids})
        elif run_ids:
            console.print(
                f"\n[green]Live step complete.[/green] "
                f"Run IDs: {', '.join(str(r) for r in run_ids)}"
            )
            console.print("View: [bold]iol simulate list[/bold]")

    # ── compare-all ──────────────────────────────────────────────────────────

    @simulate_app.command("compare-all")
    def compare_all(
        ctx: typer.Context,
        days: int = typer.Option(30, "--days", help="Look-back window in days"),
        json_out: bool = typer.Option(False, "--json"),
    ):
        """Compare all live bots over their most recent monthly runs - leaderboard."""
        from datetime import date as _date, timedelta

        from iol_cli.db import connect, init_db, resolve_db_path

        db_path = resolve_db_path(ctx.obj.config.db_path)
        conn = connect(db_path)
        init_db(conn)

        cutoff = (_date.today() - timedelta(days=days)).isoformat()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT r.id, c.name, r.date_from, r.date_to,
                   r.total_return_pct, r.sharpe_ratio, r.max_drawdown_pct,
                   r.final_value_ars, r.initial_value_ars,
                   r.engine_driven, r.avg_regime_score
            FROM simulation_runs r
            JOIN simulation_bot_configs c ON r.bot_config_id = c.id
            WHERE r.mode = 'live' AND r.date_from >= ?
            ORDER BY c.name, r.id DESC
            """,
            (cutoff,),
        )
        rows = cur.fetchall()

        seen: set = set()
        records = []
        for row in rows:
            bot_name = row[1]
            if bot_name not in seen:
                seen.add(bot_name)
                records.append(dict(zip(
                    ["id", "bot_name", "date_from", "date_to",
                     "total_return_pct", "sharpe_ratio", "max_drawdown_pct",
                     "final_value_ars", "initial_value_ars",
                     "engine_driven", "avg_regime_score"],
                    row,
                )))

        if not records:
            console.print(
                f"[yellow]No live runs found in the last {days} days. "
                "Run:[/yellow] iol simulate live-step --bots all"
            )
            return

        records.sort(key=lambda r: (r.get("total_return_pct") or -999), reverse=True)

        if json_out:
            print_json({"window_days": days, "bots": records})
            return

        from rich.table import Table

        console.rule("[bold]Live Bot Leaderboard[/bold]")
        console.print(f"  Leader: [bold green]{records[0]['bot_name']}[/bold green]\n")

        table = Table(show_lines=True)
        table.add_column("#", justify="right", width=3)
        table.add_column("Bot", width=14)
        table.add_column("From", width=11)
        table.add_column("To", width=11)
        table.add_column("Return%", justify="right", width=9)
        table.add_column("Sharpe", justify="right", width=7)
        table.add_column("MaxDD%", justify="right", width=8)
        table.add_column("Régimen avg", justify="right", width=12)
        table.add_column("Engines", width=8)

        for rank, r in enumerate(records, 1):
            ret = r.get("total_return_pct")
            ret_color = "green" if (ret or 0) >= 0 else "red"
            ret_str = f"[{ret_color}]{ret:+.1f}%[/{ret_color}]" if ret is not None else "-"
            rs = r.get("avg_regime_score")
            table.add_row(
                str(rank),
                r["bot_name"],
                r["date_from"],
                r["date_to"],
                ret_str,
                f"{r['sharpe_ratio']:.2f}" if r.get("sharpe_ratio") else "-",
                f"{r['max_drawdown_pct']:.1f}%" if r.get("max_drawdown_pct") else "-",
                f"{rs:.0f}/100" if rs is not None else "-",
                "[green]on[/green]" if r.get("engine_driven") else "[dim]off[/dim]",
            )

        console.print(table)

    # ── swing sub-app ────────────────────────────────────────────────────────
    swing_app = _build_swing_app(print_json=print_json)
    simulate_app.add_typer(swing_app, name="swing")

    # ── event sub-app ────────────────────────────────────────────────────────
    event_app = _build_event_app(print_json=print_json)
    simulate_app.add_typer(event_app, name="event")

    return simulate_app


# ── Swing sub-app ─────────────────────────────────────────────────────────────

def _build_swing_app(*, print_json) -> typer.Typer:
    app = typer.Typer(help="Swing trading simulation (3-10 day holds)")

    @app.command("bots")
    def swing_bots():
        """List swing bot presets."""
        from iol_engines.simulation.swing_bot_config import list_swing_presets
        from rich.table import Table

        table = Table(title="Swing Bot Presets", show_lines=False)
        table.add_column("Name", style="bold", width=20)
        table.add_column("Hold (days)", justify="center", width=11)
        table.add_column("Stop%", justify="right", width=7)
        table.add_column("Target%", justify="right", width=8)
        table.add_column("Trailing ATR×", justify="right", width=14)
        table.add_column("Max Pos", justify="right", width=8)
        table.add_column("Min Score", justify="right", width=10)

        for cfg in list_swing_presets():
            table.add_row(
                cfg.name,
                f"{cfg.min_hold_days}–{cfg.max_hold_days}",
                f"{cfg.stop_loss_pct:.0%}",
                f"{cfg.take_profit_pct:.0%}",
                f"{cfg.trailing_atr_mult:.1f}×",
                str(cfg.max_positions),
                f"{cfg.min_engine_score:.0f}",
            )
        console.print(table)
        console.print(
            "\nRun a backtest: [bold]iol simulate swing run --bot swing-balanced "
            "--date-from 2025-01-01 --date-to 2026-03-01 --initial-cash-ars 1000000[/bold]"
        )

    @app.command("run")
    def swing_run(
        ctx: typer.Context,
        bot: str = typer.Option("swing-balanced", "--bot"),
        date_from: str = typer.Option(..., "--date-from"),
        date_to: str = typer.Option(..., "--date-to"),
        initial_cash_ars: float = typer.Option(1_000_000.0, "--initial-cash-ars"),
        json_out: bool = typer.Option(False, "--json"),
    ):
        """Run a swing trading backtest."""
        from iol_cli.db import connect, init_db, resolve_db_path
        from iol_engines.simulation.swing_bot_config import get_swing_preset
        from iol_engines.simulation.swing_runner import run_swing_backtest

        db_path = resolve_db_path(ctx.obj.config.db_path)
        conn = connect(db_path)
        init_db(conn)

        try:
            config = get_swing_preset(bot)
        except ValueError as exc:
            console.print(f"[red]{exc}[/red]")
            raise typer.Exit(code=1)

        run_id = run_swing_backtest(conn, config, date_from, date_to, initial_cash_ars,
                                    verbose=not json_out)
        result = _load_swing_run(conn, run_id)
        if json_out:
            print_json(result)
        else:
            _print_swing_run_summary(result)
            console.print(f"\nView trades: [bold]iol simulate swing show --run-id {run_id} --trades[/bold]")

    @app.command("list")
    def swing_list(
        ctx: typer.Context,
        bot: Optional[str] = typer.Option(None, "--bot"),
        limit: int = typer.Option(20, "--limit"),
        json_out: bool = typer.Option(False, "--json"),
    ):
        """List recent swing simulation runs."""
        from iol_cli.db import connect, init_db, resolve_db_path

        db_path = resolve_db_path(ctx.obj.config.db_path)
        conn = connect(db_path)
        init_db(conn)

        where = "WHERE bot_name = ?" if bot else ""
        params = (bot, limit) if bot else (limit,)
        rows = conn.execute(
            f"""
            SELECT id, bot_name, date_from, date_to, total_return_pct,
                   sharpe_ratio, max_drawdown_pct, avg_hold_days, total_trades, final_value
            FROM swing_simulation_runs
            {where}
            ORDER BY id DESC LIMIT ?
            """,
            params,
        ).fetchall()

        if json_out:
            cols = ["id", "bot_name", "date_from", "date_to", "total_return_pct",
                    "sharpe_ratio", "max_drawdown_pct", "avg_hold_days", "total_trades", "final_value"]
            print_json([dict(zip(cols, r)) for r in rows])
            return

        if not rows:
            console.print("[yellow]No swing runs found.[/yellow]")
            return

        from rich.table import Table
        table = Table(title="Swing Simulation Runs", show_lines=False)
        table.add_column("ID", justify="right", width=5)
        table.add_column("Bot", width=20)
        table.add_column("From", width=12)
        table.add_column("To", width=12)
        table.add_column("Return%", justify="right", width=9)
        table.add_column("Sharpe", justify="right", width=7)
        table.add_column("MaxDD%", justify="right", width=8)
        table.add_column("Avg Hold", justify="right", width=9)
        table.add_column("Trades", justify="right", width=7)

        for r in rows:
            ret = r[4]
            ret_color = "green" if (ret or 0) >= 0 else "red"
            ret_str = f"[{ret_color}]{ret:+.1f}%[/{ret_color}]" if ret is not None else "-"
            table.add_row(
                str(r[0]), r[1], r[2], r[3], ret_str,
                f"{r[5]:.2f}" if r[5] else "-",
                f"{r[6]:.1f}%" if r[6] else "-",
                f"{r[7]:.1f}d" if r[7] else "-",
                str(r[8]) if r[8] else "-",
            )
        console.print(table)

    @app.command("show")
    def swing_show(
        ctx: typer.Context,
        run_id: int = typer.Option(..., "--run-id"),
        trades: bool = typer.Option(False, "--trades"),
        json_out: bool = typer.Option(False, "--json"),
    ):
        """Show details for a swing simulation run."""
        from iol_cli.db import connect, init_db, resolve_db_path

        db_path = resolve_db_path(ctx.obj.config.db_path)
        conn = connect(db_path)
        init_db(conn)

        result = _load_swing_run(conn, run_id)
        if not result:
            console.print(f"[red]Swing run #{run_id} not found.[/red]")
            raise typer.Exit(code=1)

        trade_rows = []
        if trades:
            trade_rows = conn.execute(
                """
                SELECT symbol, entry_date, exit_date, entry_price, exit_price,
                       amount_ars, pnl_ars, return_pct, hold_days, exit_reason
                FROM swing_simulation_trades
                WHERE run_id = ?
                ORDER BY entry_date, id
                """,
                (run_id,),
            ).fetchall()

        if json_out:
            result["trades"] = trade_rows
            print_json(result)
            return

        _print_swing_run_summary(result)
        if trades and trade_rows:
            _print_swing_trades_table(trade_rows)

    @app.command("live-step")
    def swing_live_step(
        ctx: typer.Context,
        bots: str = typer.Option("all", "--bots"),
        as_of: Optional[str] = typer.Option(None, "--as-of"),
        initial_cash_ars: float = typer.Option(1_000_000.0, "--initial-cash-ars"),
        json_out: bool = typer.Option(False, "--json"),
    ):
        """Execute one daily live paper-trading step for swing bots."""
        from datetime import date as _date
        from iol_cli.db import connect, init_db, resolve_db_path
        from iol_engines.simulation.swing_runner import run_swing_live_step

        target_date = as_of or _date.today().isoformat()
        all_swing = ["swing-conservative", "swing-balanced", "swing-aggressive"]
        bot_list = all_swing if bots.strip().lower() == "all" else [b.strip() for b in bots.split(",")]

        db_path = resolve_db_path(ctx.obj.config.db_path)
        conn = connect(db_path)
        init_db(conn)

        run_ids = run_swing_live_step(conn, bot_list, target_date,
                                      initial_cash_ars=initial_cash_ars, verbose=not json_out)

        if json_out:
            print_json({"date": target_date, "bots": bot_list, "run_ids": run_ids})
        elif run_ids:
            console.print(f"\n[green]Swing live step complete.[/green] Run IDs: {', '.join(str(r) for r in run_ids)}")

    return app


# ── Event sub-app ─────────────────────────────────────────────────────────────

def _build_event_app(*, print_json) -> typer.Typer:
    app = typer.Typer(help="Event-driven trading simulation (reacts to engine events)")

    @app.command("bots")
    def event_bots():
        """List event bot presets."""
        from iol_engines.simulation.event_bot_config import list_event_presets
        from rich.table import Table

        table = Table(title="Event Bot Presets", show_lines=False)
        table.add_column("Name", style="bold", width=22)
        table.add_column("Max Pos", justify="right", width=8)
        table.add_column("Reserve%", justify="right", width=9)
        table.add_column("Min Score", justify="right", width=10)
        table.add_column("Cooldown (d)", justify="right", width=12)
        table.add_column("Reaction rules", justify="right", width=15)

        for cfg in list_event_presets():
            table.add_row(
                cfg.name,
                str(cfg.max_positions),
                f"{cfg.cash_reserve_pct:.0%}",
                f"{cfg.min_engine_score:.0f}",
                str(cfg.hold_after_event_days),
                str(len(cfg.reaction_rules)),
            )
        console.print(table)
        console.print(
            "\nDetect events: [bold]iol simulate event detect --as-of 2026-03-15[/bold]"
        )

    @app.command("run")
    def event_run(
        ctx: typer.Context,
        bot: str = typer.Option("event-adaptive", "--bot"),
        date_from: str = typer.Option(..., "--date-from"),
        date_to: str = typer.Option(..., "--date-to"),
        initial_cash_ars: float = typer.Option(1_000_000.0, "--initial-cash-ars"),
        json_out: bool = typer.Option(False, "--json"),
    ):
        """Run an event-driven backtest."""
        from iol_cli.db import connect, init_db, resolve_db_path
        from iol_engines.simulation.event_bot_config import get_event_preset
        from iol_engines.simulation.event_runner import run_event_backtest

        db_path = resolve_db_path(ctx.obj.config.db_path)
        conn = connect(db_path)
        init_db(conn)

        try:
            config = get_event_preset(bot)
        except ValueError as exc:
            console.print(f"[red]{exc}[/red]")
            raise typer.Exit(code=1)

        run_id = run_event_backtest(conn, config, date_from, date_to, initial_cash_ars,
                                    verbose=not json_out)
        result = _load_event_run(conn, run_id)
        if json_out:
            print_json(result)
        else:
            _print_event_run_summary(result)
            console.print(f"\nView trades: [bold]iol simulate event show --run-id {run_id} --trades[/bold]")

    @app.command("list")
    def event_list(
        ctx: typer.Context,
        bot: Optional[str] = typer.Option(None, "--bot"),
        limit: int = typer.Option(20, "--limit"),
        json_out: bool = typer.Option(False, "--json"),
    ):
        """List recent event simulation runs."""
        from iol_cli.db import connect, init_db, resolve_db_path

        db_path = resolve_db_path(ctx.obj.config.db_path)
        conn = connect(db_path)
        init_db(conn)

        where = "WHERE bot_name = ?" if bot else ""
        params = (bot, limit) if bot else (limit,)
        rows = conn.execute(
            f"""
            SELECT id, bot_name, date_from, date_to, total_return_pct,
                   sharpe_ratio, max_drawdown_pct, total_events_triggered, total_trades, final_value
            FROM event_simulation_runs
            {where}
            ORDER BY id DESC LIMIT ?
            """,
            params,
        ).fetchall()

        if json_out:
            cols = ["id", "bot_name", "date_from", "date_to", "total_return_pct",
                    "sharpe_ratio", "max_drawdown_pct", "total_events_triggered", "total_trades", "final_value"]
            print_json([dict(zip(cols, r)) for r in rows])
            return

        if not rows:
            console.print("[yellow]No event runs found.[/yellow]")
            return

        from rich.table import Table
        table = Table(title="Event Simulation Runs", show_lines=False)
        table.add_column("ID", justify="right", width=5)
        table.add_column("Bot", width=20)
        table.add_column("From", width=12)
        table.add_column("To", width=12)
        table.add_column("Return%", justify="right", width=9)
        table.add_column("Sharpe", justify="right", width=7)
        table.add_column("Events", justify="right", width=7)
        table.add_column("Trades", justify="right", width=7)

        for r in rows:
            ret = r[4]
            ret_color = "green" if (ret or 0) >= 0 else "red"
            ret_str = f"[{ret_color}]{ret:+.1f}%[/{ret_color}]" if ret is not None else "-"
            table.add_row(
                str(r[0]), r[1], r[2], r[3], ret_str,
                f"{r[5]:.2f}" if r[5] else "-",
                str(r[7]) if r[7] is not None else "-",
                str(r[8]) if r[8] is not None else "-",
            )
        console.print(table)

    @app.command("show")
    def event_show(
        ctx: typer.Context,
        run_id: int = typer.Option(..., "--run-id"),
        trades: bool = typer.Option(False, "--trades"),
        json_out: bool = typer.Option(False, "--json"),
    ):
        """Show details for an event simulation run."""
        from iol_cli.db import connect, init_db, resolve_db_path

        db_path = resolve_db_path(ctx.obj.config.db_path)
        conn = connect(db_path)
        init_db(conn)

        result = _load_event_run(conn, run_id)
        if not result:
            console.print(f"[red]Event run #{run_id} not found.[/red]")
            raise typer.Exit(code=1)

        trade_rows = []
        if trades:
            trade_rows = conn.execute(
                """
                SELECT symbol, trade_date, action, amount_ars, price, pnl_ars,
                       trigger_event_type, portfolio_value_after
                FROM event_simulation_trades
                WHERE run_id = ?
                ORDER BY rowid
                """,
                (run_id,),
            ).fetchall()

        if json_out:
            result["trades"] = trade_rows
            print_json(result)
            return

        _print_event_run_summary(result)
        if trades and trade_rows:
            _print_event_trades_table(trade_rows)

    @app.command("detect")
    def event_detect(
        ctx: typer.Context,
        as_of: Optional[str] = typer.Option(None, "--as-of"),
        json_out: bool = typer.Option(False, "--json"),
    ):
        """Detect and display current engine events without executing trades."""
        from datetime import date as _date
        from iol_cli.db import connect, init_db, resolve_db_path
        from iol_engines.simulation.event_detector import detect_all_events

        target_date = as_of or _date.today().isoformat()
        db_path = resolve_db_path(ctx.obj.config.db_path)
        conn = connect(db_path)
        init_db(conn)

        events = detect_all_events(conn, target_date)

        if json_out:
            print_json([
                {"event_type": e.event_type, "severity": e.severity,
                 "symbol": e.symbol, "description": e.description, "payload": e.payload}
                for e in events
            ])
            return

        if not events:
            console.print(f"[green]No events detected as of {target_date}.[/green]")
            return

        console.rule(f"[bold]Engine Events - {target_date}[/bold]")
        _SEV_COLOR = {"critical": "red", "high": "yellow", "medium": "cyan"}
        for ev in events:
            color = _SEV_COLOR.get(ev.severity, "white")
            sym_str = f" [{ev.symbol}]" if ev.symbol else ""
            console.print(
                f"  [{color}]{ev.severity.upper():9s}[/{color}] "
                f"[bold]{ev.event_type}[/bold]{sym_str}  {ev.description}"
            )

    @app.command("live-step")
    def event_live_step(
        ctx: typer.Context,
        bots: str = typer.Option("all", "--bots"),
        as_of: Optional[str] = typer.Option(None, "--as-of"),
        initial_cash_ars: float = typer.Option(1_000_000.0, "--initial-cash-ars"),
        json_out: bool = typer.Option(False, "--json"),
    ):
        """Execute one daily live paper-trading step for event bots."""
        from datetime import date as _date
        from iol_cli.db import connect, init_db, resolve_db_path
        from iol_engines.simulation.event_runner import run_event_live_step

        target_date = as_of or _date.today().isoformat()
        all_event = ["event-defensive", "event-opportunistic", "event-adaptive"]
        bot_list = all_event if bots.strip().lower() == "all" else [b.strip() for b in bots.split(",")]

        db_path = resolve_db_path(ctx.obj.config.db_path)
        conn = connect(db_path)
        init_db(conn)

        run_ids = run_event_live_step(conn, bot_list, target_date,
                                      initial_cash_ars=initial_cash_ars, verbose=not json_out)

        if json_out:
            print_json({"date": target_date, "bots": bot_list, "run_ids": run_ids})
        elif run_ids:
            console.print(f"\n[green]Event live step complete.[/green] Run IDs: {', '.join(str(r) for r in run_ids)}")

    return app


# ── Pretty-print helpers ──────────────────────────────────────────────────────

def _print_run_summary(r: dict) -> None:
    ret = r.get("total_return_pct")
    ret_color = "green" if (ret or 0) >= 0 else "red"
    console.rule(f"[bold]Simulation Run #{r['id']} - {r['bot_name']}[/bold]")
    console.print(f"  Period:      {r['date_from']} to {r['date_to']}")
    console.print(f"  Status:      {r['status']}")
    console.print(f"  Initial ARS: {r.get('initial_value_ars', 0):,.0f}")
    console.print(f"  Final ARS:   {r.get('final_value_ars', 0):,.0f}")
    if ret is not None:
        console.print(f"  Return:      [{ret_color}]{ret:+.1f}%[/{ret_color}]")
    if r.get("sharpe_ratio") is not None:
        console.print(f"  Sharpe:      {r['sharpe_ratio']:.3f}")
    if r.get("max_drawdown_pct") is not None:
        console.print(f"  Max DD:      {r['max_drawdown_pct']:.1f}%")
    m = r.get("metrics") or {}
    if m.get("win_rate_pct") is not None:
        console.print(f"  Win rate:    {m['win_rate_pct']:.1f}%")
    if r.get("error_message"):
        console.print(f"  [red]Error: {r['error_message']}[/red]")


def _print_trades_table(trades: list) -> None:
    from rich.table import Table

    table = Table(title=f"Paper Trades ({len(trades)})", show_lines=False)
    table.add_column("Date", width=12)
    table.add_column("Symbol", style="bold", width=8)
    table.add_column("Action", width=6)
    table.add_column("ARS", justify="right", width=12)
    table.add_column("Price", justify="right", width=10)
    table.add_column("Portfolio After", justify="right", width=16)

    _ACTION_COLORS = {"buy": "green", "trim": "yellow", "exit": "red"}
    for t in trades:
        color = _ACTION_COLORS.get(t.get("action", ""), "white")
        table.add_row(
            t.get("trade_date", ""),
            t.get("symbol", ""),
            f"[{color}]{t.get('action', '')}[/{color}]",
            f"{t.get('amount_ars', 0):,.0f}",
            f"{t.get('price', 0):,.2f}",
            f"{t.get('portfolio_value_after', 0):,.0f}",
        )

    console.print(table)


# ── Swing helpers ─────────────────────────────────────────────────────────────

def _load_swing_run(conn, run_id: int) -> Optional[dict]:
    row = conn.execute(
        """
        SELECT id, bot_name, date_from, date_to, initial_cash, final_value,
               total_return_pct, sharpe_ratio, max_drawdown_pct, win_rate_pct,
               avg_hold_days, total_trades, mode, created_at
        FROM swing_simulation_runs WHERE id = ?
        """,
        (run_id,),
    ).fetchone()
    if not row:
        return None
    cols = ["id", "bot_name", "date_from", "date_to", "initial_cash", "final_value",
            "total_return_pct", "sharpe_ratio", "max_drawdown_pct", "win_rate_pct",
            "avg_hold_days", "total_trades", "mode", "created_at"]
    return dict(zip(cols, row))


def _print_swing_run_summary(r: dict) -> None:
    ret = r.get("total_return_pct")
    ret_color = "green" if (ret or 0) >= 0 else "red"
    console.rule(f"[bold]Swing Run #{r['id']} - {r['bot_name']}[/bold]")
    console.print(f"  Period:      {r['date_from']} to {r['date_to']}")
    console.print(f"  Mode:        {r.get('mode', 'backtest')}")
    console.print(f"  Initial ARS: {r.get('initial_cash', 0):,.0f}")
    console.print(f"  Final ARS:   {r.get('final_value', 0) or 0:,.0f}")
    if ret is not None:
        console.print(f"  Return:      [{ret_color}]{ret:+.1f}%[/{ret_color}]")
    if r.get("sharpe_ratio") is not None:
        console.print(f"  Sharpe:      {r['sharpe_ratio']:.3f}")
    if r.get("max_drawdown_pct") is not None:
        console.print(f"  Max DD:      {r['max_drawdown_pct']:.1f}%")
    if r.get("win_rate_pct") is not None:
        console.print(f"  Win rate:    {r['win_rate_pct']:.1f}%")
    if r.get("avg_hold_days") is not None:
        console.print(f"  Avg hold:    {r['avg_hold_days']:.1f} days")
    if r.get("total_trades") is not None:
        console.print(f"  Trades:      {r['total_trades']}")


def _print_swing_trades_table(trades: list) -> None:
    from rich.table import Table

    table = Table(title=f"Swing Trades ({len(trades)})", show_lines=False)
    table.add_column("Symbol", style="bold", width=8)
    table.add_column("Entry", width=12)
    table.add_column("Exit", width=12)
    table.add_column("Hold", justify="right", width=6)
    table.add_column("Entry $", justify="right", width=11)
    table.add_column("Exit $", justify="right", width=11)
    table.add_column("ARS", justify="right", width=12)
    table.add_column("P&L ARS", justify="right", width=12)
    table.add_column("Return%", justify="right", width=9)
    table.add_column("Exit Reason", width=16)

    for t in trades:
        pnl = t[6]
        pnl_color = "green" if (pnl or 0) >= 0 else "red"
        ret = t[7]
        ret_str = f"[{pnl_color}]{ret:+.1f}%[/{pnl_color}]" if ret is not None else "-"
        pnl_str = f"[{pnl_color}]{pnl:+,.0f}[/{pnl_color}]" if pnl is not None else "-"
        table.add_row(
            t[0],
            t[1] or "-",
            t[2] or "open",
            f"{t[8]}d" if t[8] else "-",
            f"{t[3]:,.2f}" if t[3] else "-",
            f"{t[4]:,.2f}" if t[4] else "-",
            f"{t[5]:,.0f}" if t[5] else "-",
            pnl_str,
            ret_str,
            t[9] or "-",
        )
    console.print(table)


# ── Event helpers ─────────────────────────────────────────────────────────────

def _load_event_run(conn, run_id: int) -> Optional[dict]:
    row = conn.execute(
        """
        SELECT id, bot_name, date_from, date_to, initial_cash, final_value,
               total_return_pct, sharpe_ratio, max_drawdown_pct, win_rate_pct,
               total_events_triggered, total_trades, mode, created_at
        FROM event_simulation_runs WHERE id = ?
        """,
        (run_id,),
    ).fetchone()
    if not row:
        return None
    cols = ["id", "bot_name", "date_from", "date_to", "initial_cash", "final_value",
            "total_return_pct", "sharpe_ratio", "max_drawdown_pct", "win_rate_pct",
            "total_events_triggered", "total_trades", "mode", "created_at"]
    return dict(zip(cols, row))


def _print_event_run_summary(r: dict) -> None:
    ret = r.get("total_return_pct")
    ret_color = "green" if (ret or 0) >= 0 else "red"
    console.rule(f"[bold]Event Run #{r['id']} - {r['bot_name']}[/bold]")
    console.print(f"  Period:      {r['date_from']} to {r['date_to']}")
    console.print(f"  Mode:        {r.get('mode', 'backtest')}")
    console.print(f"  Initial ARS: {r.get('initial_cash', 0):,.0f}")
    console.print(f"  Final ARS:   {r.get('final_value', 0) or 0:,.0f}")
    if ret is not None:
        console.print(f"  Return:      [{ret_color}]{ret:+.1f}%[/{ret_color}]")
    if r.get("sharpe_ratio") is not None:
        console.print(f"  Sharpe:      {r['sharpe_ratio']:.3f}")
    if r.get("max_drawdown_pct") is not None:
        console.print(f"  Max DD:      {r['max_drawdown_pct']:.1f}%")
    if r.get("win_rate_pct") is not None:
        console.print(f"  Win rate:    {r['win_rate_pct']:.1f}%")
    if r.get("total_events_triggered") is not None:
        console.print(f"  Events triggered: {r['total_events_triggered']}")
    if r.get("total_trades") is not None:
        console.print(f"  Trades:      {r['total_trades']}")


def _print_event_trades_table(trades: list) -> None:
    from rich.table import Table

    table = Table(title=f"Event Trades ({len(trades)})", show_lines=False)
    table.add_column("Symbol", style="bold", width=8)
    table.add_column("Date", width=12)
    table.add_column("Action", width=6)
    table.add_column("ARS", justify="right", width=12)
    table.add_column("Price", justify="right", width=11)
    table.add_column("P&L ARS", justify="right", width=12)
    table.add_column("Event", width=28)
    table.add_column("Portfolio After", justify="right", width=16)

    _ACTION_COLORS = {"buy": "green", "trim": "yellow", "exit": "red"}
    for t in trades:
        color = _ACTION_COLORS.get(t[2], "white")
        pnl = t[5]
        pnl_color = "green" if (pnl or 0) >= 0 else "red"
        pnl_str = f"[{pnl_color}]{pnl:+,.0f}[/{pnl_color}]" if pnl is not None else "-"
        table.add_row(
            t[0],
            t[1],
            f"[{color}]{t[2]}[/{color}]",
            f"{t[3]:,.0f}" if t[3] else "-",
            f"{t[4]:,.2f}" if t[4] else "-",
            pnl_str,
            t[6] or "-",
            f"{t[7]:,.0f}" if t[7] else "-",
        )
    console.print(table)
