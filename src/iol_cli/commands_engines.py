"""CLI commands for the multi-engine financial advisor.

Sub-app tree registered in cli.py:
  iol engines regime run / show
  iol engines macro run / show
  iol engines smart-money run / show
  iol engines run-all
  iol engines strategy show
"""
from __future__ import annotations

import json
from datetime import date
from typing import Any, Callable, Optional

import typer
from rich.console import Console

console = Console()


def _utc_today() -> str:
    return date.today().isoformat()


def build_engines_app(
    *,
    print_json: Callable[[Any], None],
) -> typer.Typer:
    """Build and return the ``engines`` sub-application."""
    engines_app = typer.Typer(help="Financial analysis engines (regime, macro, smart-money, strategy)")
    regime_app = typer.Typer(help="Market regime detection engine")
    macro_app = typer.Typer(help="Macro momentum engine (Argentina + global)")
    sm_app = typer.Typer(help="Smart money engine (institutional 13F tracking)")
    engines_app.add_typer(regime_app, name="regime")
    engines_app.add_typer(macro_app, name="macro")
    engines_app.add_typer(sm_app, name="smart-money")

    # ── run-all ─────────────────────────────────────────────────────────────

    @engines_app.command("run-all")
    def run_all(
        ctx: typer.Context,
        as_of: Optional[str] = typer.Option(None, "--as-of", help="Date YYYY-MM-DD (default: today)"),
        budget_ars: Optional[float] = typer.Option(None, "--budget-ars", help="Override available ARS budget"),
        skip_smart_money: bool = typer.Option(False, "--skip-smart-money", help="Skip 13F fetch (faster)"),
        skip_external: bool = typer.Option(False, "--skip-external", help="Offline mode — no HTTP calls"),
        force_regime: bool = typer.Option(False, "--force-regime", help="Re-run regime even if cached"),
        force_macro: bool = typer.Option(False, "--force-macro", help="Re-run macro even if cached"),
        json_out: bool = typer.Option(False, "--json", help="Output raw JSON"),
    ):
        """Run all 5 engines in sequence and display the full action plan."""
        from iol_cli.db import connect, init_db, resolve_db_path
        from iol_engines.registry import run_full_engine_pipeline

        target_date = as_of or _utc_today()
        db_path = resolve_db_path(ctx.obj.config.db_path)
        conn = connect(db_path)
        init_db(conn)

        console.rule(f"[bold]Full Engine Pipeline[/bold] — {target_date}")
        try:
            result = run_full_engine_pipeline(
                target_date,
                conn,
                budget_ars=budget_ars,
                force_regime=force_regime,
                force_macro=force_macro,
                skip_smart_money=skip_smart_money,
                skip_external=skip_external,
                verbose=not json_out,
            )
        except Exception as exc:
            console.print(f"[red]Pipeline error:[/red] {exc}")
            raise typer.Exit(code=1)

        if json_out:
            plan = result["strategy"]
            print_json({
                "as_of": result["as_of"],
                "regime": result["regime"].to_dict(),
                "macro": result["macro"].to_dict(),
                "smart_money": [s.to_dict() for s in result["smart_money"]],
                "strategy": plan.to_dict(),
            })
        else:
            _print_strategy_plan(result["strategy"])

    # ── strategy show ────────────────────────────────────────────────────────

    @engines_app.command("strategy-show")
    def strategy_show(
        ctx: typer.Context,
        as_of: Optional[str] = typer.Option(None, "--as-of", help="Date YYYY-MM-DD (default: today)"),
        json_out: bool = typer.Option(False, "--json"),
    ):
        """Show the latest cached strategy action plan."""
        from iol_cli.db import connect, init_db, resolve_db_path
        from iol_engines.strategy.engine import PortfolioStrategyEngine

        target_date = as_of or _utc_today()
        db_path = resolve_db_path(ctx.obj.config.db_path)
        conn = connect(db_path)
        init_db(conn)

        plan = PortfolioStrategyEngine().load_latest(conn, target_date)
        if plan is None:
            console.print("[yellow]No strategy plan found. Run:[/yellow] iol engines run-all")
            raise typer.Exit(code=0)

        if json_out:
            print_json(plan.to_dict())
        else:
            _print_strategy_plan(plan)

    # ── regime run ──────────────────────────────────────────────────────────

    @regime_app.command("run")
    def regime_run(
        ctx: typer.Context,
        as_of: Optional[str] = typer.Option(
            None,
            "--as-of",
            help="Date YYYY-MM-DD (default: today)",
        ),
        fetch_vix: bool = typer.Option(
            False,
            "--fetch-vix",
            help="Fetch VIX from FRED API (requires internet)",
        ),
        force_refresh: bool = typer.Option(
            False,
            "--force-refresh",
            help="Re-compute even if a cached signal exists for this date",
        ),
    ):
        """Run the Market Regime Engine and store the result."""
        from iol_cli.db import connect, init_db, resolve_db_path
        from iol_engines.regime.engine import MarketRegimeEngine

        target_date = as_of or _utc_today()
        db_path = resolve_db_path(ctx.obj.config.db_path)
        conn = connect(db_path)
        init_db(conn)

        console.print(f"[bold]Running Market Regime Engine[/bold] as_of={target_date} fetch_vix={fetch_vix}")
        engine = MarketRegimeEngine()
        try:
            sig = engine.run(
                target_date,
                conn,
                fetch_vix=fetch_vix,
                force_refresh=force_refresh,
            )
        except Exception as exc:
            console.print(f"[red]Engine error:[/red] {exc}")
            raise typer.Exit(code=1)

        _print_regime(sig, print_json)

    # ── regime show ─────────────────────────────────────────────────────────

    @regime_app.command("show")
    def regime_show(
        ctx: typer.Context,
        as_of: Optional[str] = typer.Option(
            None,
            "--as-of",
            help="Date YYYY-MM-DD (default: today)",
        ),
        json_out: bool = typer.Option(
            False,
            "--json",
            help="Output raw JSON",
        ),
    ):
        """Show the latest cached regime signal."""
        from iol_cli.db import connect, init_db, resolve_db_path
        from iol_engines.regime.engine import MarketRegimeEngine

        target_date = as_of or _utc_today()
        db_path = resolve_db_path(ctx.obj.config.db_path)
        conn = connect(db_path)
        init_db(conn)

        engine = MarketRegimeEngine()
        sig = engine.load_latest(conn, target_date)

        if sig is None:
            console.print("[yellow]No regime signal found. Run:[/yellow] iol engines regime run")
            raise typer.Exit(code=0)

        if json_out:
            print_json(sig.to_dict())
        else:
            _print_regime(sig, print_json)

    # ── macro run ───────────────────────────────────────────────────────────

    @macro_app.command("run")
    def macro_run(
        ctx: typer.Context,
        as_of: Optional[str] = typer.Option(None, "--as-of", help="Date YYYY-MM-DD (default: today)"),
        skip_external: bool = typer.Option(
            False, "--skip-external", help="Skip all external HTTP calls (offline mode)"
        ),
        force_refresh: bool = typer.Option(
            False, "--force-refresh", help="Re-compute even if cached signal exists"
        ),
    ):
        """Run the Macro Momentum Engine and store the result."""
        from iol_cli.db import connect, init_db, resolve_db_path
        from iol_engines.macro.engine import MacroMomentumEngine

        target_date = as_of or _utc_today()
        db_path = resolve_db_path(ctx.obj.config.db_path)
        conn = connect(db_path)
        init_db(conn)

        console.print(f"[bold]Running Macro Momentum Engine[/bold] as_of={target_date}")
        engine = MacroMomentumEngine()
        try:
            sig = engine.run(target_date, conn, force_refresh=force_refresh, skip_external=skip_external)
        except Exception as exc:
            console.print(f"[red]Engine error:[/red] {exc}")
            raise typer.Exit(code=1)

        _print_macro(sig, print_json)

    # ── macro show ──────────────────────────────────────────────────────────

    @macro_app.command("show")
    def macro_show(
        ctx: typer.Context,
        as_of: Optional[str] = typer.Option(None, "--as-of", help="Date YYYY-MM-DD (default: today)"),
        json_out: bool = typer.Option(False, "--json", help="Output raw JSON"),
    ):
        """Show the latest cached macro signal."""
        from iol_cli.db import connect, init_db, resolve_db_path
        from iol_engines.macro.engine import MacroMomentumEngine

        target_date = as_of or _utc_today()
        db_path = resolve_db_path(ctx.obj.config.db_path)
        conn = connect(db_path)
        init_db(conn)

        engine = MacroMomentumEngine()
        sig = engine.load_latest(conn, target_date)
        if sig is None:
            console.print("[yellow]No macro signal found. Run:[/yellow] iol engines macro run")
            raise typer.Exit(code=0)

        if json_out:
            print_json(sig.to_dict())
        else:
            _print_macro(sig, print_json)

    # ── smart-money run ─────────────────────────────────────────────────────

    @sm_app.command("run")
    def sm_run(
        ctx: typer.Context,
        as_of: Optional[str] = typer.Option(None, "--as-of", help="Date YYYY-MM-DD (default: today)"),
        symbols: Optional[str] = typer.Option(
            None, "--symbols",
            help="Comma-separated symbols to track (default: full CEDEAR universe)"
        ),
        force_refresh: bool = typer.Option(False, "--force-refresh"),
    ):
        """Fetch 13F filings and store institutional conviction scores."""
        from iol_cli.db import connect, init_db, resolve_db_path
        from iol_engines.smart_money.engine import SmartMoneyEngine

        target_date = as_of or _utc_today()
        sym_list = [s.strip().upper() for s in symbols.split(",")] if symbols else None

        db_path = resolve_db_path(ctx.obj.config.db_path)
        conn = connect(db_path)
        init_db(conn)

        console.print(f"[bold]Running Smart Money Engine[/bold] as_of={target_date}")
        console.print("Fetching 13F filings from SEC EDGAR (may take 30-60s)...")
        engine = SmartMoneyEngine()
        try:
            signals = engine.run(target_date, conn, symbols=sym_list, force_refresh=force_refresh)
        except Exception as exc:
            console.print(f"[red]Engine error:[/red] {exc}")
            raise typer.Exit(code=1)

        if not signals:
            console.print("[yellow]No signals produced — check SEC connectivity.[/yellow]")
        else:
            _print_smart_money_table(signals)

    # ── smart-money show ────────────────────────────────────────────────────

    @sm_app.command("show")
    def sm_show(
        ctx: typer.Context,
        as_of: Optional[str] = typer.Option(None, "--as-of", help="Date YYYY-MM-DD (default: today)"),
        symbol: Optional[str] = typer.Option(None, "--symbol", help="Show single symbol"),
        min_conviction: float = typer.Option(0, "--min-conviction", help="Filter by min conviction (0-100)"),
        json_out: bool = typer.Option(False, "--json"),
    ):
        """Show cached institutional conviction signals."""
        from iol_cli.db import connect, init_db, resolve_db_path
        from iol_engines.smart_money.engine import SmartMoneyEngine

        target_date = as_of or _utc_today()
        db_path = resolve_db_path(ctx.obj.config.db_path)
        conn = connect(db_path)
        init_db(conn)

        engine = SmartMoneyEngine()
        if symbol:
            sig = engine.load_latest(conn, target_date, symbol=symbol.upper())
            if sig is None:
                console.print(f"[yellow]No signal for {symbol}. Run:[/yellow] iol engines smart-money run")
                raise typer.Exit(code=0)
            if json_out:
                print_json(sig.to_dict())
            else:
                _print_smart_money_table([sig])
        else:
            signals = engine.load_latest(conn, target_date) or []
            signals = [s for s in signals if s.conviction_score >= min_conviction]
            if not signals:
                console.print("[yellow]No smart money signals found. Run:[/yellow] iol engines smart-money run")
                raise typer.Exit(code=0)
            if json_out:
                print_json([s.to_dict() for s in signals])
            else:
                _print_smart_money_table(signals)

    # ── accuracy ─────────────────────────────────────────────────────────────

    @engines_app.command("accuracy")
    def accuracy(
        ctx: typer.Context,
        days: int = typer.Option(90, "--days", help="Look-back window in days"),
        engine_filter: Optional[str] = typer.Option(
            None, "--engine",
            help="Filter: regime | macro | smart_money | strategy",
        ),
        update: bool = typer.Option(
            True, "--update/--no-update",
            help="Compute new outcomes before reporting (default: on)",
        ),
        lookahead: int = typer.Option(20, "--lookahead", help="Lookahead window in calendar days"),
        json_out: bool = typer.Option(False, "--json"),
    ):
        """Show signal accuracy for each engine based on observed market outcomes."""
        from iol_cli.db import connect, init_db, resolve_db_path
        from iol_engines.analysis.accuracy import compute_signal_outcomes, get_accuracy_report

        db_path = resolve_db_path(ctx.obj.config.db_path)
        conn = connect(db_path)
        init_db(conn)

        if update:
            counts = compute_signal_outcomes(conn, lookahead_days=lookahead)
            if not json_out:
                new_total = sum(counts.values())
                if new_total:
                    console.print(
                        "[dim]Computed " + str(new_total) + " new outcome(s): "
                        + ", ".join(f"{k}={v}" for k, v in counts.items() if v)
                        + "[/dim]"
                    )

        report = get_accuracy_report(conn, days=days, engine=engine_filter)

        if json_out:
            print_json({"window_days": days, "engines": report})
            return

        from rich.table import Table

        console.rule(f"[bold]Engine Signal Accuracy — last {days} days[/bold]")
        table = Table(show_lines=False)
        table.add_column("Motor", width=14)
        table.add_column("Hit Rate", justify="right", width=10)
        table.add_column("Hits", justify="right", width=6)
        table.add_column("Evaluated", justify="right", width=10)
        table.add_column("Pending", justify="right", width=8)
        table.add_column("Total", justify="right", width=6)
        table.add_column("Última eval.", width=14)

        for r in report:
            hr = r["hit_rate_pct"]
            if hr is None:
                hr_str = "[dim]n/a[/dim]"
            else:
                color = "green" if hr >= 65 else ("yellow" if hr >= 50 else "red")
                hr_str = f"[{color}]{hr:.1f}%[/{color}]"

            table.add_row(
                r["engine"],
                hr_str,
                str(r["hits"]),
                str(r["evaluated"]),
                str(r["pending"]),
                str(r["total"]),
                r["last_eval_date"] or "—",
            )

        console.print(table)
        console.print(
            "\n[dim]Verde ≥65% | Amarillo 50-65% | Rojo <50%  "
            "— Pending: señales sin suficiente historia de retornos aún.[/dim]"
        )

    return engines_app


# ── Pretty-print helpers ─────────────────────────────────────────────────────

_REGIME_COLORS = {
    "bull": "green",
    "sideways": "yellow",
    "bear": "red",
    "crisis": "bold red",
}

_VOL_COLORS = {
    "low": "green",
    "normal": "white",
    "high": "yellow",
    "extreme": "red",
}


def _fmt_opt(value: Optional[float], fmt: str = ".2f", suffix: str = "") -> str:
    if value is None:
        return "[dim]n/a[/dim]"
    return f"{value:{fmt}}{suffix}"


def _stress_color(score: float) -> str:
    if score < 30:
        return "green"
    if score < 60:
        return "yellow"
    return "red"


def _print_macro(sig: Any, print_json: Callable) -> None:
    ar_color = _stress_color(sig.argentina_macro_stress)
    # global_risk_on: high = good = green
    global_color = "green" if sig.global_risk_on >= 60 else ("yellow" if sig.global_risk_on >= 40 else "red")

    console.rule(f"[bold]Macro Momentum — {sig.as_of}[/bold]")
    console.print(
        f"  AR Stress:      [{ar_color}]{sig.argentina_macro_stress:.1f}/100[/{ar_color}]  "
        "(higher = worse for local equities)"
    )
    console.print(
        f"  Global Risk-On: [{global_color}]{sig.global_risk_on:.1f}/100[/{global_color}]  "
        "(higher = more risk appetite)"
    )
    console.print(f"  BCRA Rate:      {_fmt_opt(sig.bcra_rate_pct, '.1f', '% TNA')}")
    console.print(f"  USD/ARS (ofic): {_fmt_opt(sig.usd_ars_official, '.2f')}")
    console.print(f"  Fed Rate:       {_fmt_opt(sig.fed_rate_pct, '.2f', '%')}")
    console.print(f"  US CPI YoY:     {_fmt_opt(sig.us_cpi_yoy_pct, '.2f', '%')}")
    if sig.sentiment_score is not None:
        sent_color = "green" if sig.sentiment_score > 10 else ("red" if sig.sentiment_score < -10 else "yellow")
        console.print(f"  Sentiment:      [{sent_color}]{sig.sentiment_score:+.1f}[/{sent_color}]  (-100 bearish ↔ +100 bullish)")


_DIR_COLORS = {
    "accumulate": "green",
    "distribute": "red",
    "neutral": "white",
}
_DIR_ICONS = {
    "accumulate": "▲",
    "distribute": "▼",
    "neutral": "─",
}


def _print_smart_money_table(signals: list) -> None:
    from rich.table import Table

    signals_sorted = sorted(signals, key=lambda s: s.conviction_score, reverse=True)
    table = Table(title="Smart Money — Institutional 13F Signals", show_lines=False)
    table.add_column("Symbol", style="bold", width=8)
    table.add_column("Direction", width=12)
    table.add_column("Conviction", justify="right", width=11)
    table.add_column("Added by", width=30)
    table.add_column("Trimmed by", width=30)
    table.add_column("13F Date", width=12)

    for sig in signals_sorted:
        color = _DIR_COLORS.get(sig.net_institutional_direction, "white")
        icon = _DIR_ICONS.get(sig.net_institutional_direction, "─")
        table.add_row(
            sig.symbol,
            f"[{color}]{icon} {sig.net_institutional_direction}[/{color}]",
            f"{sig.conviction_score:.0f}/100",
            ", ".join(sig.top_holders_added) or "—",
            ", ".join(sig.top_holders_trimmed) or "—",
            sig.latest_13f_date or "—",
        )

    console.print(table)


def _print_regime(sig: Any, print_json: Callable) -> None:
    regime_color = _REGIME_COLORS.get(sig.regime, "white")
    vol_color = _VOL_COLORS.get(sig.volatility_regime, "white")

    console.rule(f"[bold]Market Regime — {sig.as_of}[/bold]")
    console.print(
        f"  Regime:        [{regime_color}]{sig.regime.upper()}[/{regime_color}]  "
        f"(score {sig.regime_score:.1f}/100, confidence {sig.confidence:.0%})"
    )
    console.print(f"  Breadth:       {sig.breadth_score:.1f}% symbols above MA50")
    console.print(
        f"  Volatility:    [{vol_color}]{sig.volatility_regime}[/{vol_color}]"
    )
    console.print(f"  Favoured:      {', '.join(sig.favored_asset_classes)}")
    console.print(
        f"  Equity adj.:   {sig.defensive_weight_adjustment:+.0%} "
        "(negative = reduce equity exposure)"
    )
    if sig.notes:
        console.print(f"  Notes:         {sig.notes}")


def _print_strategy_plan(plan: Any) -> None:
    from rich.table import Table

    console.rule(f"[bold]Strategy Action Plan — {plan.as_of}[/bold]")
    regime_color = _REGIME_COLORS.get(plan.regime, "white")
    console.print(
        f"  Regime:        [{regime_color}]{plan.regime.upper()}[/{regime_color}]"
    )
    console.print(f"  Cash ARS:      {plan.portfolio_cash_ars:,.0f}")
    console.print(f"  Deployed ARS:  {plan.total_deployed_ars:,.0f}")
    console.print(
        f"  Defensive:     {'[yellow]yes[/yellow]' if plan.defensive_overlay_applied else '[green]no[/green]'}"
    )
    if plan.notes:
        console.print(f"  Notes:         {plan.notes}")

    if not plan.actions:
        console.print("\n[yellow]No actions in plan. Run:[/yellow] iol advisor opportunities run --mode both")
        return

    table = Table(title=f"\nActions ({len(plan.actions)})", show_lines=False)
    table.add_column("Symbol", style="bold", width=8)
    table.add_column("Action", width=8)
    table.add_column("ARS", justify="right", width=14)
    table.add_column("Weight%", justify="right", width=8)
    table.add_column("Score", justify="right", width=7)
    table.add_column("Reason", no_wrap=False)

    _ACTION_COLORS = {"buy": "green", "trim": "yellow", "exit": "red"}
    for a in plan.actions:
        color = _ACTION_COLORS.get(a.action, "white")
        table.add_row(
            a.symbol,
            f"[{color}]{a.action}[/{color}]",
            f"{a.amount_ars:,.0f}",
            f"{a.weight_pct:.1f}%",
            f"{a.candidate_score:.0f}",
            a.reason[:60] + "…" if len(a.reason) > 60 else a.reason,
        )

    console.print(table)
