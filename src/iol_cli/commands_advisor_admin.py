from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Optional

import typer
from rich.console import Console

from iol_advisor.service import build_unified_context, load_briefing_history_payload, load_latest_briefing_payload

from .advisor_context import render_advisor_context_md
from .db import connect, init_db, resolve_db_path


console = Console()

ALERT_SEVERITIES = {"low", "medium", "high"}
ALERT_STATUSES = {"open", "closed", "all"}
EVENT_TYPES = {"note", "macro", "portfolio", "order", "other"}


def _print_json(data: Any) -> None:
    try:
        text = json.dumps(data, ensure_ascii=True, indent=2)
        console.print_json(text)
    except Exception:
        console.print(data)


def _parse_iso_date_optional(value: Optional[str], label: str) -> Optional[str]:
    if value is None:
        return None
    v = value.strip()
    if not v:
        return None
    try:
        datetime.fromisoformat(v if len(v) > 10 else f"{v}T00:00:00")
    except Exception as exc:
        raise typer.BadParameter(f"{label} must be YYYY-MM-DD") from exc
    return v[:10]


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _maybe_read_stdin(value: str) -> str:
    if value != "-":
        return value
    text = console.input()
    return text.rstrip("\n")


def _normalize_enum(value: str, label: str, allowed: set) -> str:
    normalized = (value or "").strip().lower()
    if normalized not in allowed:
        allowed_txt = "|".join(sorted(allowed))
        raise typer.BadParameter(f"{label} must be one of {allowed_txt}")
    return normalized


def _latest_snapshot_date(conn) -> Optional[str]:
    row = conn.execute("SELECT snapshot_date FROM portfolio_snapshots ORDER BY snapshot_date DESC LIMIT 1").fetchone()
    return str(row["snapshot_date"]) if row and row["snapshot_date"] else None


def register_advisor_admin_commands(
    advisor_app: typer.Typer,
    advisor_alert_app: typer.Typer,
    advisor_event_app: typer.Typer,
    advisor_briefing_app: typer.Typer,
) -> None:
    @advisor_app.command("context")
    def advisor_context(
        ctx: typer.Context,
        as_of: Optional[str] = typer.Option(None, "--as-of", help="Optional YYYY-MM-DD; uses snapshot on-or-before date"),
        limit: int = typer.Option(10, "--limit", min=1, max=100, help="Top N for movers/tables"),
        history_days: int = typer.Option(365, "--history-days", min=1, max=3650, help="Days of history to include"),
        include_cash: bool = typer.Option(True, "--include-cash/--no-include-cash", help="Include ARS cash in allocation"),
        include_orders: bool = typer.Option(False, "--include-orders", help="Include recent orders (from local DB)"),
        orders_limit: int = typer.Option(20, "--orders-limit", min=1, max=500, help="Max orders to include when enabled"),
        out: Optional[str] = typer.Option(None, "--out", help="Write output to file instead of stdout"),
        fmt: str = typer.Option("json", "--format", help="json|md"),
    ):
        db_path = resolve_db_path(ctx.obj.config.db_path)
        payload = build_unified_context(
            db_path=db_path,
            as_of=as_of,
            limit=int(limit),
            history_days=int(history_days),
            include_cash=bool(include_cash),
            include_orders=bool(include_orders),
            orders_limit=int(orders_limit),
        )
        fmt_norm = (fmt or "").strip().lower()
        if fmt_norm not in ("json", "md"):
            raise typer.BadParameter("--format must be json or md")
        if fmt_norm == "json":
            if out:
                with open(out, "w", encoding="utf-8") as fh:
                    fh.write(json.dumps(payload, ensure_ascii=True, indent=2) + "\n")
                _print_json({"out": out})
                return
            _print_json(payload)
            return
        text = render_advisor_context_md(payload)
        if out:
            with open(out, "w", encoding="utf-8") as fh:
                fh.write(text)
            _print_json({"out": out})
            return
        console.print(text)

    @advisor_app.command("log")
    def advisor_log(
        ctx: typer.Context,
        prompt: str = typer.Option(..., "--prompt", help="Original user prompt (or '-' to read stdin)"),
        response: str = typer.Option(..., "--response", help="Assistant response (or '-' to read stdin)"),
        snapshot_date: Optional[str] = typer.Option(None, "--snapshot-date", help="Optional YYYY-MM-DD context"),
    ):
        db_path = resolve_db_path(ctx.obj.config.db_path)
        conn = connect(db_path)
        init_db(conn)
        try:
            created_at = datetime.utcnow().isoformat(timespec="seconds") + "Z"
            resolved_snapshot = snapshot_date or _latest_snapshot_date(conn)
            cur = conn.execute(
                """
                INSERT INTO advisor_logs (created_at, snapshot_date, prompt, response, env, base_url)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    created_at,
                    resolved_snapshot,
                    _maybe_read_stdin(prompt),
                    _maybe_read_stdin(response),
                    ctx.obj.env,
                    ctx.obj.base_url,
                ),
            )
            conn.commit()
            _print_json({"id": cur.lastrowid, "created_at": created_at, "snapshot_date": resolved_snapshot})
        finally:
            conn.close()

    @advisor_app.command("list")
    def advisor_list(
        ctx: typer.Context,
        limit: int = typer.Option(20, "--limit", min=1, max=200),
    ):
        db_path = resolve_db_path(ctx.obj.config.db_path)
        conn = connect(db_path)
        init_db(conn)
        try:
            rows = conn.execute(
                """
                SELECT id, created_at, snapshot_date, env, base_url, prompt, response
                FROM advisor_logs
                ORDER BY id DESC
                LIMIT ?
                """,
                (int(limit),),
            ).fetchall()
            _print_json([dict(row) for row in rows])
        finally:
            conn.close()

    @advisor_alert_app.command("create")
    def advisor_alert_create(
        ctx: typer.Context,
        alert_type: str = typer.Option(..., "--type", help="Alert type"),
        title: str = typer.Option(..., "--title", help="Short alert title"),
        description: str = typer.Option(..., "--description", help="Alert detail"),
        severity: str = typer.Option("medium", "--severity", help="low|medium|high"),
        symbol: Optional[str] = typer.Option(None, "--symbol"),
        snapshot_date: Optional[str] = typer.Option(None, "--snapshot-date", help="Optional YYYY-MM-DD"),
        due_date: Optional[str] = typer.Option(None, "--due-date", help="Optional YYYY-MM-DD"),
    ):
        sev = _normalize_enum(severity, "--severity", ALERT_SEVERITIES)
        snap = _parse_iso_date_optional(snapshot_date, "--snapshot-date")
        due = _parse_iso_date_optional(due_date, "--due-date")
        alert_type_v = alert_type.strip()
        title_v = title.strip()
        description_v = description.strip()
        symbol_v = symbol.strip() if symbol and symbol.strip() else None
        if not alert_type_v or not title_v or not description_v:
            raise typer.BadParameter("--type, --title and --description are required")
        now = _utc_now_iso()
        db_path = resolve_db_path(ctx.obj.config.db_path)
        conn = connect(db_path)
        init_db(conn)
        try:
            cur = conn.execute(
                """
                INSERT INTO advisor_alerts (
                    created_at, updated_at, status, severity, alert_type, title, description, symbol, snapshot_date, due_date
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (now, now, "open", sev, alert_type_v, title_v, description_v, symbol_v, snap, due),
            )
            conn.commit()
            _print_json({"id": cur.lastrowid, "status": "open", "severity": sev, "type": alert_type_v, "symbol": symbol_v, "snapshot_date": snap, "due_date": due, "created_at": now})
        finally:
            conn.close()

    @advisor_alert_app.command("list")
    def advisor_alert_list(
        ctx: typer.Context,
        status: str = typer.Option("open", "--status", help="open|closed|all"),
        severity: Optional[str] = typer.Option(None, "--severity", help="low|medium|high"),
        symbol: Optional[str] = typer.Option(None, "--symbol"),
        limit: int = typer.Option(50, "--limit", min=1, max=200),
    ):
        status_v = _normalize_enum(status, "--status", ALERT_STATUSES)
        severity_v = _normalize_enum(severity, "--severity", ALERT_SEVERITIES) if severity else None
        symbol_v = symbol.strip() if symbol and symbol.strip() else None
        where = ["1=1"]
        params: list[Any] = []
        if status_v != "all":
            where.append("status = ?")
            params.append(status_v)
        if severity_v:
            where.append("severity = ?")
            params.append(severity_v)
        if symbol_v:
            where.append("symbol = ?")
            params.append(symbol_v)
        params.append(int(limit))
        db_path = resolve_db_path(ctx.obj.config.db_path)
        conn = connect(db_path)
        init_db(conn)
        try:
            rows = conn.execute(
                f"""
                SELECT id, created_at, updated_at, status, severity, alert_type, title, description, symbol, snapshot_date, due_date, closed_at, closed_reason
                FROM advisor_alerts
                WHERE {" AND ".join(where)}
                ORDER BY id DESC
                LIMIT ?
                """,
                tuple(params),
            ).fetchall()
            _print_json([dict(r) for r in rows])
        finally:
            conn.close()

    @advisor_alert_app.command("close")
    def advisor_alert_close(
        ctx: typer.Context,
        alert_id: int = typer.Option(..., "--id", min=1),
        reason: str = typer.Option(..., "--reason"),
    ):
        reason_v = reason.strip()
        if not reason_v:
            raise typer.BadParameter("--reason is required")
        now = _utc_now_iso()
        db_path = resolve_db_path(ctx.obj.config.db_path)
        conn = connect(db_path)
        init_db(conn)
        try:
            row = conn.execute("SELECT id FROM advisor_alerts WHERE id = ?", (int(alert_id),)).fetchone()
            if not row:
                console.print("Alert ID not found.")
                raise typer.Exit(code=1)
            conn.execute(
                """
                UPDATE advisor_alerts
                SET status = 'closed', updated_at = ?, closed_at = ?, closed_reason = ?
                WHERE id = ?
                """,
                (now, now, reason_v, int(alert_id)),
            )
            conn.commit()
            _print_json({"id": int(alert_id), "status": "closed", "closed_reason": reason_v, "closed_at": now})
        finally:
            conn.close()

    @advisor_event_app.command("add")
    def advisor_event_add(
        ctx: typer.Context,
        event_type: str = typer.Option(..., "--type", help="note|macro|portfolio|order|other"),
        title: str = typer.Option(..., "--title"),
        description: Optional[str] = typer.Option(None, "--description"),
        symbol: Optional[str] = typer.Option(None, "--symbol"),
        snapshot_date: Optional[str] = typer.Option(None, "--snapshot-date", help="Optional YYYY-MM-DD"),
        alert_id: Optional[int] = typer.Option(None, "--alert-id", min=1),
    ):
        event_type_v = _normalize_enum(event_type, "--type", EVENT_TYPES)
        title_v = title.strip()
        if not title_v:
            raise typer.BadParameter("--title is required")
        snap = _parse_iso_date_optional(snapshot_date, "--snapshot-date")
        symbol_v = symbol.strip() if symbol and symbol.strip() else None
        description_v = description.strip() if description and description.strip() else None
        now = _utc_now_iso()
        db_path = resolve_db_path(ctx.obj.config.db_path)
        conn = connect(db_path)
        init_db(conn)
        try:
            if alert_id is not None:
                linked = conn.execute("SELECT id FROM advisor_alerts WHERE id = ?", (int(alert_id),)).fetchone()
                if not linked:
                    console.print("Alert ID not found.")
                    raise typer.Exit(code=1)
            cur = conn.execute(
                """
                INSERT INTO advisor_events(created_at, event_type, title, description, symbol, snapshot_date, alert_id, payload_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (now, event_type_v, title_v, description_v, symbol_v, snap, alert_id, None),
            )
            conn.commit()
            _print_json({"id": cur.lastrowid, "type": event_type_v, "title": title_v, "symbol": symbol_v, "snapshot_date": snap, "alert_id": alert_id, "created_at": now})
        finally:
            conn.close()

    @advisor_event_app.command("list")
    def advisor_event_list(
        ctx: typer.Context,
        event_type: Optional[str] = typer.Option(None, "--type", help="note|macro|portfolio|order|other"),
        symbol: Optional[str] = typer.Option(None, "--symbol"),
        limit: int = typer.Option(50, "--limit", min=1, max=200),
    ):
        event_type_v = _normalize_enum(event_type, "--type", EVENT_TYPES) if event_type else None
        symbol_v = symbol.strip() if symbol and symbol.strip() else None
        where = ["1=1"]
        params: list[Any] = []
        if event_type_v:
            where.append("event_type = ?")
            params.append(event_type_v)
        if symbol_v:
            where.append("symbol = ?")
            params.append(symbol_v)
        params.append(int(limit))
        db_path = resolve_db_path(ctx.obj.config.db_path)
        conn = connect(db_path)
        init_db(conn)
        try:
            rows = conn.execute(
                f"""
                SELECT id, created_at, event_type, title, description, symbol, snapshot_date, alert_id, payload_json
                FROM advisor_events
                WHERE {" AND ".join(where)}
                ORDER BY id DESC
                LIMIT ?
                """,
                tuple(params),
            ).fetchall()
            _print_json([dict(r) for r in rows])
        finally:
            conn.close()

    @advisor_briefing_app.command("list")
    def advisor_briefing_list(
        ctx: typer.Context,
        cadence: Optional[str] = typer.Option(None, "--cadence", help="daily|weekly"),
        limit: int = typer.Option(20, "--limit", min=1, max=200),
    ):
        cadence_v = cadence.strip().lower() if cadence and cadence.strip() else None
        if cadence_v is not None and cadence_v not in ("daily", "weekly"):
            raise typer.BadParameter("--cadence must be daily|weekly")
        db_path = resolve_db_path(ctx.obj.config.db_path)
        _print_json(load_briefing_history_payload(db_path, cadence_v, int(limit)))

    @advisor_briefing_app.command("latest")
    def advisor_briefing_latest(
        ctx: typer.Context,
        cadence: str = typer.Option("daily", "--cadence", help="daily|weekly"),
    ):
        cadence_v = cadence.strip().lower()
        if cadence_v not in ("daily", "weekly"):
            raise typer.BadParameter("--cadence must be daily|weekly")
        db_path = resolve_db_path(ctx.obj.config.db_path)
        _print_json(load_latest_briefing_payload(db_path, cadence_v) or {})

    @advisor_app.command("seguimiento")
    def advisor_seguimiento(
        ctx: typer.Context,
        out: Optional[str] = typer.Option(None, "--out", help="Write markdown to file instead of stdout"),
        alerts_limit: int = typer.Option(20, "--alerts-limit", min=1, max=200),
    ):
        db_path = resolve_db_path(ctx.obj.config.db_path)
        conn = connect(db_path)
        init_db(conn)
        try:
            latest_log = conn.execute(
                """
                SELECT id, created_at, snapshot_date, env, base_url, prompt, response
                FROM advisor_logs
                ORDER BY id DESC
                LIMIT 1
                """
            ).fetchone()
            alerts = conn.execute(
                """
                SELECT id, severity, alert_type, title, description, symbol, snapshot_date, due_date, created_at
                FROM advisor_alerts
                WHERE status = 'open'
                ORDER BY CASE severity WHEN 'high' THEN 3 WHEN 'medium' THEN 2 ELSE 1 END DESC, due_date ASC, id DESC
                LIMIT ?
                """,
                (int(alerts_limit),),
            ).fetchall()
        finally:
            conn.close()

        now = _utc_now_iso()
        lines = [
            "# Memoria del Asesor (Ultima Conversacion)",
            "",
            "Este archivo es un resumen operativo.",
            "Fuente de verdad: SQLite (`advisor_logs`, `advisor_alerts`, `advisor_events`).",
            "",
            "## Metadata",
            f"- `generated_at_utc`: {now}",
            f"- `advisor_log_id`: {latest_log['id'] if latest_log else '-'}",
            f"- `context_snapshot_date`: {latest_log['snapshot_date'] if latest_log and latest_log['snapshot_date'] else '-'}",
            f"- `env`: {latest_log['env'] if latest_log and latest_log['env'] else '-'}",
            f"- `base_url`: {latest_log['base_url'] if latest_log and latest_log['base_url'] else '-'}",
            "",
            "## Resumen (5 lineas max)",
        ]
        if latest_log and latest_log["response"]:
            raw_lines = [str(x).strip() for x in str(latest_log["response"]).splitlines() if str(x).strip()]
            lines.extend([f"- {r}" for r in raw_lines[:5]])
        else:
            lines.append("- Sin registro reciente en `advisor_logs`.")
        lines.append("")
        lines.append("## Alertas/Triggers (fuente: advisor_alerts status=open)")
        if alerts:
            for alert in alerts:
                symbol = f" symbol={alert['symbol']}" if alert["symbol"] else ""
                due = f" due={alert['due_date']}" if alert["due_date"] else ""
                lines.append(f"- [#{alert['id']}] [{alert['severity']}] {alert['alert_type']} | {alert['title']}{symbol}{due}")
        else:
            lines.append("- Sin alertas abiertas.")
        text = "\n".join(lines) + "\n"
        if out:
            with open(out, "w", encoding="utf-8") as fh:
                fh.write(text)
            _print_json({"out": out, "generated_at_utc": now, "open_alerts": len(alerts)})
            return
        console.print(text)
