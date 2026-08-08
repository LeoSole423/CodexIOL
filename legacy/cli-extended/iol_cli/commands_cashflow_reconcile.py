from __future__ import annotations

import hashlib
import json
import re
from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional

import typer
from rich.console import Console

from iol_reconciliation.service import (
    apply_proposal as apply_reconciliation_proposal,
    dismiss_proposal as dismiss_reconciliation_proposal,
    ensure_latest_run as ensure_latest_reconciliation_run,
    explain_interval as explain_reconciliation_interval,
    get_open_payload as get_open_reconciliation_payload,
    run_reconciliation,
)

from .db import connect, init_db, resolve_db_path


console = Console()


def _print_json(data: Any) -> None:
    try:
        text = json.dumps(data, ensure_ascii=True, indent=2)
        console.print_json(text)
    except Exception:
        console.print(data)


def _parse_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def _parse_iso_date_optional(value: Optional[str], label: str) -> Optional[str]:
    if value is None:
        return None
    v = value.strip()
    if not v:
        return None
    try:
        date.fromisoformat(v)
    except Exception as exc:
        raise typer.BadParameter(f"{label} must be YYYY-MM-DD") from exc
    return v


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _normalize_cashflow_amount(kind: str, amount: float) -> float:
    kind_v = (kind or "").strip().lower()
    if kind_v not in ("deposit", "withdraw", "correction"):
        raise typer.BadParameter("--kind must be deposit|withdraw|correction")
    amount_f = float(amount)
    if kind_v in ("deposit", "withdraw") and amount_f < 0:
        raise typer.BadParameter("--amount must be >= 0 for deposit|withdraw")
    if kind_v == "deposit":
        return abs(amount_f)
    if kind_v == "withdraw":
        return -abs(amount_f)
    return amount_f


def _norm_text(v: Any) -> str:
    if v is None:
        return ""
    s = str(v).strip().lower()
    if not s:
        return ""
    repl = {"á": "a", "é": "e", "í": "i", "ó": "o", "ú": "u", "ü": "u", "ñ": "n"}
    for k, vv in repl.items():
        s = s.replace(k, vv)
    return " ".join(s.split())


def _norm_currency(v: Any) -> str:
    s = _norm_text(v)
    if s in ("ars", "peso_argentino", "peso argentino", "pesos", "$", "ars$", "ar$"):
        return "ARS"
    if s in ("usd", "u$s", "us$", "dolar", "dolar estadounidense", "dolares", "dolar_estadounidense"):
        return "USD"
    if not s:
        return "ARS"
    return str(v).strip().upper()


def _safe_float(v: Any) -> Optional[float]:
    if v is None:
        return None
    try:
        return float(v)
    except Exception:
        return None


def _extract_first(rec: Dict[str, Any], keys: List[str]) -> Any:
    for key in keys:
        if key in rec and rec.get(key) is not None:
            return rec.get(key)
    return None


def _normalize_datetime_or_date(v: Any) -> tuple[Optional[str], Optional[str]]:
    raw = str(v or "").strip()
    if not raw:
        return None, None
    try:
        if len(raw) == 10:
            d = date.fromisoformat(raw)
            return f"{d.isoformat()}T00:00:00", d.isoformat()
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        dt_naive = dt.replace(tzinfo=None)
        return dt_naive.isoformat(timespec="seconds"), dt_naive.date().isoformat()
    except Exception:
        try:
            d = date.fromisoformat(raw[:10])
            return f"{d.isoformat()}T00:00:00", d.isoformat()
        except Exception:
            return None, None


def _normalize_symbol_base(v: Any) -> str:
    s = str(v or "").strip().upper()
    if not s:
        return ""
    return re.sub(r"\s+(US\$|USD)$", "", s).strip()


def _infer_movement_kind(kind_raw: Any, description_raw: Any) -> str:
    k = _norm_text(kind_raw)
    d = _norm_text(description_raw)
    s = f"{k} {d}".strip()
    # Accept already-normalized kinds (including legacy and new)
    if k in (
        "external_deposit",
        "external_withdraw",
        "dividend_income",
        "coupon_income",
        "bond_amortization_income",
        "dividend_or_coupon_income",
        "operational_fee_or_tax",
        "settlement_carryover",
        "rotation_internal",
        "fx_revaluation_usd_cash",
        "transfer_internal",
        "correction_unknown",
    ):
        return k
    if any(w in s for w in ("deposito", "aporte", "ingreso externo", "transferencia recibida")):
        return "external_deposit"
    if any(w in s for w in ("retiro", "extraccion", "egreso externo", "transferencia enviada")):
        return "external_withdraw"
    if any(w in s for w in ("amortizacion", "devolucion de capital", "pago de capital")):
        return "bond_amortization_income"
    if any(w in s for w in ("dividendo", "pago de dividendos", "acreditacion dividendo")):
        return "dividend_income"
    if any(w in s for w in ("renta", "cupon", "pago de renta", "pago de cupon", "interes")):
        return "coupon_income"
    if any(w in s for w in ("comision", "impuesto", "iva", "arancel", "gasto", "fee", "derecho")):
        return "operational_fee_or_tax"
    if any(w in s for w in ("liquidacion", "settlement", "carryover")):
        return "settlement_carryover"
    if any(w in s for w in ("rotacion", "rebalance", "rebalanceo")):
        return "rotation_internal"
    if any(w in s for w in ("revaluacion", "tipo de cambio", "fx")):
        return "fx_revaluation_usd_cash"
    if any(w in s for w in ("transferencia interna", "traspaso", "pase interno")):
        return "transfer_internal"
    return "correction_unknown"


def _normalize_movement_amount(kind: str, amount: float) -> float:
    value = float(amount)
    if kind == "external_deposit":
        return abs(value)
    if kind == "external_withdraw":
        return -abs(value)
    if kind in ("dividend_or_coupon_income", "dividend_income", "coupon_income", "bond_amortization_income"):
        return abs(value)
    if kind == "operational_fee_or_tax":
        return -abs(value)
    return value


def _rows_from_json_payload(payload: Any) -> List[Dict[str, Any]]:
    if isinstance(payload, list):
        return [row for row in payload if isinstance(row, dict)]
    if isinstance(payload, dict):
        for key in ("rows", "movements", "items", "data", "result"):
            value = payload.get(key)
            if isinstance(value, list):
                return [row for row in value if isinstance(row, dict)]
    return []


def _movement_to_row(rec: Dict[str, Any], fmt: str, default_source: str) -> Dict[str, Any]:
    occurred_raw = _extract_first(
        rec,
        ["occurred_at", "occurredAt", "timestamp", "datetime", "fecha", "fechaHora", "fecha_operacion", "created_at"],
    )
    occurred_at, inferred_date = _normalize_datetime_or_date(occurred_raw)
    movement_date = _extract_first(rec, ["movement_date", "movementDate", "date", "fecha", "fechaOperacion"])
    if movement_date:
        _, movement_date_norm = _normalize_datetime_or_date(movement_date)
    else:
        movement_date_norm = inferred_date
    if movement_date_norm is None:
        raise ValueError("movement_date/occurred_at is required")

    amount_raw = _extract_first(rec, ["amount", "monto", "importe", "net_amount", "value"])
    amount = _safe_float(amount_raw)
    if amount is None:
        raise ValueError("amount is required")

    currency = _norm_currency(_extract_first(rec, ["currency", "moneda", "currency_code"]))
    description = _extract_first(rec, ["description", "descripcion", "detail", "concepto", "concept"])
    kind_raw = _extract_first(rec, ["kind", "tipo", "movement_type", "movementType", "category"])
    kind = _infer_movement_kind(kind_raw, description)
    amount_norm = _normalize_movement_amount(kind, amount)
    source = str(_extract_first(rec, ["source", "origen"]) or default_source).strip() or default_source

    movement_id_raw = _extract_first(rec, ["movement_id", "movementId", "id", "numero", "nro", "transaction_id"])
    movement_id = str(movement_id_raw).strip() if movement_id_raw is not None else ""
    if not movement_id:
        stable = {
            "occurred_at": occurred_at,
            "movement_date": movement_date_norm,
            "currency": currency,
            "amount": round(float(amount_norm), 8),
            "kind": kind,
            "description": str(description or "").strip(),
            "source": source,
            "symbol_base": _normalize_symbol_base(rec.get("symbol") or rec.get("simbolo")),
        }
        digest = hashlib.sha1(json.dumps(stable, ensure_ascii=True, sort_keys=True).encode("utf-8")).hexdigest()
        movement_id = f"hash:{digest}"

    if fmt not in ("normalized", "iol_raw"):
        raise ValueError("format must be normalized|iol_raw")

    symbol_raw = _extract_first(rec, ["symbol", "simbolo", "ticker", "instrument"])
    symbol = _normalize_symbol_base(symbol_raw) if symbol_raw else None

    return {
        "movement_id": movement_id,
        "occurred_at": occurred_at,
        "movement_date": movement_date_norm,
        "currency": currency,
        "amount": float(amount_norm),
        "kind": kind,
        "symbol": symbol,
        "description": (str(description).strip() if description is not None else None),
        "source": source,
        "raw_json": json.dumps(rec, ensure_ascii=True),
    }


def build_cashflow_app() -> typer.Typer:
    app = typer.Typer(help="Manual cashflow adjustments (for real return reconciliation)")

    @app.command("add")
    def cashflow_add(
        ctx: typer.Context,
        flow_date: str = typer.Option(..., "--date", help="YYYY-MM-DD"),
        kind: str = typer.Option(..., "--kind", help="deposit|withdraw|correction"),
        amount: float = typer.Option(..., "--amount", help="Amount in ARS"),
        note: Optional[str] = typer.Option(None, "--note", help="Optional note"),
    ):
        try:
            d = _parse_date(flow_date).isoformat()
            kind_v = (kind or "").strip().lower()
            amount_norm = _normalize_cashflow_amount(kind_v, amount)
        except Exception as exc:
            raise typer.BadParameter(str(exc)) from exc

        db_path = resolve_db_path(ctx.obj.config.db_path)
        conn = connect(db_path)
        init_db(conn)
        try:
            row = conn.execute(
                """
                INSERT INTO manual_cashflow_adjustments(flow_date, kind, amount_ars, note, created_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (d, kind_v, float(amount_norm), (note.strip() if note and note.strip() else None), _utc_now_iso()),
            )
            conn.commit()
            out = conn.execute(
                """
                SELECT id, flow_date, kind, amount_ars, note, created_at
                FROM manual_cashflow_adjustments
                WHERE id = ?
                """,
                (int(row.lastrowid),),
            ).fetchone()
            _print_json(dict(out))
        finally:
            conn.close()

    @app.command("list")
    def cashflow_list(
        ctx: typer.Context,
        date_from: Optional[str] = typer.Option(None, "--from", help="YYYY-MM-DD"),
        date_to: Optional[str] = typer.Option(None, "--to", help="YYYY-MM-DD"),
    ):
        f = _parse_iso_date_optional(date_from, "--from")
        t = _parse_iso_date_optional(date_to, "--to")
        db_path = resolve_db_path(ctx.obj.config.db_path)
        conn = connect(db_path)
        init_db(conn)
        try:
            rows = conn.execute(
                """
                SELECT id, flow_date, kind, amount_ars, note, created_at
                FROM manual_cashflow_adjustments
                WHERE (? IS NULL OR flow_date >= ?)
                  AND (? IS NULL OR flow_date <= ?)
                ORDER BY flow_date DESC, id DESC
                """,
                (f, f, t, t),
            ).fetchall()
            _print_json([dict(r) for r in rows])
        finally:
            conn.close()

    @app.command("delete")
    def cashflow_delete(
        ctx: typer.Context,
        row_id: int = typer.Option(..., "--id", min=1, help="Adjustment ID"),
    ):
        db_path = resolve_db_path(ctx.obj.config.db_path)
        conn = connect(db_path)
        init_db(conn)
        try:
            cur = conn.execute("DELETE FROM manual_cashflow_adjustments WHERE id = ?", (int(row_id),))
            conn.commit()
            if int(cur.rowcount or 0) <= 0:
                console.print("Adjustment ID not found.")
                raise typer.Exit(code=1)
            _print_json({"ok": True, "id": int(row_id)})
        finally:
            conn.close()

    @app.command("import-movements")
    def cashflow_import_movements(
        ctx: typer.Context,
        file_path: str = typer.Option(..., "--file", help="Path to JSON file"),
        fmt: str = typer.Option("normalized", "--format", help="normalized|iol_raw"),
        source: str = typer.Option("json_import", "--source", help="Movement source tag"),
        date_from: Optional[str] = typer.Option(None, "--from", help="YYYY-MM-DD"),
        date_to: Optional[str] = typer.Option(None, "--to", help="YYYY-MM-DD"),
        replace_window: bool = typer.Option(False, "--replace-window", help="Delete existing movements in selected window before importing"),
    ):
        fmt_norm = (fmt or "").strip().lower()
        if fmt_norm not in ("normalized", "iol_raw"):
            raise typer.BadParameter("--format must be normalized|iol_raw")
        f = _parse_iso_date_optional(date_from, "--from")
        t = _parse_iso_date_optional(date_to, "--to")
        if f and t and f > t:
            raise typer.BadParameter("--from must be <= --to")
        try:
            with open(file_path, "r", encoding="utf-8") as fh:
                payload = json.load(fh)
        except FileNotFoundError as exc:
            raise typer.BadParameter(f"--file not found: {file_path}") from exc
        except json.JSONDecodeError as exc:
            raise typer.BadParameter(f"--file is not valid JSON: {exc}") from exc
        src_rows = _rows_from_json_payload(payload)
        if not src_rows:
            _print_json({"inserted": 0, "updated": 0, "skipped": 0, "total_input": 0})
            return

        parsed_rows: List[Dict[str, Any]] = []
        skipped = 0
        for rec in src_rows:
            try:
                row = _movement_to_row(rec, fmt_norm, source)
                movement_date = str(row["movement_date"])
                if f and movement_date < f:
                    skipped += 1
                    continue
                if t and movement_date > t:
                    skipped += 1
                    continue
                parsed_rows.append(row)
            except Exception:
                skipped += 1
        if not parsed_rows:
            _print_json({"inserted": 0, "updated": 0, "skipped": skipped, "total_input": len(src_rows)})
            return
        if replace_window and (f is None or t is None):
            dates = sorted(str(r["movement_date"]) for r in parsed_rows)
            f = f or dates[0]
            t = t or dates[-1]

        db_path = resolve_db_path(ctx.obj.config.db_path)
        conn = connect(db_path)
        init_db(conn)
        inserted = 0
        updated = 0
        try:
            if replace_window and f and t:
                conn.execute("DELETE FROM account_cash_movements WHERE movement_date >= ? AND movement_date <= ?", (f, t))
                conn.commit()
            for row in parsed_rows:
                exists = conn.execute(
                    "SELECT id FROM account_cash_movements WHERE movement_id = ?",
                    (row["movement_id"],),
                ).fetchone()
                if exists:
                    conn.execute(
                        """
                        UPDATE account_cash_movements
                        SET occurred_at = ?, movement_date = ?, currency = ?, amount = ?, kind = ?,
                            symbol = ?, description = ?, source = ?, raw_json = ?
                        WHERE movement_id = ?
                        """,
                        (
                            row.get("occurred_at"),
                            row["movement_date"],
                            row["currency"],
                            float(row["amount"]),
                            row["kind"],
                            row.get("symbol"),
                            row.get("description"),
                            row.get("source"),
                            row.get("raw_json"),
                            row["movement_id"],
                        ),
                    )
                    updated += 1
                else:
                    conn.execute(
                        """
                        INSERT INTO account_cash_movements(
                            movement_id, occurred_at, movement_date, currency, amount, kind,
                            symbol, description, source, raw_json, created_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            row["movement_id"],
                            row.get("occurred_at"),
                            row["movement_date"],
                            row["currency"],
                            float(row["amount"]),
                            row["kind"],
                            row.get("symbol"),
                            row.get("description"),
                            row.get("source"),
                            row.get("raw_json"),
                            _utc_now_iso(),
                        ),
                    )
                    inserted += 1
            conn.commit()
        finally:
            conn.close()
        _print_json(
            {
                "inserted": int(inserted),
                "updated": int(updated),
                "skipped": int(skipped),
                "total_input": int(len(src_rows)),
                "selected_rows": int(len(parsed_rows)),
                "window": {"from": f, "to": t},
                "replace_window": bool(replace_window),
            }
        )

    return app


def build_reconcile_app() -> typer.Typer:
    app = typer.Typer(help="Movement reconciliation and inference resolution")

    @app.command("run")
    def reconcile_run(
        ctx: typer.Context,
        as_of: Optional[str] = typer.Option(None, "--as-of", help="Optional YYYY-MM-DD"),
        days: int = typer.Option(30, "--days", min=2, max=3650),
        date_from: Optional[str] = typer.Option(None, "--from", help="YYYY-MM-DD"),
        date_to: Optional[str] = typer.Option(None, "--to", help="YYYY-MM-DD"),
        force: bool = typer.Option(False, "--force", help="Recompute even if an equivalent run already exists"),
    ):
        f = _parse_iso_date_optional(date_from, "--from")
        t = _parse_iso_date_optional(date_to, "--to")
        if f and t and f > t:
            raise typer.BadParameter("--from must be <= --to")
        db_path = resolve_db_path(ctx.obj.config.db_path)
        conn = connect(db_path)
        init_db(conn)
        try:
            payload = run_reconciliation(conn, as_of=as_of, days=int(days), date_from=f, date_to=t, force=bool(force))
            _print_json(payload)
        finally:
            conn.close()

    @app.command("list-open")
    def reconcile_list_open(
        ctx: typer.Context,
        as_of: Optional[str] = typer.Option(None, "--as-of", help="Optional YYYY-MM-DD"),
    ):
        db_path = resolve_db_path(ctx.obj.config.db_path)
        conn = connect(db_path)
        init_db(conn)
        try:
            payload = get_open_reconciliation_payload(conn, as_of=as_of, ensure=True)
            _print_json(payload)
        finally:
            conn.close()

    @app.command("apply")
    def reconcile_apply(
        ctx: typer.Context,
        proposal_id: int = typer.Option(..., "--proposal-id", min=1),
        note: Optional[str] = typer.Option(None, "--note", help="Optional note to audit the decision"),
    ):
        db_path = resolve_db_path(ctx.obj.config.db_path)
        conn = connect(db_path)
        init_db(conn)
        try:
            payload = apply_reconciliation_proposal(conn, int(proposal_id), note=note)
            _print_json(payload)
        except ValueError as exc:
            console.print(str(exc))
            raise typer.Exit(code=1)
        finally:
            conn.close()

    @app.command("dismiss")
    def reconcile_dismiss(
        ctx: typer.Context,
        proposal_id: int = typer.Option(..., "--proposal-id", min=1),
        reason: str = typer.Option(..., "--reason", help="Why the proposal should be ignored"),
    ):
        db_path = resolve_db_path(ctx.obj.config.db_path)
        conn = connect(db_path)
        init_db(conn)
        try:
            payload = dismiss_reconciliation_proposal(conn, int(proposal_id), reason=reason)
            _print_json(payload)
        except ValueError as exc:
            console.print(str(exc))
            raise typer.Exit(code=1)
        finally:
            conn.close()

    @app.command("explain")
    def reconcile_explain(
        ctx: typer.Context,
        interval_id: int = typer.Option(..., "--interval-id", min=1),
    ):
        db_path = resolve_db_path(ctx.obj.config.db_path)
        conn = connect(db_path)
        init_db(conn)
        try:
            ensure_latest_reconciliation_run(conn)
            payload = explain_reconciliation_interval(conn, int(interval_id))
            _print_json(payload)
        except ValueError as exc:
            console.print(str(exc))
            raise typer.Exit(code=1)
        finally:
            conn.close()

    return app
