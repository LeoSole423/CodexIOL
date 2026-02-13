import hashlib
import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Tuple

from .config import Config
from .db import connect, init_db, resolve_db_path
from .iol_client import IOLClient
from .util import default_valid_until, normalize_market, normalize_order_type, normalize_plazo


class BatchError(RuntimeError):
    pass


@dataclass
class BatchDefaults:
    market: str
    plazo: str
    order_type: str
    price_mode: str
    valid_until: Optional[str]


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def load_plan(plan_path: str) -> Tuple[Dict[str, Any], bytes]:
    with open(plan_path, "rb") as f:
        raw = f.read()
    try:
        plan = json.loads(raw.decode("utf-8"))
    except Exception as exc:
        raise BatchError(f"Invalid JSON in plan: {plan_path}") from exc
    if not isinstance(plan, dict):
        raise BatchError("Plan must be a JSON object")
    return plan, raw


def _get_defaults(plan: Dict[str, Any], price_mode_override: Optional[str], default_market: str, default_plazo: str) -> BatchDefaults:
    defaults = plan.get("defaults") or {}
    if not isinstance(defaults, dict):
        raise BatchError("defaults must be an object")
    market = str(defaults.get("market") or default_market)
    plazo = str(defaults.get("plazo") or default_plazo)
    order_type = str(defaults.get("order_type") or "limit")
    price_mode = str(price_mode_override or defaults.get("price_mode") or "fast").lower().strip()
    valid_until = defaults.get("valid_until")
    if valid_until is not None:
        valid_until = str(valid_until)
    return BatchDefaults(market=market, plazo=plazo, order_type=order_type, price_mode=price_mode, valid_until=valid_until)


def validate_plan(plan: Dict[str, Any]) -> None:
    version = plan.get("version")
    if version != 1:
        raise BatchError(f"Unsupported plan version: {version}")
    defaults = plan.get("defaults") or {}
    if defaults is not None:
        if not isinstance(defaults, dict):
            raise BatchError("defaults must be an object")
        if "order_type" in defaults:
            ot = str(defaults.get("order_type") or "").lower().strip()
            if ot and ot not in ("limit", "market"):
                raise BatchError("defaults.order_type must be 'limit' or 'market'")
        if "price_mode" in defaults:
            pm = str(defaults.get("price_mode") or "").lower().strip()
            if pm and pm not in ("fast", "mid", "last"):
                raise BatchError("defaults.price_mode must be fast|mid|last")
    ops = plan.get("ops")
    if not isinstance(ops, list) or not ops:
        raise BatchError("Plan ops must be a non-empty array")
    for idx, op in enumerate(ops):
        if not isinstance(op, dict):
            raise BatchError(f"op[{idx}] must be an object")
        kind = op.get("kind")
        if kind not in ("order", "fci"):
            raise BatchError(f"op[{idx}].kind must be 'order' or 'fci'")
        if kind == "order":
            side = op.get("side")
            if side not in ("buy", "sell"):
                raise BatchError(f"op[{idx}].side must be 'buy' or 'sell'")
            symbol = op.get("symbol")
            if not symbol:
                raise BatchError(f"op[{idx}].symbol is required")
            if "notional_divisor" in op and op.get("notional_divisor") is not None:
                try:
                    if float(op.get("notional_divisor")) <= 0:
                        raise BatchError(f"op[{idx}].notional_divisor must be > 0")
                except ValueError:
                    raise BatchError(f"op[{idx}].notional_divisor must be a number")
            order_type = str(op.get("order_type") or "limit").lower().strip()
            if order_type not in ("limit", "market"):
                raise BatchError(f"op[{idx}].order_type must be 'limit' or 'market'")
            qty = op.get("quantity")
            amt = op.get("amount")
            if side == "buy":
                if (qty is None and amt is None) or (qty is not None and amt is not None):
                    raise BatchError(f"op[{idx}] buy requires exactly one of quantity or amount")
                if qty is not None and float(qty) <= 0:
                    raise BatchError(f"op[{idx}] buy quantity must be > 0")
                if amt is not None and float(amt) <= 0:
                    raise BatchError(f"op[{idx}] buy amount must be > 0")
            if side == "sell":
                if qty is None:
                    raise BatchError(f"op[{idx}] sell requires quantity")
                if amt is not None:
                    raise BatchError(f"op[{idx}] sell must not include amount")
                if float(qty) <= 0:
                    raise BatchError(f"op[{idx}] sell quantity must be > 0")
        else:
            action = op.get("action")
            if action not in ("subscribe", "redeem"):
                raise BatchError(f"op[{idx}].action must be 'subscribe' or 'redeem'")
            symbol = op.get("symbol")
            if not symbol:
                raise BatchError(f"op[{idx}].symbol is required")
            if action == "subscribe" and op.get("amount") is None:
                raise BatchError(f"op[{idx}] subscribe requires amount")
            if action == "redeem" and op.get("quantity") is None:
                raise BatchError(f"op[{idx}] redeem requires quantity")
            if action == "subscribe" and float(op.get("amount")) <= 0:
                raise BatchError(f"op[{idx}] subscribe amount must be > 0")
            if action == "redeem" and float(op.get("quantity")) <= 0:
                raise BatchError(f"op[{idx}] redeem quantity must be > 0")


def _pick_price_from_quote(quote: Dict[str, Any], side: str, price_mode: str) -> Tuple[float, Dict[str, Any]]:
    puntas = quote.get("puntas") or []
    last = quote.get("ultimoPrecio")
    bid = None
    ask = None
    if puntas:
        bid = puntas[0].get("precioCompra")
        ask = puntas[0].get("precioVenta")
    used = {"bid": bid, "ask": ask, "last": last, "mode": price_mode}

    def _require(val: Any, label: str) -> float:
        if val is None:
            raise BatchError(f"Missing quote field for price mode {price_mode}: {label}")
        return float(val)

    mode = price_mode.lower().strip()
    if mode == "fast":
        if side == "sell" and bid is not None:
            return float(bid), used
        if side == "buy" and ask is not None:
            return float(ask), used
        if last is not None:
            return float(last), used
        raise BatchError("Quote has no puntas and no ultimoPrecio")
    if mode == "last":
        return _require(last, "ultimoPrecio"), used
    if mode == "mid":
        if bid is not None and ask is not None:
            return (float(bid) + float(ask)) / 2.0, used
        if last is not None:
            return float(last), used
        raise BatchError("Quote has no puntas and no ultimoPrecio")
    raise BatchError(f"Invalid price_mode: {price_mode}")


def _build_order_payload(
    side: str,
    market: str,
    symbol: str,
    quantity: Optional[float],
    amount: Optional[float],
    price: float,
    plazo: str,
    valid_until: Optional[str],
    order_type: str,
    source_id: Optional[int],
) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "mercado": normalize_market(market),
        "simbolo": symbol,
        "precio": float(price),
        "validez": valid_until or default_valid_until(),
    }
    if side == "buy":
        payload["plazo"] = normalize_plazo(plazo)
    if side == "sell" and plazo:
        payload["plazo"] = normalize_plazo(plazo)
    if quantity is not None:
        payload["cantidad"] = float(quantity)
    if amount is not None:
        payload["monto"] = float(amount)
    if order_type:
        payload["tipoOrden"] = normalize_order_type(order_type)
    if source_id is not None:
        payload["idFuente"] = int(source_id)
    return payload


def _resolve_snapshot_date(conn, plan: Dict[str, Any]) -> Optional[str]:
    ctx = plan.get("context") or {}
    if isinstance(ctx, dict) and ctx.get("snapshot_date"):
        return str(ctx["snapshot_date"])
    row = conn.execute("SELECT snapshot_date FROM portfolio_snapshots ORDER BY snapshot_date DESC LIMIT 1").fetchone()
    return row["snapshot_date"] if row else None


def _load_snapshot_context(conn, snapshot_date: Optional[str]) -> Dict[str, Any]:
    if not snapshot_date:
        return {"snapshot_date": None, "cash_ars": None, "cash_usd": None, "holdings": {}}
    row = conn.execute(
        """
        SELECT cash_disponible_ars, cash_disponible_usd
        FROM portfolio_snapshots
        WHERE snapshot_date=?
        """,
        (snapshot_date,),
    ).fetchone()
    cash_ars = row["cash_disponible_ars"] if row else None
    cash_usd = row["cash_disponible_usd"] if row else None
    holdings = {}
    for a in conn.execute(
        "SELECT symbol, quantity FROM portfolio_assets WHERE snapshot_date=?",
        (snapshot_date,),
    ).fetchall():
        try:
            holdings[str(a["symbol"])] = float(a["quantity"] or 0.0)
        except Exception:
            holdings[str(a["symbol"])] = 0.0
    return {"snapshot_date": snapshot_date, "cash_ars": cash_ars, "cash_usd": cash_usd, "holdings": holdings}


def _risk_warnings(kind: str, op: Dict[str, Any], snapshot_ctx: Dict[str, Any], implied_price: Optional[float]) -> List[str]:
    warnings: List[str] = []
    holdings = snapshot_ctx.get("holdings") or {}
    cash_ars = snapshot_ctx.get("cash_ars")

    if kind == "order":
        side = str(op.get("side") or "").lower().strip()
        symbol = str(op.get("symbol") or "")
        qty = op.get("quantity")
        amt = op.get("amount")
        # Some instruments (e.g. AR bonds) are quoted "per 100 nominal" in IOL.
        # Plans may specify a divisor to make cash/notional warnings comparable to ARS cash.
        notional_divisor = op.get("notional_divisor", 1)
        try:
            notional_divisor_f = float(notional_divisor)
            if notional_divisor_f <= 0:
                notional_divisor_f = 1.0
        except Exception:
            notional_divisor_f = 1.0
        if side == "sell" and qty is not None:
            owned = float(holdings.get(symbol, 0.0) or 0.0)
            if float(qty) > owned + 1e-9:
                warnings.append(f"SELL qty {qty} > snapshot holdings {owned} for {symbol}")
        if side == "buy":
            if cash_ars is not None:
                try:
                    cash_ars_f = float(cash_ars)
                    if amt is not None:
                        if float(amt) > cash_ars_f + 1e-9:
                            warnings.append(f"BUY amount {amt} ARS > cash_disponible_ars {cash_ars_f} (snapshot)")
                    elif qty is not None and implied_price is not None:
                        notional = float(qty) * float(implied_price) / notional_divisor_f
                        if notional > cash_ars_f + 1e-9:
                            warnings.append(f"BUY notional {notional:.2f} ARS > cash_disponible_ars {cash_ars_f} (snapshot)")
                except Exception:
                    pass
    else:
        action = str(op.get("action") or "").lower().strip()
        if action == "subscribe" and snapshot_ctx.get("cash_ars") is not None:
            try:
                amt = float(op.get("amount"))
                cash_ars_f = float(snapshot_ctx["cash_ars"])
                if amt > cash_ars_f + 1e-9:
                    warnings.append(f"FCI subscribe amount {amt} ARS > cash_disponible_ars {cash_ars_f} (snapshot)")
            except Exception:
                pass

    return warnings


def run_batch(
    client: IOLClient,
    config: Config,
    plan_path: str,
    dry_run: bool,
    price_mode_override: Optional[str],
    default_market: str,
    default_plazo: str,
    confirm_enabled: bool,
    on_preview: Optional[Callable[[List[Dict[str, Any]]], None]] = None,
) -> Dict[str, Any]:
    plan, raw = load_plan(plan_path)
    validate_plan(plan)

    defaults = _get_defaults(plan, price_mode_override, default_market, default_plazo)

    db_path = resolve_db_path(config.db_path)
    conn = connect(db_path)
    init_db(conn)
    snapshot_date = _resolve_snapshot_date(conn, plan)
    snapshot_ctx = _load_snapshot_context(conn, snapshot_date)

    created_at = _utc_now_iso()
    plan_hash = _sha256_hex(raw)
    run_status = "preview"
    run_id = conn.execute(
        """
        INSERT INTO batch_runs (created_at_utc, plan_path, plan_hash, snapshot_date, status, error_message)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (created_at, os.path.abspath(plan_path), plan_hash, snapshot_date, run_status, None),
    ).lastrowid
    conn.commit()

    summary_ops: List[Dict[str, Any]] = []
    prepared_rows: List[Dict[str, Any]] = []

    try:
        ops: List[Dict[str, Any]] = plan.get("ops") or []
        # Phase 1: preview/prepare (compute quotes/prices/payloads, store prepared ops)
        for idx, op in enumerate(ops):
            kind = op["kind"]
            op_created_at = _utc_now_iso()
            op_row_id = conn.execute(
                """
                INSERT INTO batch_ops (
                    run_id, idx, kind, action, symbol, payload_json, quote_json,
                    result_json, status, iol_order_number, error_message, created_at_utc
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (run_id, idx, kind, "preview", None, None, None, None, "preview", None, None, op_created_at),
            ).lastrowid
            conn.commit()

            try:
                if kind == "order":
                    side = op["side"]
                    action = side
                    symbol = str(op["symbol"])
                    market = str(op.get("market") or defaults.market)
                    plazo = str(op.get("plazo") or defaults.plazo)
                    order_type = str(op.get("order_type") or defaults.order_type)
                    qty = op.get("quantity")
                    amt = op.get("amount")
                    fixed_price = op.get("price")
                    price_mode = str(op.get("price_mode") or defaults.price_mode)
                    especie_d = bool(op.get("especie_d") or False)
                    source_id = op.get("source_id")
                    valid_until = op.get("valid_until") or defaults.valid_until

                    quote_json = None
                    used_meta = None
                    if fixed_price is None:
                        quote = client.get_quote(normalize_market(market), symbol)
                        price, used_meta = _pick_price_from_quote(quote, side=side, price_mode=price_mode)
                        quote_json = json.dumps({"quote": quote, "used": used_meta}, ensure_ascii=True)
                    else:
                        price = float(fixed_price)

                    payload = _build_order_payload(
                        side=side,
                        market=market,
                        symbol=symbol,
                        quantity=qty,
                        amount=amt,
                        price=price,
                        plazo=plazo,
                        valid_until=valid_until,
                        order_type=order_type,
                        source_id=source_id,
                    )

                    warnings = _risk_warnings(kind, op, snapshot_ctx, implied_price=price)

                    conn.execute(
                        """
                        UPDATE batch_ops
                        SET action=?, symbol=?, payload_json=?, quote_json=?, status=?
                        WHERE id=?
                        """,
                        (action, symbol, json.dumps(payload, ensure_ascii=True), quote_json, "prepared", op_row_id),
                    )
                    conn.commit()

                    summary_ops.append(
                        {
                            "idx": idx,
                            "kind": kind,
                            "action": action,
                            "symbol": symbol,
                            "market": market,
                            "plazo": plazo,
                            "order_type": order_type,
                            "quantity": qty,
                            "amount": amt,
                            "price": price,
                            "price_mode": price_mode if fixed_price is None else "fixed",
                            "especie_d": especie_d,
                            "status": "prepared",
                            "batch_op_id": op_row_id,
                            "quote_used": used_meta,
                            "warnings": warnings,
                        }
                    )
                    prepared_rows.append(
                        {
                            "id": op_row_id,
                            "idx": idx,
                            "kind": kind,
                            "side": side,
                            "action": action,
                            "symbol": symbol,
                            "payload": payload,
                            "especie_d": especie_d,
                        }
                    )

                else:
                    action = op["action"]
                    symbol = str(op["symbol"])
                    validate_only = bool(op.get("validate_only") or False)
                    if action == "subscribe":
                        payload = {"simbolo": symbol, "monto": float(op["amount"]), "soloValidar": validate_only}
                    else:
                        payload = {"simbolo": symbol, "cantidad": float(op["quantity"]), "soloValidar": validate_only}

                    warnings = _risk_warnings(kind, op, snapshot_ctx, implied_price=None)

                    conn.execute(
                        """
                        UPDATE batch_ops
                        SET action=?, symbol=?, payload_json=?, status=?
                        WHERE id=?
                        """,
                        (action, symbol, json.dumps(payload, ensure_ascii=True), "prepared", op_row_id),
                    )
                    conn.commit()

                    summary_ops.append(
                        {
                            "idx": idx,
                            "kind": kind,
                            "action": action,
                            "symbol": symbol,
                            "payload": payload,
                            "status": "prepared",
                            "batch_op_id": op_row_id,
                            "warnings": warnings,
                        }
                    )
                    prepared_rows.append(
                        {
                            "id": op_row_id,
                            "idx": idx,
                            "kind": kind,
                            "action": action,
                            "symbol": symbol,
                            "payload": payload,
                        }
                    )

            except Exception as exc:
                conn.execute("UPDATE batch_ops SET status=?, error_message=? WHERE id=?", ("error", str(exc), op_row_id))
                conn.execute("UPDATE batch_runs SET status=?, error_message=? WHERE id=?", ("error", str(exc), run_id))
                conn.commit()
                raise

        if dry_run or not confirm_enabled:
            conn.execute("UPDATE batch_runs SET status=? WHERE id=?", ("dry_run", run_id))
            conn.commit()
            return {
                "run_id": run_id,
                "created_at_utc": created_at,
                "snapshot_date": snapshot_date,
                "plan_path": os.path.abspath(plan_path),
                "plan_hash": plan_hash,
                "dry_run": True,
                "ops": summary_ops,
            }

        if on_preview is not None:
            on_preview(summary_ops)

        # Phase 2: execute (sequential; stop on first error)
        conn.execute("UPDATE batch_runs SET status=? WHERE id=?", ("running", run_id))
        conn.commit()
        for row in prepared_rows:
            op_row_id = row["id"]
            try:
                if row["kind"] == "order":
                    if row["side"] == "buy":
                        result = client.buy(row["payload"], especie_d=bool(row.get("especie_d") or False))
                    else:
                        result = client.sell(row["payload"], especie_d=bool(row.get("especie_d") or False))
                else:
                    if row["action"] == "subscribe":
                        result = client.fci_subscribe(row["payload"])
                    else:
                        result = client.fci_redeem(row["payload"])

                result_json = json.dumps(result, ensure_ascii=True)
                op_number = result.get("numeroOperacion") if isinstance(result, dict) else None
                conn.execute(
                    """
                    UPDATE batch_ops
                    SET result_json=?, status=?, iol_order_number=?
                    WHERE id=?
                    """,
                    (result_json, "ok", op_number, op_row_id),
                )
                conn.commit()

                for s in summary_ops:
                    if s.get("batch_op_id") == op_row_id:
                        s["status"] = "ok"
                        s["iol_order_number"] = op_number
                        break

            except Exception as exc:
                conn.execute("UPDATE batch_ops SET status=?, error_message=? WHERE id=?", ("error", str(exc), op_row_id))
                conn.execute("UPDATE batch_runs SET status=?, error_message=? WHERE id=?", ("error", str(exc), run_id))
                conn.commit()
                raise

        conn.execute("UPDATE batch_runs SET status=? WHERE id=?", ("ok", run_id))
        conn.commit()
    except Exception as exc:
        raise
    finally:
        conn.close()

    return {
        "run_id": run_id,
        "created_at_utc": created_at,
        "snapshot_date": snapshot_date,
        "plan_path": os.path.abspath(plan_path),
        "plan_hash": plan_hash,
        "dry_run": False,
        "ops": summary_ops,
    }


def plan_template() -> Dict[str, Any]:
    return {
        "version": 1,
        "created_at_utc": _utc_now_iso(),
        "context": {"country": "argentina", "snapshot_date": None},
        "defaults": {"market": "bcba", "plazo": "t1", "order_type": "limit", "price_mode": "fast"},
        "ops": [
            {
                "kind": "order",
                "side": "sell",
                "symbol": "ALUA",
                "quantity": 1,
                "price_mode": "fast",
                "plazo": "t1",
                "order_type": "limit",
                "notes": "SELL uses bid in fast mode",
            },
            {
                "kind": "order",
                "side": "buy",
                "symbol": "SPY",
                "quantity": 1,
                "price_mode": "fast",
                "plazo": "t1",
                "order_type": "limit",
                "notes": "BUY uses ask in fast mode",
            },
            {
                "kind": "fci",
                "action": "subscribe",
                "symbol": "ADRDOLA",
                "amount": 10000.0,
                "validate_only": False,
            },
        ],
    }


def plan_from_md(md_path: str, out_path: str) -> Dict[str, Any]:
    with open(md_path, "r", encoding="utf-8") as f:
        text = f.read()

    snapshot_date = None
    for line in text.splitlines():
        if "snapshot IOL" in line and "**" in line:
            # Example: snapshot IOL **2026-02-06**
            parts = line.split("**")
            if len(parts) >= 3:
                snapshot_date = parts[1].strip()
                break

    adr_amount = None
    for line in text.splitlines():
        if "ADRDOLA" in line and "ARS" in line and "target" in line:
            # Example: ADRDOLA (9,7% target): **ARS 222,318.90**
            if "**" in line:
                parts = line.split("**")
                if len(parts) >= 3 and "ARS" in parts[1]:
                    raw_amt = parts[1].replace("ARS", "").strip()
                    raw_amt = raw_amt.replace(",", "")
                    try:
                        adr_amount = float(raw_amt)
                    except ValueError:
                        pass

    ops: List[Dict[str, Any]] = []
    in_table = False
    for line in text.splitlines():
        l = line.strip()
        if l.startswith("| Simbolo |"):
            in_table = True
            continue
        if in_table:
            if not l.startswith("|"):
                break
            cols = [c.strip() for c in l.strip("|").split("|")]
            if len(cols) < 6 or cols[0] in ("---", ""):
                continue
            symbol, asset_type, _, _, delta_qty, action = cols[:6]
            action = action.upper()
            delta_qty = delta_qty.replace(".", "").replace(",", ".") if "," in delta_qty else delta_qty
            try:
                delta = float(delta_qty)
            except ValueError:
                continue
            if action == "MANTENER" or abs(delta) < 1e-9:
                continue
            if action == "VENDER":
                ops.append({"kind": "order", "side": "sell", "symbol": symbol, "quantity": abs(delta)})
            elif action == "COMPRAR":
                ops.append({"kind": "order", "side": "buy", "symbol": symbol, "quantity": abs(delta)})
            elif action == "RESCATAR":
                ops.append({"kind": "fci", "action": "redeem", "symbol": symbol, "quantity": abs(delta)})
            elif action == "SUSCRIBIR":
                if symbol == "ADRDOLA" and adr_amount is not None:
                    ops.append({"kind": "fci", "action": "subscribe", "symbol": symbol, "amount": adr_amount})
                else:
                    raise BatchError(f"Cannot infer subscribe amount for {symbol}. Provide amount in plan JSON.")

    plan = {
        "version": 1,
        "created_at_utc": _utc_now_iso(),
        "context": {"country": "argentina", "snapshot_date": snapshot_date},
        "defaults": {"market": "bcba", "plazo": "t1", "order_type": "limit", "price_mode": "fast"},
        "ops": ops,
    }

    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(plan, f, indent=2, ensure_ascii=True)
    return plan
