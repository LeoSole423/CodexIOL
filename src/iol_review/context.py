from __future__ import annotations

import hashlib
import json
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from . import SCHEMA_VERSION
from .gateway import ReadOnlyIOLGateway


def _number(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _pick(item: dict[str, Any], *names: str) -> Any:
    for name in names:
        if name in item and item[name] is not None:
            return item[name]
    return None


def _items(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [row for row in payload if isinstance(row, dict)]
    if isinstance(payload, dict):
        for name in ("activos", "titulos", "items", "data"):
            if isinstance(payload.get(name), list):
                return [row for row in payload[name] if isinstance(row, dict)]
    return []


def normalize_portfolio(payload: Any, country: str) -> dict[str, Any]:
    assets: list[dict[str, Any]] = []
    for row in _items(payload):
        title = row.get("titulo") if isinstance(row.get("titulo"), dict) else {}
        symbol = _pick(row, "simbolo", "symbol", "ticker", "especie") or _pick(title, "simbolo", "symbol", "ticker")
        if not symbol:
            continue
        assets.append({
            "symbol": str(symbol).upper(),
            "market": country,
            "instrument": _pick(row, "descripcion", "description", "tipo", "instrumento") or _pick(title, "descripcion", "description", "tipo", "instrumento"),
            "quantity": _number(_pick(row, "cantidad", "quantity")),
            "price_iol": _number(_pick(row, "ultimoPrecio", "precio", "lastPrice", "cotizacion")),
            "valuation": _number(_pick(row, "valorizado", "valuation", "importe")),
            "gain_percent_reported": _number(_pick(row, "gananciaPorcentaje", "profitPercent")),
            "gain_amount_reported": _number(_pick(row, "gananciaDinero", "profitAmount")),
            "currency": _pick(row, "moneda", "currency") or _pick(title, "moneda", "currency"),
        })
    cash: list[dict[str, Any]] = []
    if isinstance(payload, dict):
        for row in _items(payload.get("disponible") or payload.get("efectivo") or []):
            cash.append({"currency": _pick(row, "moneda", "currency"), "amount": _number(_pick(row, "importe", "saldo", "amount"))})
        for key, currency in (("totalEnPesos", "ARS"), ("totalEnDolares", "USD")):
            value = _number(payload.get(key))
            if value is not None:
                cash.append({"currency": currency, "amount": value, "reported_total": True})
    return {"country": country, "assets": assets, "cash": cash}


def normalize_account_status(payload: Any) -> dict[str, Any]:
    """Persist a small financial summary, never the raw account response."""
    source = payload if isinstance(payload, dict) else {}
    balances = []
    for row in _items(source.get("saldos") or source.get("cuentas") or []):
        balances.append({"currency": _pick(row, "moneda", "currency"), "amount": _number(_pick(row, "saldo", "importe", "amount", "disponible"))})
    totals = {currency: _number(source.get(key)) for key, currency in (("totalEnPesos", "ARS"), ("totalEnDolares", "USD"))}
    return {"balances": balances, "reported_totals": {key: value for key, value in totals.items() if value is not None}}


def normalize_orders(payload: Any) -> list[dict[str, Any]]:
    return [{
        "number": _pick(row, "numero", "number", "id"),
        "symbol": _pick(row, "simbolo", "symbol", "especie"),
        "market": _pick(row, "mercado", "market", "pais"),
        "side": _pick(row, "tipo", "side", "operacion"),
        "status": _pick(row, "estado", "status"),
        "quantity": _number(_pick(row, "cantidad", "quantity")),
        "price": _number(_pick(row, "precio", "price")),
        "created_at": _pick(row, "fecha", "createdAt", "fechaOperacion"),
    } for row in _items(payload)]


def normalize_movements(payload: Any) -> list[dict[str, Any]]:
    return [{
        "date": _pick(row, "fecha", "date"),
        "type": _pick(row, "tipo", "type", "concepto"),
        "symbol": _pick(row, "simbolo", "symbol", "especie"),
        "currency": _pick(row, "moneda", "currency"),
        "amount": _number(_pick(row, "importe", "amount", "monto")),
    } for row in _items(payload)]


def stable_hash(document: dict[str, Any]) -> str:
    copy = dict(document)
    copy.pop("content_hash", None)
    encoded = json.dumps(copy, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def build_context(gateway: ReadOnlyIOLGateway, as_of: date | None = None, generated_at: datetime | None = None) -> dict[str, Any]:
    as_of = as_of or date.today()
    generated_at = generated_at or datetime.now(timezone.utc)
    warnings: list[str] = []
    sources: dict[str, dict[str, Any]] = {}
    result: dict[str, Any] = {"schema_version": SCHEMA_VERSION, "as_of": as_of.isoformat(), "generated_at": generated_at.isoformat(), "sources": sources, "warnings": warnings}
    calls = (("account_status", gateway.account_status), ("portfolio_argentina", lambda: gateway.portfolio("argentina")), ("portfolio_estados_unidos", lambda: gateway.portfolio("estados_unidos")), ("orders", gateway.orders))
    for name, call in calls:
        try:
            value = call()
            if name == "account_status":
                result[name] = normalize_account_status(value)
            elif name == "orders":
                result[name] = normalize_orders(value)
            else:
                result[name] = normalize_portfolio(value, name.removeprefix("portfolio_"))
            sources[name] = {"status": "available"}
        except Exception as exc:
            sources[name] = {"status": "unavailable", "error": type(exc).__name__}
            warnings.append(f"{name} unavailable")
    movements, movement_error = gateway.optional_movements(as_of)
    result["movements"] = normalize_movements(movements) if movements is not None else None
    sources["movements"] = {"status": "available" if movement_error is None else "unavailable"}
    if movement_error is not None:
        warnings.append("movements unavailable")
    result["content_hash"] = stable_hash(result)
    return result


def write_context(path: Path, context: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(context, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    temporary.replace(path)
