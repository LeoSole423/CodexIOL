from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Optional


def normalize_market(value: str) -> str:
    mapping = {"bcba": "bCBA", "nyse": "nYSE", "nasdaq": "nASDAQ", "amex": "aMEX", "bcs": "bCS", "rofx": "rOFX"}
    return mapping.get(value.strip().lower(), value.strip())


def normalize_country(value: str) -> str:
    mapping = {"ar": "argentina", "arg": "argentina", "argentina": "argentina", "usa": "estados_Unidos", "us": "estados_Unidos", "eeuu": "estados_Unidos", "estados_unidos": "estados_Unidos", "estados unidos": "estados_Unidos"}
    return mapping.get(value.strip().lower(), value.strip())


def normalize_plazo(value: str) -> str:
    mapping = {"ci": "t0", "t0": "t0", "t1": "t1", "t2": "t2", "t3": "t3", "24": "t1", "48": "t2"}
    result = mapping.get(value.strip().lower())
    if result is None:
        raise ValueError("plazo must be ci, t0, t1, t2 or t3")
    return result


def normalize_order_type(value: str) -> str:
    mapping = {"limit": "precioLimite", "limite": "precioLimite", "preciolimite": "precioLimite", "market": "precioMercado", "mercado": "precioMercado", "preciomercado": "precioMercado"}
    result = mapping.get(value.strip().lower())
    if result is None:
        raise ValueError("order type must be limit or market")
    return result


def default_valid_until() -> str:
    return (datetime.now(timezone.utc) + timedelta(days=1)).isoformat()


def simulate_notional(quantity: Optional[float], price: Optional[float], amount: Optional[float], commission_rate: float, commission_min: float, side: str) -> dict:
    notional = float(amount) if amount is not None else float(quantity or 0) * float(price or 0)
    commission = max(notional * commission_rate, commission_min) if notional > 0 else 0.0
    return {"notional": notional, "commission": commission, "total": notional + commission if side == "buy" else notional - commission}
