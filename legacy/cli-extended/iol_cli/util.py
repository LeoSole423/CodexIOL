from datetime import datetime, timedelta, timezone
from typing import Optional


def normalize_market(value: str) -> str:
    if not value:
        return value
    key = value.strip().lower()
    mapping = {
        "bcba": "bCBA",
        "nyse": "nYSE",
        "nasdaq": "nASDAQ",
        "amex": "aMEX",
        "bcs": "bCS",
        "rofx": "rOFX",
    }
    return mapping.get(key, value)


def normalize_country(value: str) -> str:
    if not value:
        return value
    key = value.strip().lower()
    mapping = {
        "ar": "argentina",
        "arg": "argentina",
        "argentina": "argentina",
        "usa": "estados_Unidos",
        "us": "estados_Unidos",
        "eeuu": "estados_Unidos",
        "estados_unidos": "estados_Unidos",
        "estados unidos": "estados_Unidos",
    }
    return mapping.get(key, value)


def normalize_plazo(value: str) -> str:
    if not value:
        return value
    key = value.strip().lower()
    mapping = {
        "ci": "t0",
        "t0": "t0",
        "t1": "t1",
        "t2": "t2",
        "t3": "t3",
        "24": "t1",
        "48": "t2",
    }
    return mapping.get(key, value)


def normalize_order_type(value: str) -> str:
    if not value:
        return value
    key = value.strip().lower()
    mapping = {
        "limit": "precioLimite",
        "limite": "precioLimite",
        "preciolimite": "precioLimite",
        "market": "precioMercado",
        "mercado": "precioMercado",
        "preciomercado": "precioMercado",
    }
    return mapping.get(key, value)


def default_valid_until() -> str:
    now = datetime.now(timezone.utc)
    end = now + timedelta(days=1)
    return end.isoformat()


def simulate_notional(quantity: Optional[float], price: Optional[float], amount: Optional[float],
                      commission_rate: float, commission_min: float, side: str) -> dict:
    notional = 0.0
    if amount is not None:
        notional = float(amount)
    elif quantity is not None and price is not None:
        notional = float(quantity) * float(price)
    commission = max(notional * commission_rate, commission_min) if notional > 0 else 0.0
    if side == "buy":
        total = notional + commission
    else:
        total = notional - commission
    return {
        "notional": notional,
        "commission": commission,
        "total": total,
    }
