from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


class ConfigError(RuntimeError):
    pass


@dataclass(frozen=True)
class Config:
    username: str
    password: str
    base_url: str
    timeout: int
    commission_rate: float
    commission_min: float
    state_dir: Path
    ticket_ttl_minutes: int


def _integer(name: str, default: int) -> int:
    raw = os.getenv(name, str(default)).strip()
    try:
        value = int(raw)
    except ValueError as exc:
        raise ConfigError(f"{name} must be an integer") from exc
    if value <= 0:
        raise ConfigError(f"{name} must be greater than zero")
    return value


def _number(name: str, default: float) -> float:
    raw = os.getenv(name, str(default)).strip()
    try:
        value = float(raw)
    except ValueError as exc:
        raise ConfigError(f"{name} must be numeric") from exc
    if value < 0:
        raise ConfigError(f"{name} cannot be negative")
    return value


def load_config() -> Config:
    load_dotenv()
    username = os.getenv("IOL_USERNAME", "").strip()
    password = os.getenv("IOL_PASSWORD", "").strip()
    if not username or not password:
        raise ConfigError("IOL_USERNAME and IOL_PASSWORD are required")
    return Config(
        username=username,
        password=password,
        base_url=(os.getenv("IOL_API_URL", "https://api.invertironline.com").strip().rstrip("/")),
        timeout=_integer("IOL_TIMEOUT", 20),
        commission_rate=_number("IOL_COMMISSION_RATE", 0.0),
        commission_min=_number("IOL_COMMISSION_MIN", 0.0),
        state_dir=Path(os.getenv("IOL_STATE_DIR", "data/cli-state").strip()),
        ticket_ttl_minutes=_integer("IOL_TICKET_TTL_MINUTES", 30),
    )
