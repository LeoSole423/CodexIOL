from __future__ import annotations

import os
import tomllib
from pathlib import Path
from typing import Any


REQUIRED_FIELDS = ("objectives", "horizon", "liquidity_needs", "loss_tolerance", "allowed_markets", "allowed_instruments")


class ProfileError(RuntimeError):
    pass


def profile_path() -> Path:
    return Path(os.getenv("IOL_REVIEW_PROFILE_PATH", "config/investor-profile.toml"))


def load_profile(path: Path | None = None) -> dict[str, Any]:
    target = path or profile_path()
    try:
        with target.open("rb") as source:
            profile = tomllib.load(source)
    except FileNotFoundError as exc:
        raise ProfileError(f"Investor profile not found: {target}") from exc
    except tomllib.TOMLDecodeError as exc:
        raise ProfileError(f"Invalid investor profile: {exc}") from exc
    validate_profile(profile)
    return profile


def validate_profile(profile: dict[str, Any]) -> None:
    missing = [name for name in REQUIRED_FIELDS if not profile.get(name) or (isinstance(profile[name], str) and not profile[name].strip())]
    if missing:
        raise ProfileError("Missing required profile fields: " + ", ".join(missing))
    if not isinstance(profile["allowed_markets"], list) or not profile["allowed_markets"]:
        raise ProfileError("allowed_markets must be a non-empty list")
    if not isinstance(profile["allowed_instruments"], list) or not profile["allowed_instruments"]:
        raise ProfileError("allowed_instruments must be a non-empty list")
    if profile.get("schema_version", 1) != 1:
        raise ProfileError("Unsupported investor profile schema_version")
