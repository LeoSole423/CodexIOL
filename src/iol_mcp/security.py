from __future__ import annotations

import re
from pathlib import Path
from typing import Any


FORBIDDEN_TOOL_TERMS = (
    "buy", "sell", "execute", "cancel", "prepare", "subscribe", "redeem",
    "transfer", "post", "put", "patch", "delete", "raw_request", "api_request",
)
SENSITIVE_KEYS = {
    "access_token", "refresh_token", "authorization", "cookie", "cookies", "password",
    "username", "iol_username", "iol_password", "dni", "cuit", "cuil", "email",
    "address", "domicilio", "account_number", "numero_cuenta", "account_id", "continuity_document",
}
SENSITIVE_KEY_NORMALIZED = {re.sub(r"[^a-z0-9]", "", key) for key in SENSITIVE_KEYS}
SENSITIVE_TEXT = re.compile(
    r"(?i)(bearer\s+[a-z0-9._~+/=-]+|(?:access|refresh)[ _-]?token\s*[:=]\s*\S+|"
    r"(?:iol_)?(?:username|password)\s*[:=]\s*\S+|[\w.+-]+@[\w.-]+\.[a-z]{2,})"
)


def sanitize_text(value: str, limit: int = 4_000) -> str:
    """Keep investment memory useful without forwarding obvious secrets or PII."""
    kept: list[str] = []
    for line in value.splitlines():
        lowered = line.lower()
        if any(key in lowered for key in SENSITIVE_KEYS):
            continue
        kept.append(SENSITIVE_TEXT.sub("[redacted]", line))
    return "\n".join(kept)[:limit]


def sanitize(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            str(key): sanitize(item)
            for key, item in value.items()
            if re.sub(r"[^a-z0-9]", "", str(key).lower()) not in SENSITIVE_KEY_NORMALIZED
        }
    if isinstance(value, list):
        return [sanitize(item) for item in value]
    if isinstance(value, tuple):
        return [sanitize(item) for item in value]
    if isinstance(value, str):
        return SENSITIVE_TEXT.sub("[redacted]", value)
    return value


def safe_document_path(root: Path, candidate: str) -> Path | None:
    """Resolve a relative document only when it remains in the approved root."""
    try:
        resolved_root = root.resolve()
        resolved = (Path.cwd() / candidate).resolve()
        resolved.relative_to(resolved_root)
    except (OSError, ValueError):
        return None
    return resolved
