from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path


def record_decision(path: Path, review_id: str, status: str, notes: str) -> Path:
    if status not in {"accepted", "rejected", "modified"}:
        raise ValueError("status must be accepted, rejected, or modified")
    if not review_id.strip() or not notes.strip():
        raise ValueError("review id and notes are required")
    path.parent.mkdir(parents=True, exist_ok=True)
    record = {"review_id": review_id, "status": status, "notes": notes, "recorded_at": datetime.now(timezone.utc).isoformat()}
    path.write_text(json.dumps(record, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return path
