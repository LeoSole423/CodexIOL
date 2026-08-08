from __future__ import annotations

import json
import os
import tempfile
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict


class TicketError(RuntimeError):
    pass


class TicketStore:
    def __init__(self, state_dir: Path, ttl_minutes: int):
        self.state_dir = state_dir
        self.path = state_dir / "order-tickets.json"
        self.ttl_minutes = ttl_minutes

    def _load(self) -> Dict[str, Any]:
        if not self.path.exists():
            return {"version": 1, "tickets": {}}
        try:
            data = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise TicketError("ticket store is unreadable or corrupt") from exc
        if not isinstance(data.get("tickets"), dict):
            raise TicketError("ticket store has an invalid format")
        return data

    def _save(self, data: Dict[str, Any]) -> None:
        self.state_dir.mkdir(parents=True, exist_ok=True)
        fd, temp_name = tempfile.mkstemp(prefix="tickets-", suffix=".tmp", dir=self.state_dir)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as stream:
                json.dump(data, stream, ensure_ascii=True, indent=2)
                stream.flush()
                os.fsync(stream.fileno())
            try: os.chmod(temp_name, 0o600)
            except OSError: pass
            os.replace(temp_name, self.path)
        finally:
            if os.path.exists(temp_name): os.unlink(temp_name)

    def create(self, operation: str, payload: dict, summary: dict, especie_d: bool) -> dict:
        now = datetime.now(timezone.utc)
        ticket = {"id": str(uuid.uuid4()), "operation": operation, "payload": payload, "summary": summary, "especie_d": especie_d, "created_at": now.isoformat(), "expires_at": (now + timedelta(minutes=self.ttl_minutes)).isoformat(), "status": "prepared"}
        data = self._load()
        data["tickets"][ticket["id"]] = ticket
        self._save(data)
        return ticket

    def get_executable(self, ticket_id: str) -> dict:
        ticket = self._load()["tickets"].get(ticket_id)
        if not ticket: raise TicketError("ticket not found")
        if ticket.get("status") != "prepared": raise TicketError("ticket is no longer executable")
        if datetime.fromisoformat(ticket["expires_at"]) <= datetime.now(timezone.utc): raise TicketError("ticket expired")
        return ticket

    def mark_executing(self, ticket_id: str) -> None:
        data = self._load()
        ticket = data["tickets"].get(ticket_id)
        if not ticket: raise TicketError("ticket not found")
        if ticket.get("status") != "prepared": raise TicketError("ticket is no longer executable")
        ticket["status"] = "executing"
        ticket["execution_started_at"] = datetime.now(timezone.utc).isoformat()
        self._save(data)

    def mark_executed(self, ticket_id: str) -> None:
        data = self._load()
        ticket = data["tickets"].get(ticket_id)
        if not ticket: raise TicketError("ticket not found")
        ticket["status"] = "executed"
        ticket["executed_at"] = datetime.now(timezone.utc).isoformat()
        self._save(data)
