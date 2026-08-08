import json
from datetime import datetime, timedelta, timezone

import pytest

from iol_cli.tickets import TicketError, TicketStore


def test_ticket_lifecycle(tmp_path):
    store = TicketStore(tmp_path, 30)
    ticket = store.create("buy", {"simbolo": "GGAL"}, {"total": 10}, False)
    assert store.get_executable(ticket["id"])["status"] == "prepared"
    store.mark_executing(ticket["id"])
    with pytest.raises(TicketError, match="no longer executable"):
        store.get_executable(ticket["id"])
    store.mark_executed(ticket["id"])
    with pytest.raises(TicketError, match="no longer executable"):
        store.get_executable(ticket["id"])


def test_missing_corrupt_and_expired(tmp_path):
    store = TicketStore(tmp_path, 30)
    with pytest.raises(TicketError, match="not found"):
        store.get_executable("missing")
    store.path.parent.mkdir(parents=True, exist_ok=True)
    store.path.write_text("not-json", encoding="utf-8")
    with pytest.raises(TicketError, match="corrupt"):
        store.get_executable("missing")
    store.path.write_text(json.dumps({"version": 1, "tickets": {"old": {"id": "old", "status": "prepared", "expires_at": (datetime.now(timezone.utc) - timedelta(minutes=1)).isoformat()}}}), encoding="utf-8")
    with pytest.raises(TicketError, match="expired"):
        store.get_executable("old")
