import json
import os
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Optional

PENDING_FILE = os.path.join("data", "pending_orders.json")


def _ensure_dir() -> None:
    dirname = os.path.dirname(PENDING_FILE)
    if dirname and not os.path.exists(dirname):
        os.makedirs(dirname, exist_ok=True)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def load_pending() -> Dict[str, Any]:
    _ensure_dir()
    if not os.path.exists(PENDING_FILE):
        return {"version": 1, "orders": {}}
    with open(PENDING_FILE, "r", encoding="utf-8") as f:
        try:
            data = json.load(f)
        except json.JSONDecodeError:
            return {"version": 1, "orders": {}}
    if "orders" not in data:
        data["orders"] = {}
    return data


def save_pending(data: Dict[str, Any]) -> None:
    _ensure_dir()
    with open(PENDING_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=True)


def add_pending(order: Dict[str, Any]) -> str:
    data = load_pending()
    confirmation_id = str(uuid.uuid4())
    order = dict(order)
    order["created_at"] = _now_iso()
    data["orders"][confirmation_id] = order
    save_pending(data)
    return confirmation_id


def get_pending(confirmation_id: str) -> Optional[Dict[str, Any]]:
    data = load_pending()
    return data.get("orders", {}).get(confirmation_id)


def remove_pending(confirmation_id: str) -> None:
    data = load_pending()
    if confirmation_id in data.get("orders", {}):
        del data["orders"][confirmation_id]
        save_pending(data)
