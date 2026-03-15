"""Base protocol and helpers shared across all engines."""
from __future__ import annotations

import sqlite3
from typing import Any, Optional


class BaseEngine:
    """Minimal contract every engine must satisfy.

    Each engine:
      - run()       → produce a signal, upsert it into the DB, return it.
      - load_latest() → return the most recent cached signal on or before *as_of*.
    """

    def run(self, as_of: str, conn: sqlite3.Connection, **kwargs: Any) -> Any:  # pragma: no cover
        raise NotImplementedError

    def load_latest(self, conn: sqlite3.Connection, as_of: str) -> Optional[Any]:  # pragma: no cover
        raise NotImplementedError
