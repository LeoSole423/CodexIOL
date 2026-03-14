import os
import sqlite3
import tempfile
import unittest
from typing import Optional, Tuple


def create_temp_sqlite_db(schema_sql: Optional[str] = None) -> Tuple[sqlite3.Connection, str]:
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    if schema_sql:
        conn.executescript(schema_sql)
        conn.commit()
    return conn, path


def cleanup_temp_sqlite_db(conn: sqlite3.Connection, path: str) -> None:
    conn.close()
    if os.path.exists(path):
        os.unlink(path)


def base_cli_env(db_path: str) -> dict:
    """Return a copy of os.environ with IOL_* vars set for CLI tests."""
    env = os.environ.copy()
    env["IOL_USERNAME"] = "user"
    env["IOL_PASSWORD"] = "pass"
    env["IOL_DB_PATH"] = db_path
    env["IOL_API_URL"] = "https://api.invertironline.com"
    return env


# ---------------------------------------------------------------------------
# Canonical table schemas (superset of all columns used across tests).
# Extra nullable columns don't affect test behaviour.
# ---------------------------------------------------------------------------

SCHEMA_SNAPSHOTS = """
CREATE TABLE portfolio_snapshots (
  snapshot_date TEXT PRIMARY KEY,
  total_value REAL,
  currency TEXT,
  titles_value REAL,
  cash_total_ars REAL,
  cash_disponible_ars REAL,
  cash_disponible_usd REAL,
  retrieved_at TEXT
);
"""

SCHEMA_ASSETS = """
CREATE TABLE portfolio_assets (
  snapshot_date TEXT,
  symbol TEXT,
  description TEXT,
  market TEXT,
  type TEXT,
  currency TEXT,
  plazo TEXT,
  quantity REAL,
  last_price REAL,
  ppc REAL,
  total_value REAL,
  daily_var_pct REAL,
  daily_var_points REAL,
  gain_pct REAL,
  gain_amount REAL,
  committed REAL,
  raw_json TEXT,
  PRIMARY KEY (snapshot_date, symbol)
);
"""

SCHEMA_ORDERS = """
CREATE TABLE orders (
  order_number INTEGER PRIMARY KEY,
  status TEXT,
  symbol TEXT,
  side TEXT,
  side_norm TEXT,
  quantity REAL,
  price REAL,
  operated_amount REAL,
  currency TEXT,
  created_at TEXT,
  updated_at TEXT,
  operated_at TEXT
);
"""

SCHEMA_CASH_MOVEMENTS = """
CREATE TABLE account_cash_movements (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  movement_id TEXT,
  occurred_at TEXT,
  movement_date TEXT,
  currency TEXT,
  amount REAL,
  kind TEXT,
  description TEXT,
  source TEXT,
  raw_json TEXT,
  created_at TEXT
);
"""


# ---------------------------------------------------------------------------
# Base test case classes
# ---------------------------------------------------------------------------

class WebDbTestCase(unittest.TestCase):
    """Base for web API tests that need a temp SQLite DB with IOL_DB_PATH set.

    Subclasses set ``schema_sql`` (combine SCHEMA_* constants as needed) and
    use ``self.conn`` / ``self.path`` in their test methods.
    """

    schema_sql: str = ""

    def setUp(self):
        self.conn, self.path = create_temp_sqlite_db(self.schema_sql)
        self._prev_env = os.environ.get("IOL_DB_PATH")
        os.environ["IOL_DB_PATH"] = self.path

    def tearDown(self):
        if self._prev_env is None:
            os.environ.pop("IOL_DB_PATH", None)
        else:
            os.environ["IOL_DB_PATH"] = self._prev_env
        cleanup_temp_sqlite_db(self.conn, self.path)


class InitDbTestCase(unittest.TestCase):
    """Base for web API tests using a full CLI-initialized DB with IOL_DB_PATH set.

    Calls ``iol_cli.db.init_db`` so all production tables are present.
    Subclasses that need to seed data should call ``super().setUp()`` first,
    then open a connection via ``self.connect()``.
    """

    def setUp(self):
        from iol_cli.db import connect as _connect, init_db as _init_db
        self._prev_db = os.environ.get("IOL_DB_PATH")
        self._tmp = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self._tmp.name, "test.db")
        conn = _connect(self.db_path)
        _init_db(conn)
        conn.close()
        os.environ["IOL_DB_PATH"] = self.db_path

    def tearDown(self):
        if self._prev_db is None:
            os.environ.pop("IOL_DB_PATH", None)
        else:
            os.environ["IOL_DB_PATH"] = self._prev_db
        self._tmp.cleanup()

    def connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn
