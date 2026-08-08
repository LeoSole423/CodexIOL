import json
import os
import sqlite3
import unittest
from unittest.mock import patch

from iol_web.inflation_ar import InflationFetchResult
from iol_web.routes_api import quality
from tests_support import cleanup_temp_sqlite_db, create_temp_sqlite_db


TEST_SCHEMA = """
CREATE TABLE portfolio_snapshots (
  snapshot_date TEXT PRIMARY KEY,
  total_value REAL,
  cash_total_ars REAL,
  cash_disponible_ars REAL,
  retrieved_at TEXT
);
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
CREATE TABLE account_cash_movements (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  movement_id TEXT,
  occurred_at TEXT,
  movement_date TEXT NOT NULL,
  currency TEXT NOT NULL,
  amount REAL NOT NULL,
  kind TEXT NOT NULL,
  description TEXT,
  source TEXT,
  raw_json TEXT,
  created_at TEXT NOT NULL
);
CREATE TABLE advisor_evidence (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  created_at TEXT NOT NULL,
  symbol TEXT NOT NULL,
  query TEXT NOT NULL,
  source_name TEXT NOT NULL,
  source_url TEXT NOT NULL,
  published_date TEXT,
  retrieved_at_utc TEXT NOT NULL,
  claim TEXT NOT NULL,
  confidence TEXT NOT NULL,
  date_confidence TEXT NOT NULL,
  notes TEXT,
  conflict_key TEXT
);
CREATE TABLE advisor_opportunity_runs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  created_at_utc TEXT NOT NULL,
  as_of TEXT NOT NULL,
  mode TEXT NOT NULL,
  universe TEXT NOT NULL,
  budget_ars REAL NOT NULL,
  top_n INTEGER NOT NULL,
  status TEXT NOT NULL,
  error_message TEXT,
  config_json TEXT NOT NULL,
  pipeline_warnings_json TEXT,
  run_metrics_json TEXT
);
"""

LEGACY_RUNS_SCHEMA = """
CREATE TABLE portfolio_snapshots (
  snapshot_date TEXT PRIMARY KEY,
  total_value REAL,
  cash_total_ars REAL,
  cash_disponible_ars REAL,
  retrieved_at TEXT
);
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
CREATE TABLE account_cash_movements (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  movement_id TEXT,
  occurred_at TEXT,
  movement_date TEXT NOT NULL,
  currency TEXT NOT NULL,
  amount REAL NOT NULL,
  kind TEXT NOT NULL,
  description TEXT,
  source TEXT,
  raw_json TEXT,
  created_at TEXT NOT NULL
);
CREATE TABLE advisor_evidence (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  created_at TEXT NOT NULL,
  symbol TEXT NOT NULL,
  query TEXT NOT NULL,
  source_name TEXT NOT NULL,
  source_url TEXT NOT NULL,
  published_date TEXT,
  retrieved_at_utc TEXT NOT NULL,
  claim TEXT NOT NULL,
  confidence TEXT NOT NULL,
  date_confidence TEXT NOT NULL,
  notes TEXT,
  conflict_key TEXT
);
CREATE TABLE advisor_opportunity_runs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  created_at_utc TEXT NOT NULL,
  as_of TEXT NOT NULL,
  mode TEXT NOT NULL,
  universe TEXT NOT NULL,
  budget_ars REAL NOT NULL,
  top_n INTEGER NOT NULL,
  status TEXT NOT NULL,
  error_message TEXT,
  config_json TEXT NOT NULL,
  pipeline_warnings_json TEXT
);
"""


class TestWebQualityApi(unittest.TestCase):
    def setUp(self):
        self._prev_db = os.environ.get("IOL_DB_PATH")

    def tearDown(self):
        if self._prev_db is None:
            os.environ.pop("IOL_DB_PATH", None)
        else:
            os.environ["IOL_DB_PATH"] = self._prev_db

    def test_quality_surfaces_cashflow_imports_and_scoring_health(self):
        conn, path = create_temp_sqlite_db(TEST_SCHEMA)
        try:
            conn.executemany(
                """
                INSERT INTO portfolio_snapshots(snapshot_date,total_value,cash_total_ars,cash_disponible_ars,retrieved_at)
                VALUES(?,?,?,?,?)
                """,
                [
                    ("2026-03-05", 100.0, 20.0, 20.0, "2026-03-05T20:00:00Z"),
                    ("2026-03-06", 110.0, 30.0, 30.0, "2026-03-06T20:00:00Z"),
                ],
            )
            conn.execute(
                """
                INSERT INTO account_cash_movements(
                  movement_id,occurred_at,movement_date,currency,amount,kind,description,source,raw_json,created_at
                ) VALUES(?,?,?,?,?,?,?,?,?,?)
                """,
                ("m1", "2026-03-06T11:00:00", "2026-03-06", "ARS", 10.0, "external_deposit", "aporte", "test", "{}", "2026-03-06T12:00:00Z"),
            )
            conn.execute(
                """
                INSERT INTO advisor_evidence(
                  created_at,symbol,query,source_name,source_url,published_date,retrieved_at_utc,claim,confidence,date_confidence,notes,conflict_key
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    "2026-03-06T12:00:00Z",
                    "AAPL",
                    "AAPL outlook",
                    "Reuters",
                    "https://www.reuters.com/aapl",
                    "2026-03-06",
                    "2026-03-06T12:00:00Z",
                    "Technology outlook stable",
                    "high",
                    "high",
                    json.dumps({"source_tier": "reuters", "stance": "bullish"}, ensure_ascii=True),
                    "AAPL:reuters",
                ),
            )
            conn.execute(
                """
                INSERT INTO advisor_opportunity_runs(
                  created_at_utc,as_of,mode,universe,budget_ars,top_n,status,error_message,config_json,pipeline_warnings_json,run_metrics_json
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    "2026-03-06T21:00:00Z",
                    "2026-03-06",
                    "both",
                    "bcba_cedears",
                    100000.0,
                    10,
                    "ok",
                    None,
                    "{}",
                    None,
                    json.dumps({"score_dispersion": 12.0, "fresh_evidence_ratio": 0.5}, ensure_ascii=True),
                ),
            )
            conn.commit()
            os.environ["IOL_DB_PATH"] = path

            mocked = InflationFetchResult(
                series_id="mock",
                fetched_at=0.0,
                stale=False,
                data=[("2026-03-01", 0.02)],
                source="mock",
            )
            with patch("iol_web.routes_api.get_inflation_series", return_value=mocked):
                out = quality()
            rows = {str(r.get("id")): r for r in (out.get("rows") or [])}
            self.assertIn("cashflow_imports", rows)
            self.assertEqual(rows["cashflow_imports"]["kind"], "ok")
            self.assertIn("scoring_health", rows)
            self.assertEqual(rows["scoring_health"]["kind"], "ok")
            self.assertIn("evidence_freshness", rows)
        finally:
            cleanup_temp_sqlite_db(conn, path)

    def test_quality_tolerates_legacy_runs_table_without_run_metrics_json(self):
        conn, path = create_temp_sqlite_db(LEGACY_RUNS_SCHEMA)
        try:
            conn.executemany(
                """
                INSERT INTO portfolio_snapshots(snapshot_date,total_value,cash_total_ars,cash_disponible_ars,retrieved_at)
                VALUES(?,?,?,?,?)
                """,
                [
                    ("2026-03-05", 100.0, 20.0, 20.0, "2026-03-05T20:00:00Z"),
                    ("2026-03-06", 110.0, 30.0, 30.0, "2026-03-06T20:00:00Z"),
                ],
            )
            conn.execute(
                """
                INSERT INTO advisor_opportunity_runs(
                  created_at_utc,as_of,mode,universe,budget_ars,top_n,status,error_message,config_json,pipeline_warnings_json
                ) VALUES(?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    "2026-03-06T21:00:00Z",
                    "2026-03-06",
                    "both",
                    "bcba_cedears",
                    100000.0,
                    10,
                    "ok",
                    None,
                    "{}",
                    None,
                ),
            )
            conn.commit()
            os.environ["IOL_DB_PATH"] = path

            mocked = InflationFetchResult(
                series_id="mock",
                fetched_at=0.0,
                stale=False,
                data=[("2026-03-01", 0.02)],
                source="mock",
            )
            with patch("iol_web.routes_api.get_inflation_series", return_value=mocked):
                out = quality()
            rows = {str(r.get("id")): r for r in (out.get("rows") or [])}
            self.assertIn("scoring_health", rows)
            self.assertIn(rows["scoring_health"]["kind"], {"warn", "info"})
        finally:
            cleanup_temp_sqlite_db(conn, path)


if __name__ == "__main__":
    unittest.main()
