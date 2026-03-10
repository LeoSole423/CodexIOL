import os
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from iol_web.inflation_ar import InflationFetchResult
from iol_web.routes_api import kpi_monthly_vs_inflation


def _mk_db():
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE portfolio_snapshots (
          snapshot_date TEXT PRIMARY KEY,
          total_value REAL,
          cash_total_ars REAL,
          cash_disponible_ars REAL
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
        CREATE TABLE manual_cashflow_adjustments (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          flow_date TEXT NOT NULL,
          kind TEXT NOT NULL,
          amount_ars REAL NOT NULL,
          note TEXT,
          created_at TEXT NOT NULL
        );
        """
    )
    conn.commit()
    return conn, path


def _cleanup(conn, path):
    conn.close()
    if os.path.exists(path):
        os.unlink(path)


class TestWebKpiMonthlyVsInflation(unittest.TestCase):
    def setUp(self):
        self._prev_db = os.environ.get("IOL_DB_PATH")

    def tearDown(self):
        if self._prev_db is None:
            os.environ.pop("IOL_DB_PATH", None)
        else:
            os.environ["IOL_DB_PATH"] = self._prev_db

    def test_month_ok_adjusted_by_flows_and_real_uses_net(self):
        conn, path = _mk_db()
        try:
            conn.executemany(
                """
                INSERT INTO portfolio_snapshots(snapshot_date,total_value,cash_total_ars,cash_disponible_ars)
                VALUES(?,?,?,?)
                """,
                [
                    ("2026-02-01", 100.0, 20.0, 20.0),
                    ("2026-02-20", 130.0, 50.0, 50.0),
                ],
            )
            conn.commit()
            os.environ["IOL_DB_PATH"] = path

            mocked = InflationFetchResult(
                series_id="mock",
                fetched_at=0.0,
                stale=False,
                data=[("2026-02-01", 0.10)],
                source="mock",
            )
            with patch("iol_web.routes_api.get_inflation_series", return_value=mocked):
                out = kpi_monthly_vs_inflation()

            self.assertEqual(out["status"], "ok")
            self.assertAlmostEqual(out["market_pct"], 30.0, places=6)
            self.assertAlmostEqual(out["contributions_ars"], 30.0, places=6)
            self.assertAlmostEqual(out["net_pct"], 0.0, places=6)
            self.assertNotEqual(out["market_pct"], out["net_pct"])
            self.assertAlmostEqual(out["inflation_pct"], 10.0, places=6)
            self.assertAlmostEqual(out["real_vs_inflation_pct"], -9.0909, places=3)
            self.assertFalse(bool(out["beats_inflation"]))
            self.assertEqual(out.get("flow_confidence"), "low")
            self.assertTrue(bool(out.get("estimated")))
        finally:
            _cleanup(conn, path)

    def test_month_projects_last_known_inflation(self):
        conn, path = _mk_db()
        try:
            conn.executemany(
                """
                INSERT INTO portfolio_snapshots(snapshot_date,total_value,cash_total_ars,cash_disponible_ars)
                VALUES(?,?,?,?)
                """,
                [
                    ("2026-02-01", 100.0, 20.0, 20.0),
                    ("2026-02-20", 100.0, 20.0, 20.0),
                ],
            )
            conn.commit()
            os.environ["IOL_DB_PATH"] = path

            mocked = InflationFetchResult(
                series_id="mock",
                fetched_at=0.0,
                stale=False,
                data=[("2026-01-01", 0.10)],
                source="mock",
            )
            with patch("iol_web.routes_api.get_inflation_series", return_value=mocked):
                out = kpi_monthly_vs_inflation()

            self.assertEqual(out["status"], "ok")
            self.assertTrue(bool(out["inflation_projected"]))
            self.assertAlmostEqual(out["inflation_pct"], 10.0, places=6)
            self.assertEqual(out["inflation_available_to"], "2026-01")
            self.assertIsNotNone(out.get("orders_coverage"))
        finally:
            _cleanup(conn, path)

    def test_month_insufficient_snapshots(self):
        conn, path = _mk_db()
        try:
            conn.execute(
                """
                INSERT INTO portfolio_snapshots(snapshot_date,total_value,cash_total_ars,cash_disponible_ars)
                VALUES(?,?,?,?)
                """,
                ("2026-02-20", 100.0, 20.0, 20.0),
            )
            conn.commit()
            os.environ["IOL_DB_PATH"] = path

            out = kpi_monthly_vs_inflation()
            self.assertEqual(out["status"], "insufficient_snapshots")
            self.assertIsNone(out["real_vs_inflation_pct"])
        finally:
            _cleanup(conn, path)

    def test_month_inflation_unavailable(self):
        conn, path = _mk_db()
        try:
            conn.executemany(
                """
                INSERT INTO portfolio_snapshots(snapshot_date,total_value,cash_total_ars,cash_disponible_ars)
                VALUES(?,?,?,?)
                """,
                [
                    ("2026-02-01", 100.0, 20.0, 20.0),
                    ("2026-02-20", 101.0, 20.0, 20.0),
                ],
            )
            conn.commit()
            os.environ["IOL_DB_PATH"] = path

            mocked = InflationFetchResult(
                series_id="mock",
                fetched_at=0.0,
                stale=False,
                data=[],
                source="mock",
            )
            with patch("iol_web.routes_api.get_inflation_series", return_value=mocked):
                out = kpi_monthly_vs_inflation()

            self.assertEqual(out["status"], "inflation_unavailable")
            self.assertIsNone(out["inflation_pct"])
            self.assertIsNone(out["real_vs_inflation_pct"])
            self.assertIsNone(out["beats_inflation"])
        finally:
            _cleanup(conn, path)

    def test_month_single_snapshot_uses_previous_month_base(self):
        conn, path = _mk_db()
        try:
            conn.executemany(
                """
                INSERT INTO portfolio_snapshots(snapshot_date,total_value,cash_total_ars,cash_disponible_ars)
                VALUES(?,?,?,?)
                """,
                [
                    ("2026-01-31", 100.0, 20.0, 20.0),
                    ("2026-02-20", 110.0, 20.0, 20.0),
                ],
            )
            conn.commit()
            os.environ["IOL_DB_PATH"] = path

            mocked = InflationFetchResult(
                series_id="mock",
                fetched_at=0.0,
                stale=False,
                data=[("2026-02-01", 0.05)],
                source="mock",
            )
            with patch("iol_web.routes_api.get_inflation_series", return_value=mocked):
                out = kpi_monthly_vs_inflation()

            self.assertEqual(out["status"], "ok")
            self.assertEqual(out["from"], "2026-01-31")
            self.assertEqual(out["to"], "2026-02-20")
            self.assertAlmostEqual(out["market_pct"], 10.0, places=6)
            self.assertAlmostEqual(out["net_pct"], 10.0, places=6)
        finally:
            _cleanup(conn, path)


if __name__ == "__main__":
    unittest.main()
