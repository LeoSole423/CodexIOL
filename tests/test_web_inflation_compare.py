import os
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from iol_web import db as webdb
from iol_web.inflation_ar import InflationFetchResult
from iol_web.routes_api import compare_inflation


def _mk_db():
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute(
        """
        CREATE TABLE portfolio_snapshots (
          snapshot_date TEXT PRIMARY KEY,
          total_value REAL
        )
        """
    )
    conn.commit()
    return conn, path


def _cleanup(conn, path):
    conn.close()
    if os.path.exists(path):
        os.unlink(path)


class TestInflationCompare(unittest.TestCase):
    def test_monthly_first_last_series(self):
        conn, path = _mk_db()
        try:
            conn.executemany(
                "INSERT INTO portfolio_snapshots(snapshot_date,total_value) VALUES(?,?)",
                [
                    ("2026-01-02", 100.0),
                    ("2026-01-31", 110.0),
                    ("2026-02-10", 200.0),
                    ("2026-02-28", 220.0),
                ],
            )
            conn.commit()

            rows = webdb.monthly_first_last_series(conn, "2026-01-01", "2026-02-29")
            self.assertEqual(len(rows), 2)
            self.assertEqual(rows[0]["month"], "2026-01")
            self.assertEqual(rows[0]["first_date"], "2026-01-02")
            self.assertEqual(rows[0]["last_date"], "2026-01-31")
            self.assertAlmostEqual(rows[0]["first_value"], 100.0)
            self.assertAlmostEqual(rows[0]["last_value"], 110.0)
        finally:
            _cleanup(conn, path)

    def test_inflation_pct_by_month_decimal(self):
        res = InflationFetchResult(
            series_id="x",
            fetched_at=0.0,
            stale=False,
            data=[("2025-12-01", 0.0284527), ("2026-01-01", 0.10)],
            source="test",
        )
        m = res.inflation_pct_by_month()
        self.assertAlmostEqual(m["2025-12"], 2.84527, places=4)
        self.assertAlmostEqual(m["2026-01"], 10.0, places=6)

    def test_real_return_formula(self):
        # Example: portfolio +12%, inflation +10% => real ~= (1.12/1.10 - 1) = 1.81818%
        portfolio_pct = 12.0
        infl_pct = 10.0
        real = ((1.0 + portfolio_pct / 100.0) / (1.0 + infl_pct / 100.0) - 1.0) * 100.0
        self.assertAlmostEqual(real, 1.81818, places=4)

    def test_compare_inflation_projects_last_month(self):
        conn, path = _mk_db()
        try:
            # Two months of snapshots: Jan and Feb (current). Feb inflation will be missing in the mocked series.
            conn.executemany(
                "INSERT INTO portfolio_snapshots(snapshot_date,total_value) VALUES(?,?)",
                [
                    ("2026-01-02", 100.0),
                    ("2026-01-31", 110.0),
                    ("2026-02-06", 220.0),
                    ("2026-02-28", 242.0),
                ],
            )
            conn.commit()

            os.environ["IOL_DB_PATH"] = path

            mocked = InflationFetchResult(
                series_id="mock",
                fetched_at=0.0,
                stale=False,
                # Only Jan published: 10% (as decimal 0.10)
                data=[("2026-01-01", 0.10)],
                source="mock",
            )

            with patch("iol_web.routes_api.get_inflation_series", return_value=mocked):
                out = compare_inflation(2)

            rows = out.get("rows") or []
            self.assertEqual(len(rows), 2)
            jan = rows[0]
            feb = rows[1]
            self.assertEqual(jan["month"], "2026-01")
            self.assertEqual(feb["month"], "2026-02")
            self.assertFalse(jan.get("inflation_projected"))
            self.assertTrue(feb.get("inflation_projected"))
            self.assertAlmostEqual(feb.get("inflation_pct"), 10.0, places=6)
        finally:
            _cleanup(conn, path)
            if os.environ.get("IOL_DB_PATH") == path:
                del os.environ["IOL_DB_PATH"]


if __name__ == "__main__":
    unittest.main()
