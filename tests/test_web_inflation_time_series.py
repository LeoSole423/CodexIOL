import os
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from iol_web.inflation_ar import InflationFetchResult
from iol_web.inflation_compare import compounded_inflation_pct, inflation_factor_for_date
from iol_web.routes_api import compare_inflation_annual, compare_inflation_series


def _mk_db():
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE portfolio_snapshots (
          snapshot_date TEXT PRIMARY KEY,
          total_value REAL
        );
        """
    )
    conn.commit()
    return conn, path


def _cleanup(conn, path):
    conn.close()
    if os.path.exists(path):
        os.unlink(path)


class TestInflationCompareHelpers(unittest.TestCase):
    def test_compounded_inflation_pct_months_excludes_base_month(self):
        infl = {"2026-02": 10.0, "2026-03": 10.0}
        pct, used, projected = compounded_inflation_pct("2026-01-15", "2026-03-02", infl)
        self.assertEqual(used, ["2026-02", "2026-03"])
        self.assertEqual(projected, [])
        # 1.1 * 1.1 - 1 = 0.21
        self.assertAlmostEqual(pct, 21.0, places=6)

    def test_inflation_factor_for_date(self):
        infl = {"2026-02": 10.0}
        f = inflation_factor_for_date("2026-01-10", "2026-02-10", infl)
        self.assertAlmostEqual(f, 1.1, places=6)


class TestInflationSeriesAndAnnual(unittest.TestCase):
    def test_series_base100(self):
        conn, path = _mk_db()
        try:
            conn.executemany(
                "INSERT INTO portfolio_snapshots(snapshot_date,total_value) VALUES(?,?)",
                [
                    ("2026-01-10", 100.0),
                    ("2026-02-10", 110.0),
                    ("2026-03-10", 121.0),
                ],
            )
            conn.commit()

            os.environ["IOL_DB_PATH"] = path
            mocked = InflationFetchResult(
                series_id="mock",
                fetched_at=0.0,
                stale=False,
                data=[
                    ("2026-02-01", 0.10),  # 10%
                    ("2026-03-01", 0.10),  # 10%
                ],
                source="mock",
            )
            with patch("iol_web.routes_api.get_inflation_series", return_value=mocked):
                out = compare_inflation_series(None, None)

            self.assertEqual(out["labels"], ["2026-01-10", "2026-02-10", "2026-03-10"])
            # Portfolio index should be 100, 110, 121
            self.assertAlmostEqual(out["portfolio_index"][0], 100.0, places=6)
            self.assertAlmostEqual(out["portfolio_index"][1], 110.0, places=6)
            self.assertAlmostEqual(out["portfolio_index"][2], 121.0, places=6)
            # Inflation index should match (base 100, then Feb and Mar compounded)
            self.assertAlmostEqual(out["inflation_index"][0], 100.0, places=6)
            self.assertAlmostEqual(out["inflation_index"][1], 110.0, places=6)
            self.assertAlmostEqual(out["inflation_index"][2], 121.0, places=6)
        finally:
            _cleanup(conn, path)
            if os.environ.get("IOL_DB_PATH") == path:
                del os.environ["IOL_DB_PATH"]

    def test_annual_includes_ytd(self):
        conn, path = _mk_db()
        try:
            conn.executemany(
                "INSERT INTO portfolio_snapshots(snapshot_date,total_value) VALUES(?,?)",
                [
                    ("2025-02-01", 100.0),
                    ("2025-12-31", 120.0),
                    ("2026-01-02", 120.0),
                    ("2026-02-10", 110.0),
                ],
            )
            conn.commit()
            os.environ["IOL_DB_PATH"] = path

            mocked = InflationFetchResult(
                series_id="mock",
                fetched_at=0.0,
                stale=False,
                data=[
                    ("2025-03-01", 0.10),  # 10% (for 2025-02->2025-12, missing months will make inflation_pct None)
                    ("2026-02-01", 0.10),
                ],
                source="mock",
            )
            with patch("iol_web.routes_api.get_inflation_series", return_value=mocked):
                out = compare_inflation_annual(10)

            rows = out["rows"]
            self.assertTrue(any(r["label"].startswith("YTD") for r in rows))
        finally:
            _cleanup(conn, path)
            if os.environ.get("IOL_DB_PATH") == path:
                del os.environ["IOL_DB_PATH"]


if __name__ == "__main__":
    unittest.main()

