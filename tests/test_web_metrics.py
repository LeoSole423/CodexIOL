import os
import sqlite3
import tempfile
import unittest

from iol_web import db as webdb
from iol_web.metrics import compute_return, target_date


def _mk_conn():
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute(
        """
        CREATE TABLE portfolio_snapshots (
          snapshot_date TEXT PRIMARY KEY,
          total_value REAL,
          currency TEXT,
          titles_value REAL,
          cash_disponible_ars REAL,
          cash_disponible_usd REAL
        )
        """
    )
    conn.commit()
    return conn, path


def _cleanup(conn: sqlite3.Connection, path: str) -> None:
    conn.close()
    if path and os.path.exists(path):
        os.unlink(path)


class TestWebMetrics(unittest.TestCase):
    def test_snapshot_selection_and_returns(self):
        conn, path = _mk_conn()
        try:
            rows = [
                ("2026-01-02", 100.0),
                ("2026-01-05", 110.0),
                ("2026-02-06", 200.0),
            ]
            conn.executemany("INSERT INTO portfolio_snapshots(snapshot_date,total_value) VALUES(?,?)", rows)
            conn.commit()

            latest = webdb.latest_snapshot(conn)
            self.assertEqual(latest.snapshot_date, "2026-02-06")
            prev = webdb.snapshot_before(conn, "2026-02-06")
            self.assertEqual(prev.snapshot_date, "2026-01-05")

            on_or_before = webdb.snapshot_on_or_before(conn, "2026-01-03")
            self.assertEqual(on_or_before.snapshot_date, "2026-01-02")

            base_ytd = webdb.first_snapshot_of_year(conn, 2026, "2026-02-06")
            self.assertEqual(base_ytd.snapshot_date, "2026-01-02")

            block = compute_return(latest, prev)
            self.assertEqual(block.from_date, "2026-01-05")
            self.assertEqual(block.to_date, "2026-02-06")
            self.assertAlmostEqual(block.delta, 90.0)
            self.assertAlmostEqual(block.pct, (200.0 - 110.0) / 110.0 * 100.0)
        finally:
            _cleanup(conn, path)

    def test_pct_none_when_base_zero(self):
        latest = webdb.Snapshot(snapshot_date="2026-02-06", total_value=10.0)
        base = webdb.Snapshot(snapshot_date="2026-02-05", total_value=0.0)
        block = compute_return(latest, base)
        self.assertEqual(block.delta, 10.0)
        self.assertIsNone(block.pct)

    def test_target_date(self):
        self.assertEqual(target_date("2026-02-06", 7), "2026-01-30")


if __name__ == "__main__":
    unittest.main()
