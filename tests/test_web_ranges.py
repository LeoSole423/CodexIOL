import os
import sqlite3
import tempfile
import unittest

from iol_web import db as webdb


def _mk_db():
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


def _cleanup(conn, path):
    conn.close()
    if os.path.exists(path):
        os.unlink(path)


class TestWebRanges(unittest.TestCase):
    def test_first_last_snapshot_in_range(self):
        conn, path = _mk_db()
        try:
            conn.executemany(
                "INSERT INTO portfolio_snapshots(snapshot_date,total_value) VALUES(?,?)",
                [
                    ("2026-02-01", 1.0),
                    ("2026-02-10", 2.0),
                    ("2026-02-28", 3.0),
                    ("2026-03-01", 4.0),
                ],
            )
            conn.commit()

            first = webdb.first_snapshot_in_range(conn, "2026-02-01", "2026-02-29")
            last = webdb.last_snapshot_in_range(conn, "2026-02-01", "2026-02-29")
            self.assertIsNotNone(first)
            self.assertIsNotNone(last)
            self.assertEqual(first.snapshot_date, "2026-02-01")
            self.assertEqual(last.snapshot_date, "2026-02-28")

            empty_first = webdb.first_snapshot_in_range(conn, "2026-04-01", "2026-04-30")
            empty_last = webdb.last_snapshot_in_range(conn, "2026-04-01", "2026-04-30")
            self.assertIsNone(empty_first)
            self.assertIsNone(empty_last)
        finally:
            _cleanup(conn, path)


if __name__ == "__main__":
    unittest.main()

