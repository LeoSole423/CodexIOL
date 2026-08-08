import unittest

from iol_web import db as webdb
from tests_support import cleanup_temp_sqlite_db, create_temp_sqlite_db


TEST_SCHEMA = """
CREATE TABLE portfolio_snapshots (
  snapshot_date TEXT PRIMARY KEY,
  total_value REAL,
  currency TEXT,
  titles_value REAL,
  cash_disponible_ars REAL,
  cash_disponible_usd REAL
);
"""


class TestWebRanges(unittest.TestCase):
    def test_first_last_snapshot_in_range(self):
        conn, path = create_temp_sqlite_db(TEST_SCHEMA)
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
            cleanup_temp_sqlite_db(conn, path)


if __name__ == "__main__":
    unittest.main()
