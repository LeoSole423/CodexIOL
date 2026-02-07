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
    conn.execute(
        """
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
        )
        """
    )
    conn.commit()
    return conn, path


def _cleanup(conn, path):
    conn.close()
    if os.path.exists(path):
        os.unlink(path)


class TestWebMoversPeriod(unittest.TestCase):
    def test_assets_delta_between_snapshots(self):
        conn, path = _mk_db()
        try:
            conn.executemany(
                "INSERT INTO portfolio_snapshots(snapshot_date,total_value) VALUES(?,?)",
                [("2026-02-05", 1000.0), ("2026-02-06", 1100.0)],
            )
            conn.executemany(
                """
                INSERT INTO portfolio_assets(snapshot_date,symbol,description,total_value)
                VALUES(?,?,?,?)
                """,
                [
                    ("2026-02-05", "AAA", "A", 100.0),
                    ("2026-02-05", "BBB", "B", 200.0),
                    ("2026-02-06", "AAA", "A", 130.0),
                    ("2026-02-06", "BBB", "B", 150.0),
                    ("2026-02-06", "CCC", "C", 10.0),
                ],
            )
            conn.commit()

            latest = webdb.latest_snapshot(conn)
            base = webdb.snapshot_before(conn, latest.snapshot_date)
            latest_assets = webdb.assets_for_snapshot(conn, latest.snapshot_date)
            base_assets = webdb.assets_for_snapshot(conn, base.snapshot_date)
            base_map = {a["symbol"]: a for a in base_assets}

            def delta(sym):
                cur = next(a for a in latest_assets if a["symbol"] == sym)
                base_val = float(base_map.get(sym, {}).get("total_value") or 0.0)
                return float(cur.get("total_value") or 0.0) - base_val

            self.assertAlmostEqual(delta("AAA"), 30.0)
            self.assertAlmostEqual(delta("BBB"), -50.0)
            self.assertAlmostEqual(delta("CCC"), 10.0)
        finally:
            _cleanup(conn, path)


if __name__ == "__main__":
    unittest.main()
