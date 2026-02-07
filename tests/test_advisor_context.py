import os
import sqlite3
import tempfile
import unittest

from iol_cli.advisor_context import build_advisor_context, compute_return


def _mk_conn():
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE portfolio_snapshots (
          snapshot_date TEXT PRIMARY KEY,
          total_value REAL,
          currency TEXT,
          retrieved_at TEXT,
          minutes_from_close INTEGER,
          source TEXT,
          titles_value REAL,
          cash_disponible_ars REAL,
          cash_disponible_usd REAL
        );
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
          PRIMARY KEY (snapshot_date, symbol)
        );
        CREATE TABLE orders (
          order_number INTEGER PRIMARY KEY,
          status TEXT,
          symbol TEXT,
          market TEXT,
          side TEXT,
          quantity REAL,
          price REAL,
          plazo TEXT,
          order_type TEXT,
          created_at TEXT,
          updated_at TEXT
        );
        """
    )
    conn.commit()
    return conn, path


def _cleanup(conn: sqlite3.Connection, path: str) -> None:
    conn.close()
    if path and os.path.exists(path):
        os.unlink(path)


class TestAdvisorContext(unittest.TestCase):
    def test_compute_return_pct_none_when_base_zero(self):
        latest = {"snapshot_date": "2026-02-06", "total_value": 10.0}
        base = {"snapshot_date": "2026-02-05", "total_value": 0.0}
        block = compute_return(latest, base)
        d = block.to_dict()
        self.assertEqual(d["delta_ars"], 10.0)
        self.assertIsNone(d["pct"])

    def test_as_of_snapshot_selection(self):
        conn, path = _mk_conn()
        try:
            conn.executemany(
                "INSERT INTO portfolio_snapshots(snapshot_date,total_value,currency) VALUES(?,?,?)",
                [
                    ("2026-01-02", 100.0, "peso_Argentino"),
                    ("2026-01-05", 110.0, "peso_Argentino"),
                    ("2026-02-06", 200.0, "peso_Argentino"),
                ],
            )
            conn.executemany(
                """
                INSERT INTO portfolio_assets(snapshot_date,symbol,description,total_value,daily_var_pct,gain_amount)
                VALUES(?,?,?,?,?,?)
                """,
                [
                    ("2026-02-06", "AAA", "A", 120.0, 10.0, 30.0),
                    ("2026-02-06", "BBB", "B", 80.0, -5.0, -10.0),
                ],
            )
            conn.commit()

            ctx = build_advisor_context(conn, as_of="2026-01-06", limit=5, history_days=30)
            self.assertEqual(ctx["as_of"], "2026-01-05")
            self.assertEqual(ctx["snapshot"]["total_value_ars"], 110.0)
        finally:
            _cleanup(conn, path)

    def test_movers_daily_uses_daily_var_pct(self):
        conn, path = _mk_conn()
        try:
            conn.executemany(
                "INSERT INTO portfolio_snapshots(snapshot_date,total_value,currency) VALUES(?,?,?)",
                [("2026-02-06", 200.0, "peso_Argentino")],
            )
            conn.executemany(
                """
                INSERT INTO portfolio_assets(snapshot_date,symbol,description,total_value,daily_var_pct,gain_amount)
                VALUES(?,?,?,?,?,?)
                """,
                [
                    ("2026-02-06", "AAA", "A", 100.0, 10.0, 0.0),
                    ("2026-02-06", "BBB", "B", 100.0, -10.0, 0.0),
                ],
            )
            conn.commit()

            ctx = build_advisor_context(conn, as_of=None, limit=10, history_days=10)
            daily = ctx["movers"]["daily"]
            self.assertEqual(daily["from"], "2026-02-06")
            gainers = daily["gainers"]
            losers = daily["losers"]
            self.assertEqual(gainers[0]["symbol"], "AAA")
            self.assertAlmostEqual(gainers[0]["delta_value"], 10.0)
            self.assertEqual(losers[0]["symbol"], "BBB")
            self.assertAlmostEqual(losers[0]["delta_value"], -10.0)
        finally:
            _cleanup(conn, path)

    def test_movers_weekly_union_adds_missing_symbols(self):
        conn, path = _mk_conn()
        try:
            conn.executemany(
                "INSERT INTO portfolio_snapshots(snapshot_date,total_value,currency) VALUES(?,?,?)",
                [
                    ("2026-01-30", 100.0, "peso_Argentino"),
                    ("2026-02-06", 200.0, "peso_Argentino"),
                ],
            )
            conn.executemany(
                "INSERT INTO portfolio_assets(snapshot_date,symbol,description,total_value) VALUES(?,?,?,?)",
                [
                    ("2026-01-30", "AAA", "A", 100.0),
                    ("2026-02-06", "BBB", "B", 50.0),
                ],
            )
            conn.commit()

            ctx = build_advisor_context(conn, as_of=None, limit=10, history_days=30)
            weekly = ctx["movers"]["weekly"]
            syms = {r["symbol"] for r in (weekly["gainers"] + weekly["losers"])}
            self.assertIn("AAA", syms)
            self.assertIn("BBB", syms)
        finally:
            _cleanup(conn, path)


if __name__ == "__main__":
    unittest.main()

