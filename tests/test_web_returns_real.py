import os
import sqlite3
import tempfile
import unittest

from iol_web.routes_api import cashflows_manual, cashflows_manual_add, cashflows_manual_delete, returns


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
          cash_total_ars REAL,
          cash_disponible_ars REAL,
          retrieved_at TEXT
        )
        """
    )
    conn.execute(
        """
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
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE manual_cashflow_adjustments (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          flow_date TEXT NOT NULL,
          kind TEXT NOT NULL,
          amount_ars REAL NOT NULL,
          note TEXT,
          created_at TEXT NOT NULL
        )
        """
    )
    conn.commit()
    return conn, path


def _cleanup(conn, path):
    conn.close()
    if os.path.exists(path):
        os.unlink(path)


class TestWebReturnsReal(unittest.TestCase):
    def setUp(self):
        self._prev_db = os.environ.get("IOL_DB_PATH")

    def tearDown(self):
        if self._prev_db is None:
            os.environ.pop("IOL_DB_PATH", None)
        else:
            os.environ["IOL_DB_PATH"] = self._prev_db

    def test_returns_adjusts_external_deposit(self):
        conn, path = _mk_db()
        try:
            conn.executemany(
                """
                INSERT INTO portfolio_snapshots(snapshot_date,total_value,cash_total_ars,cash_disponible_ars)
                VALUES(?,?,?,?)
                """,
                [
                    ("2026-02-17", 100.0, 20.0, 20.0),
                    ("2026-02-18", 150.0, 70.0, 70.0),
                ],
            )
            conn.commit()
            os.environ["IOL_DB_PATH"] = path
            out = returns()
            daily = out["daily"]
            self.assertAlmostEqual(daily["delta"], 50.0)
            self.assertAlmostEqual(daily["flow_inferred_ars"], 50.0)
            self.assertAlmostEqual(daily["flow_manual_adjustment_ars"], 0.0)
            self.assertAlmostEqual(daily["flow_total_ars"], 50.0)
            self.assertAlmostEqual(daily["real_delta"], 0.0)
            self.assertAlmostEqual(daily["real_pct"], 0.0)
            self.assertIn("ORDERS_NONE", daily.get("quality_warnings") or [])
        finally:
            _cleanup(conn, path)

    def test_returns_internal_buy_is_not_external_flow(self):
        conn, path = _mk_db()
        try:
            conn.executemany(
                """
                INSERT INTO portfolio_snapshots(snapshot_date,total_value,cash_total_ars,cash_disponible_ars)
                VALUES(?,?,?,?)
                """,
                [
                    ("2026-02-18", 100.0, 50.0, 50.0),
                    ("2026-02-19", 100.0, 20.0, 20.0),
                ],
            )
            conn.execute(
                """
                INSERT INTO orders(order_number,status,symbol,side,side_norm,operated_amount,currency,operated_at)
                VALUES(?,?,?,?,?,?,?,?)
                """,
                (1, "terminada", "AAA", "Compra", "buy", 30.0, "peso_Argentino", "2026-02-19T11:00:00"),
            )
            conn.commit()
            os.environ["IOL_DB_PATH"] = path
            out = returns()
            daily = out["daily"]
            self.assertAlmostEqual(daily["delta"], 0.0)
            self.assertAlmostEqual(daily["flow_inferred_ars"], 0.0)
            self.assertAlmostEqual(daily["real_delta"], 0.0)
            self.assertAlmostEqual(daily["real_pct"], 0.0)
        finally:
            _cleanup(conn, path)

    def test_returns_manual_adjustment_is_added(self):
        conn, path = _mk_db()
        try:
            conn.executemany(
                """
                INSERT INTO portfolio_snapshots(snapshot_date,total_value,cash_total_ars,cash_disponible_ars)
                VALUES(?,?,?,?)
                """,
                [
                    ("2026-02-17", 100.0, 20.0, 20.0),
                    ("2026-02-18", 150.0, 70.0, 70.0),
                ],
            )
            conn.execute(
                """
                INSERT INTO manual_cashflow_adjustments(flow_date,kind,amount_ars,note,created_at)
                VALUES(?,?,?,?,?)
                """,
                ("2026-02-18", "correction", 10.0, "ajuste", "2026-02-18T22:00:00Z"),
            )
            conn.commit()
            os.environ["IOL_DB_PATH"] = path
            out = returns()
            daily = out["daily"]
            self.assertAlmostEqual(daily["flow_manual_adjustment_ars"], 10.0)
            self.assertAlmostEqual(daily["flow_total_ars"], 60.0)
            self.assertAlmostEqual(daily["real_delta"], -10.0)
            self.assertAlmostEqual(daily["real_pct"], -10.0)
        finally:
            _cleanup(conn, path)

    def test_manual_cashflow_endpoints(self):
        conn, path = _mk_db()
        try:
            conn.commit()
            os.environ["IOL_DB_PATH"] = path

            rows0 = cashflows_manual(None, None)
            self.assertEqual(rows0, [])

            row1 = cashflows_manual_add(
                {"flow_date": "2026-02-18", "kind": "deposit", "amount_ars": 100.0, "note": "aporte"}
            )
            self.assertAlmostEqual(float(row1["amount_ars"]), 100.0)

            row2 = cashflows_manual_add({"flow_date": "2026-02-19", "kind": "withdraw", "amount_ars": 25.0})
            self.assertAlmostEqual(float(row2["amount_ars"]), -25.0)

            rows3 = cashflows_manual(None, None)
            self.assertEqual(len(rows3), 2)

            out4 = cashflows_manual_delete(int(row1["id"]))
            self.assertTrue(bool(out4.get("ok")))

            rows5 = cashflows_manual(None, None)
            self.assertEqual(len(rows5), 1)
        finally:
            _cleanup(conn, path)


if __name__ == "__main__":
    unittest.main()
