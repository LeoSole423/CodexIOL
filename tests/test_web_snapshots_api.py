import os
import sqlite3
import tempfile
import unittest

from fastapi.responses import JSONResponse

from iol_web.routes_api import snapshots


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
          cash_total_ars REAL,
          cash_disponible_ars REAL
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
    conn.commit()
    return conn, path


def _cleanup(conn, path):
    conn.close()
    if os.path.exists(path):
        os.unlink(path)


class TestWebSnapshotsApi(unittest.TestCase):
    def setUp(self):
        self.conn, self.path = _mk_db()
        self.prev_env = os.environ.get("IOL_DB_PATH")
        os.environ["IOL_DB_PATH"] = self.path

    def tearDown(self):
        if self.prev_env is None:
            os.environ.pop("IOL_DB_PATH", None)
        else:
            os.environ["IOL_DB_PATH"] = self.prev_env
        _cleanup(self.conn, self.path)

    def test_raw_mode_unchanged(self):
        self.conn.executemany(
            "INSERT INTO portfolio_snapshots(snapshot_date,total_value,cash_disponible_ars) VALUES(?,?,?)",
            [
                ("2026-02-10", 1000.0, 100.0),
                ("2026-02-11", 1500.0, 600.0),
                ("2026-02-12", 1600.0, 600.0),
            ],
        )
        self.conn.commit()

        out = snapshots(date_from=None, date_to=None, mode="raw")
        self.assertEqual([r["date"] for r in out], ["2026-02-10", "2026-02-11", "2026-02-12"])
        self.assertEqual([float(r["total_value"]) for r in out], [1000.0, 1500.0, 1600.0])

    def test_market_mode_adjusts_external_flows(self):
        self.conn.executemany(
            "INSERT INTO portfolio_snapshots(snapshot_date,total_value,cash_disponible_ars) VALUES(?,?,?)",
            [
                ("2026-02-10", 1000.0, 100.0),
                ("2026-02-11", 1500.0, 600.0),  # +500 (deposit inferred)
                ("2026-02-12", 1600.0, 600.0),  # +100 market
            ],
        )
        self.conn.commit()

        out = snapshots(date_from=None, date_to=None, mode="market")
        self.assertEqual([r["date"] for r in out], ["2026-02-10", "2026-02-11", "2026-02-12"])
        adjusted = [float(r["total_value"]) for r in out]
        self.assertAlmostEqual(adjusted[0], 1000.0)
        self.assertAlmostEqual(adjusted[1], 1000.0)
        self.assertAlmostEqual(adjusted[2], 1100.0)

        self.assertAlmostEqual(float(out[1]["raw_total_value"]), 1500.0)
        self.assertAlmostEqual(float(out[2]["raw_total_value"]), 1600.0)
        self.assertIn("ORDERS_NONE", out[1].get("quality_warnings") or [])

    def test_market_mode_uses_cash_total_to_avoid_settlement_spike(self):
        self.conn.executemany(
            "INSERT INTO portfolio_snapshots(snapshot_date,total_value,cash_total_ars,cash_disponible_ars) VALUES(?,?,?,?)",
            [
                ("2026-02-22", 1000.0, 100.0, 100.0),
                # External deposit +500 and buy +498 in the same day.
                ("2026-02-23", 1500.0, 102.0, 600.0),
                # Next day available cash settles -500 with no new orders.
                ("2026-02-24", 1510.0, 102.0, 100.0),
            ],
        )
        self.conn.execute(
            """
            INSERT INTO orders(order_number,status,symbol,side,side_norm,operated_amount,currency,operated_at)
            VALUES(?,?,?,?,?,?,?,?)
            """,
            (1, "terminada", "AAA", "Compra", "buy", 498.0, "peso_Argentino", "2026-02-23T11:00:00"),
        )
        self.conn.commit()

        out = snapshots(date_from=None, date_to=None, mode="market")
        self.assertEqual([r["date"] for r in out], ["2026-02-22", "2026-02-23", "2026-02-24"])
        adjusted = [float(r["total_value"]) for r in out]
        self.assertAlmostEqual(adjusted[0], 1000.0)
        self.assertAlmostEqual(adjusted[1], 1000.0)
        self.assertAlmostEqual(adjusted[2], 1010.0)
        self.assertAlmostEqual(float(out[1]["applied_flow_ars"]), 500.0)
        self.assertAlmostEqual(float(out[2]["applied_flow_ars"]), 0.0)

    def test_market_mode_smooths_settlement_carryover_pair(self):
        self.conn.executemany(
            "INSERT INTO portfolio_snapshots(snapshot_date,total_value,cash_total_ars,cash_disponible_ars) VALUES(?,?,?,?)",
            [
                ("2026-02-22", 1000.0, 100.0, 100.0),
                # Day 1: deposit + buy reported together.
                ("2026-02-23", 1490.0, 600.0, 600.0),
                # Day 2: cash liquidation carryover.
                ("2026-02-24", 1500.0, 100.0, 100.0),
            ],
        )
        self.conn.execute(
            """
            INSERT INTO orders(order_number,status,symbol,side,side_norm,operated_amount,currency,operated_at)
            VALUES(?,?,?,?,?,?,?,?)
            """,
            (2, "terminada", "AAA", "Compra", "buy", 498.0, "peso_Argentino", "2026-02-23T11:00:00"),
        )
        self.conn.commit()

        out = snapshots(date_from=None, date_to=None, mode="market")
        by_date = {r["date"]: r for r in out}
        r23 = by_date["2026-02-23"]
        r24 = by_date["2026-02-24"]

        self.assertAlmostEqual(float(r23["flow_total_ars"]), 498.0, places=6)
        self.assertAlmostEqual(float(r23["applied_flow_ars"]), 498.0, places=6)
        self.assertEqual(r23.get("display_kind"), "external_flow_probable")
        self.assertEqual(r23.get("reason_code"), "SETTLEMENT_SMOOTHED")

        self.assertAlmostEqual(float(r24["flow_total_ars"]), 0.0, places=6)
        self.assertAlmostEqual(float(r24["applied_flow_ars"]), 0.0, places=6)
        self.assertEqual(r24.get("display_kind"), "correction")
        self.assertEqual(r24.get("reason_code"), "SETTLEMENT_CARRYOVER")

        self.assertAlmostEqual(float(r23["total_value"]), 992.0, places=6)
        self.assertAlmostEqual(float(r24["total_value"]), 1002.0, places=6)

    def test_invalid_mode_returns_400(self):
        out = snapshots(date_from=None, date_to=None, mode="weird")
        self.assertIsInstance(out, JSONResponse)
        self.assertEqual(out.status_code, 400)


if __name__ == "__main__":
    unittest.main()
