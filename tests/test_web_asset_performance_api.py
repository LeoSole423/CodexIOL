import os
import sqlite3
import tempfile
import unittest

from fastapi.responses import JSONResponse

from iol_web.routes_api import assets_performance


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
          PRIMARY KEY (snapshot_date, symbol)
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


class TestAssetPerformanceApi(unittest.TestCase):
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

    def _seed_two_snapshots(self):
        self.conn.executemany(
            "INSERT INTO portfolio_snapshots(snapshot_date,total_value) VALUES(?,?)",
            [("2026-02-06", 1000.0), ("2026-02-13", 900.0)],
        )
        self.conn.executemany(
            """
            INSERT INTO portfolio_assets(
              snapshot_date,symbol,description,market,type,currency,plazo,quantity,last_price,ppc,total_value,
              daily_var_pct,daily_var_points,gain_pct,gain_amount,committed
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            [
                ("2026-02-06", "AAA", "Asset A", "bcba", "ACCIONES", "peso_Argentino", "t1", 1.0, 100.0, 100.0, 100.0, 0.0, 0.0, 0.0, 0.0, 0.0),
                ("2026-02-13", "AAA", "Asset A", "bcba", "ACCIONES", "peso_Argentino", "t1", 1.0, 120.0, 100.0, 120.0, 1.0, 1.2, 20.0, 20.0, 0.0),
                ("2026-02-13", "BBB", "Asset B", "nyse", "CEDEAR", "dolar_Estadounidense", "t1", 1.0, 50.0, 45.0, 50.0, -2.0, -1.0, 5.0, 5.0, 0.0),
            ],
        )
        self.conn.commit()

    def test_daily_returns_selected_and_weight(self):
        self._seed_two_snapshots()

        out = assets_performance(period="daily")

        self.assertEqual(out.get("period"), "daily")
        self.assertEqual(out.get("from"), "2026-02-13")
        self.assertEqual(out.get("to"), "2026-02-13")
        rows = out.get("rows") or []
        self.assertGreaterEqual(len(rows), 2)
        self.assertTrue(all("selected_value" in r for r in rows))
        self.assertTrue(all("selected_pct" in r for r in rows))
        self.assertTrue(all("weight_pct" in r for r in rows))
        total_weight = sum(float(r.get("weight_pct") or 0.0) for r in rows)
        self.assertAlmostEqual(total_weight, 100.0, places=6)

    def test_weekly_warnings_from_orders(self):
        self._seed_two_snapshots()
        self.conn.executemany(
            """
            INSERT INTO orders(order_number,status,symbol,side,side_norm,quantity,price,operated_amount,currency,created_at,updated_at,operated_at)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            [
                (1, "terminada", "AAA", "Operacion rara", None, None, None, 100.0, None, "2026-02-10T10:00:00", None, None),
                (2, "terminada", "AAA", "Compra", "buy", None, None, None, None, "2026-02-10T11:00:00", None, None),
            ],
        )
        self.conn.commit()

        out = assets_performance(period="weekly")

        self.assertIn("ORDERS_INCOMPLETE", out.get("warnings") or [])
        stats = out.get("orders_stats") or {}
        self.assertEqual(stats.get("total"), 2)
        self.assertEqual(stats.get("unclassified"), 1)
        self.assertEqual(stats.get("amount_missing"), 1)

    def test_weekly_excludes_orders_on_base_snapshot_day(self):
        self._seed_two_snapshots()
        # Weekly base is 2026-02-06 (latest=2026-02-13). This sell happened before end-of-day base snapshot
        # and must not be counted in period cashflows.
        self.conn.execute(
            """
            INSERT INTO orders(order_number,status,symbol,side,side_norm,quantity,price,operated_amount,currency,created_at,updated_at,operated_at)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (9, "terminada", "ZZZ", "Venta", "sell", None, None, 5000.0, "peso_Argentino", "2026-02-06T10:00:00", None, None),
        )
        self.conn.commit()

        out = assets_performance(period="weekly")
        rows = out.get("rows") or []
        self.assertIsNone(next((r for r in rows if r.get("symbol") == "ZZZ"), None))

    def test_monthly_and_yearly_without_snapshots_return_null_dates(self):
        self.conn.execute("INSERT INTO portfolio_snapshots(snapshot_date,total_value) VALUES(?,?)", ("2026-02-13", 900.0))
        self.conn.execute(
            """
            INSERT INTO portfolio_assets(
              snapshot_date,symbol,description,market,type,currency,plazo,quantity,last_price,ppc,total_value,
              daily_var_pct,daily_var_points,gain_pct,gain_amount,committed
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            ("2026-02-13", "AAA", "Asset A", "bcba", "ACCIONES", "peso_Argentino", "t1", 1.0, 120.0, 100.0, 120.0, 1.0, 1.2, 20.0, 20.0, 0.0),
        )
        self.conn.commit()

        out_month = assets_performance(period="monthly", month=1, year=2026)
        out_year = assets_performance(period="yearly", year=2025)

        self.assertIsNone(out_month.get("from"))
        self.assertIsNone(out_month.get("to"))
        self.assertEqual(out_month.get("rows"), [])

        self.assertIsNone(out_year.get("from"))
        self.assertIsNone(out_year.get("to"))
        self.assertEqual(out_year.get("rows"), [])

    def test_accumulated_includes_closed_assets_with_historical_pnl(self):
        self.conn.executemany(
            "INSERT INTO portfolio_snapshots(snapshot_date,total_value) VALUES(?,?)",
            [("2026-01-10", 1000.0), ("2026-02-13", 1100.0)],
        )
        self.conn.executemany(
            """
            INSERT INTO portfolio_assets(
              snapshot_date,symbol,description,market,type,currency,plazo,quantity,last_price,ppc,total_value,
              daily_var_pct,daily_var_points,gain_pct,gain_amount,committed
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            [
                ("2026-01-10", "AAA", "Asset A", "bcba", "BONOS", "peso_Argentino", "t1", 1.0, 100.0, 90.0, 100.0, 0.0, 0.0, 0.0, 0.0, 0.0),
                ("2026-02-13", "BBB", "Asset B", "nyse", "CEDEAR", "dolar_Estadounidense", "t1", 1.0, 200.0, 180.0, 200.0, 0.0, 0.0, 0.0, 0.0, 0.0),
            ],
        )
        self.conn.execute(
            """
            INSERT INTO orders(order_number,status,symbol,side,side_norm,quantity,price,operated_amount,currency,created_at,updated_at,operated_at)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (1, "terminada", "AAA", "Venta", "sell", 1.0, 120.0, 120.0, "peso_Argentino", "2026-02-01T10:00:00", None, None),
        )
        self.conn.commit()

        out = assets_performance(period="accumulated")
        rows = out.get("rows") or []
        row_a = next((r for r in rows if r.get("symbol") == "AAA"), None)

        self.assertIsNotNone(row_a)
        self.assertAlmostEqual(float(row_a.get("total_value") or 0.0), 0.0, places=6)
        self.assertGreater(float(row_a.get("selected_value") or 0.0), 0.0)
        self.assertEqual(str(row_a.get("flow_tag")), "liquidated")

    def test_invalid_params_return_400(self):
        self._seed_two_snapshots()

        out1 = assets_performance(period="invalid")
        out2 = assets_performance(period="monthly", month=13, year=2026)
        out3 = assets_performance(period="monthly", month=2, year=2025)

        self.assertIsInstance(out1, JSONResponse)
        self.assertEqual(out1.status_code, 400)
        self.assertIsInstance(out2, JSONResponse)
        self.assertEqual(out2.status_code, 400)
        self.assertIsInstance(out3, JSONResponse)
        self.assertEqual(out3.status_code, 400)


if __name__ == "__main__":
    unittest.main()
