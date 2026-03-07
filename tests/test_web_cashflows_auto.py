import os
import sqlite3
import tempfile
import unittest

from fastapi.responses import JSONResponse

from iol_web.routes_api import cashflows_auto


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
          cash_disponible_ars REAL,
          cash_disponible_usd REAL
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
        CREATE TABLE account_cash_movements (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          movement_id TEXT,
          occurred_at TEXT,
          movement_date TEXT,
          currency TEXT,
          amount REAL,
          kind TEXT,
          description TEXT,
          source TEXT,
          raw_json TEXT,
          created_at TEXT
        )
        """
    )
    conn.commit()
    return conn, path


def _cleanup(conn, path):
    conn.close()
    if os.path.exists(path):
        os.unlink(path)


def _insert_order(conn, order_number, side, side_norm, operated_amount, ts):
    conn.execute(
        """
        INSERT INTO orders(order_number,status,symbol,side,side_norm,quantity,price,operated_amount,currency,created_at,updated_at,operated_at)
        VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            int(order_number),
            "terminada",
            "AAA",
            side,
            side_norm,
            None,
            None,
            float(operated_amount),
            "peso_Argentino",
            ts,
            None,
            None,
        ),
    )


class TestWebCashflowsAuto(unittest.TestCase):
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

    def test_deposit_complete(self):
        self.conn.executemany(
            "INSERT INTO portfolio_snapshots(snapshot_date,total_value,cash_disponible_ars) VALUES(?,?,?)",
            [("2026-02-10", 1000.0, 100.0), ("2026-02-11", 1030.0, 130.0)],
        )
        self.conn.commit()

        out = cashflows_auto(days=30)
        rows = out.get("rows") or []
        # V2 threshold: abs(external_final) < 100 and no FX/imported movement => omitted.
        self.assertEqual(rows, [])

    def test_withdraw_complete(self):
        self.conn.executemany(
            "INSERT INTO portfolio_snapshots(snapshot_date,total_value,cash_disponible_ars) VALUES(?,?,?)",
            [("2026-02-10", 1000.0, 100.0), ("2026-02-11", 970.0, 70.0)],
        )
        self.conn.commit()

        out = cashflows_auto(days=30)
        rows = out.get("rows") or []
        self.assertEqual(rows, [])

    def test_correction_by_orders_incomplete(self):
        self.conn.executemany(
            "INSERT INTO portfolio_snapshots(snapshot_date,total_value,cash_disponible_ars) VALUES(?,?,?)",
            [("2026-02-10", 1000.0, 100.0), ("2026-02-11", 1020.0, 120.0)],
        )
        self.conn.execute(
            """
            INSERT INTO orders(order_number,status,symbol,side,side_norm,quantity,price,operated_amount,currency,created_at,updated_at,operated_at)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (1, "terminada", "AAA", "Operacion rara", None, None, None, 100.0, "peso_Argentino", "2026-02-11T10:00:00", None, None),
        )
        self.conn.commit()

        out = cashflows_auto(days=30)
        rows = out.get("rows") or []
        self.assertEqual(rows, [])

    def test_correction_by_cash_missing(self):
        self.conn.executemany(
            "INSERT INTO portfolio_snapshots(snapshot_date,total_value,cash_disponible_ars) VALUES(?,?,?)",
            [("2026-02-10", 1000.0, None), ("2026-02-11", 1040.0, None)],
        )
        self.conn.execute(
            """
            INSERT INTO orders(order_number,status,symbol,side,side_norm,quantity,price,operated_amount,currency,created_at,updated_at,operated_at)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (1, "terminada", "AAA", "Compra", "buy", None, None, 40.0, "peso_Argentino", "2026-02-11T10:00:00", None, None),
        )
        self.conn.commit()

        out = cashflows_auto(days=30)
        rows = out.get("rows") or []
        self.assertEqual(rows, [])

    def test_rotation_pair_two_days(self):
        self.conn.executemany(
            "INSERT INTO portfolio_snapshots(snapshot_date,total_value,cash_disponible_ars) VALUES(?,?,?)",
            [
                ("2026-02-08", 2000000.0, 1500000.0),
                ("2026-02-09", 2000000.0, 300000.0),
                ("2026-02-10", 2000000.0, 1467691.0),
            ],
        )
        # 2026-02-09: neto -1,691,959
        _insert_order(self.conn, 1, "Compra", "buy", 1192114.0, "2026-02-09T10:00:00")
        _insert_order(self.conn, 2, "Venta", "sell", 1684073.0, "2026-02-09T12:00:00")
        # 2026-02-10: neto +1,674,072
        _insert_order(self.conn, 3, "Compra", "buy", 506381.0, "2026-02-10T11:00:00")
        self.conn.commit()

        out = cashflows_auto(days=30)
        rows = out.get("rows") or []
        self.assertEqual(len(rows), 2)

        by_date = {r.get("flow_date"): r for r in rows}
        r9 = by_date.get("2026-02-09")
        r10 = by_date.get("2026-02-10")
        self.assertIsNotNone(r9)
        self.assertIsNotNone(r10)

        self.assertEqual(r9.get("display_kind"), "rotation_internal")
        self.assertEqual(r10.get("display_kind"), "rotation_internal")
        self.assertEqual(r9.get("reason_code"), "ROTATION_PAIR")
        self.assertEqual(r10.get("reason_code"), "ROTATION_PAIR")
        self.assertEqual(r9.get("paired_flow_date"), "2026-02-10")
        self.assertEqual(r10.get("paired_flow_date"), "2026-02-09")
        # Backward compatibility: keep legacy kind.
        self.assertEqual(r9.get("kind"), "withdraw")
        self.assertEqual(r10.get("kind"), "deposit")

    def test_rotation_pair_outside_window_not_detected(self):
        self.conn.executemany(
            "INSERT INTO portfolio_snapshots(snapshot_date,total_value,cash_disponible_ars) VALUES(?,?,?)",
            [
                ("2026-02-01", 1000.0, 10000.0),
                ("2026-02-02", 1000.0, 9500.0),
                ("2026-02-05", 1000.0, 10000.0),
            ],
        )
        _insert_order(self.conn, 10, "Compra", "buy", 2000.0, "2026-02-02T10:00:00")
        _insert_order(self.conn, 11, "Venta", "sell", 2000.0, "2026-02-05T10:00:00")
        self.conn.commit()

        out = cashflows_auto(days=30)
        rows = out.get("rows") or []
        self.assertEqual(len(rows), 2)
        by_date = {r.get("flow_date"): r for r in rows}
        r2 = by_date["2026-02-02"]
        r5 = by_date["2026-02-05"]
        self.assertEqual(r2.get("display_kind"), "external_deposit_probable")
        self.assertEqual(r5.get("display_kind"), "external_withdraw_probable")
        self.assertIsNone(r2.get("paired_flow_date"))
        self.assertIsNone(r5.get("paired_flow_date"))

    def test_operational_fee_or_tax(self):
        self.conn.executemany(
            "INSERT INTO portfolio_snapshots(snapshot_date,total_value,cash_disponible_ars) VALUES(?,?,?)",
            [("2026-02-10", 2000000.0, 100000.0), ("2026-02-11", 2000000.0, 80000.0)],
        )
        _insert_order(self.conn, 20, "Compra", "buy", 1000000.0, "2026-02-11T10:00:00")
        _insert_order(self.conn, 21, "Venta", "sell", 980500.0, "2026-02-11T11:00:00")
        self.conn.commit()

        out = cashflows_auto(days=30)
        rows = out.get("rows") or []
        self.assertEqual(len(rows), 1)
        r = rows[0]
        self.assertEqual(r.get("kind"), "withdraw")
        self.assertEqual(r.get("display_kind"), "operational_fee_or_tax")
        self.assertEqual(r.get("reason_code"), "OPERATIONAL_FEE_OR_TAX")
        self.assertIsNotNone(r.get("residual_ratio"))
        self.assertLessEqual(float(r.get("residual_ratio") or 1.0), 0.03)

    def test_external_flow_probable_sign(self):
        self.conn.executemany(
            "INSERT INTO portfolio_snapshots(snapshot_date,total_value,cash_disponible_ars) VALUES(?,?,?)",
            [("2026-02-10", 1000.0, 5000.0), ("2026-02-11", 1000.0, 12000.0)],
        )
        self.conn.commit()

        out = cashflows_auto(days=30)
        rows = out.get("rows") or []
        self.assertEqual(len(rows), 1)
        r = rows[0]
        self.assertEqual(r.get("kind"), "deposit")
        self.assertEqual(r.get("display_kind"), "external_deposit_probable")
        self.assertEqual(r.get("reason_code"), "EXTERNAL_FINAL_SIGN")
        self.assertEqual(r.get("display_label"), "Flujo externo probable (+)")

    def test_prefers_cash_total_and_avoids_next_day_settlement_withdraw(self):
        self.conn.executemany(
            "INSERT INTO portfolio_snapshots(snapshot_date,total_value,cash_total_ars,cash_disponible_ars) VALUES(?,?,?,?)",
            [
                ("2026-02-22", 1000.0, 100.0, 100.0),
                ("2026-02-23", 1500.0, 102.0, 600.0),
                ("2026-02-24", 1510.0, 102.0, 100.0),
            ],
        )
        _insert_order(self.conn, 30, "Compra", "buy", 498.0, "2026-02-23T11:00:00")
        self.conn.commit()

        out = cashflows_auto(days=30)
        rows = out.get("rows") or []
        self.assertEqual(len(rows), 1)
        r = rows[0]
        self.assertEqual(r.get("flow_date"), "2026-02-23")
        self.assertEqual(r.get("kind"), "deposit")
        self.assertAlmostEqual(float(r.get("amount_ars") or 0.0), 500.0)
        self.assertAlmostEqual(float(r.get("cash_delta_ars") or 0.0), 2.0)
        self.assertAlmostEqual(float(r.get("buy_amount_ars") or 0.0), 498.0)
        self.assertEqual(r.get("display_kind"), "external_deposit_probable")

    def test_cashflows_auto_smooths_settlement_carryover_pair(self):
        self.conn.executemany(
            "INSERT INTO portfolio_snapshots(snapshot_date,total_value,cash_total_ars,cash_disponible_ars) VALUES(?,?,?,?)",
            [
                ("2026-02-22", 1000.0, 100.0, 100.0),
                ("2026-02-23", 1490.0, 600.0, 600.0),
                ("2026-02-24", 1500.0, 100.0, 100.0),
            ],
        )
        _insert_order(self.conn, 31, "Compra", "buy", 498.0, "2026-02-23T11:00:00")
        self.conn.commit()

        out = cashflows_auto(days=30)
        rows = out.get("rows") or []
        self.assertEqual(len(rows), 2)
        by_date = {r.get("flow_date"): r for r in rows}
        r23 = by_date["2026-02-23"]
        r24 = by_date["2026-02-24"]

        self.assertAlmostEqual(float(r23.get("amount_ars") or 0.0), 498.0, places=6)
        self.assertEqual(r23.get("display_kind"), "external_deposit_probable")
        self.assertEqual(r23.get("reason_code"), "SETTLEMENT_SMOOTHED")

        self.assertAlmostEqual(float(r24.get("amount_ars") or 0.0), 0.0, places=6)
        self.assertEqual(r24.get("kind"), "correction")
        self.assertEqual(r24.get("display_kind"), "settlement_carryover")
        self.assertEqual(r24.get("reason_code"), "SETTLEMENT_CARRYOVER")

    def test_cashflows_auto_smooths_near_cancel_settlement_pair(self):
        self.conn.executemany(
            "INSERT INTO portfolio_snapshots(snapshot_date,total_value,cash_total_ars,cash_disponible_ars) VALUES(?,?,?,?)",
            [
                ("2026-02-09", 2000.0, 1000.0, 1000.0),
                ("2026-02-10", 1990.0, 882.0, 882.0),
                ("2026-02-11", 1980.0, 491.0, 491.0),
            ],
        )
        _insert_order(self.conn, 32, "Compra", "buy", 506.0, "2026-02-10T10:00:00")
        self.conn.commit()

        out = cashflows_auto(days=30)
        rows = out.get("rows") or []
        self.assertEqual(len(rows), 2)
        by_date = {r.get("flow_date"): r for r in rows}
        r10 = by_date["2026-02-10"]
        r11 = by_date["2026-02-11"]

        # +388 then -391 is collapsed to net -3 on traded day and 0 on carryover day.
        self.assertAlmostEqual(float(r10.get("amount_ars") or 0.0), -3.0, places=6)
        self.assertEqual(r10.get("reason_code"), "SETTLEMENT_SMOOTHED")
        self.assertAlmostEqual(float(r11.get("amount_ars") or 0.0), 0.0, places=6)
        self.assertEqual(r11.get("reason_code"), "SETTLEMENT_CARRYOVER")

    def test_fx_revaluation_usd_cash_not_external_flow(self):
        self.conn.executemany(
            "INSERT INTO portfolio_snapshots(snapshot_date,total_value,cash_total_ars,cash_disponible_ars,cash_disponible_usd) VALUES(?,?,?,?,?)",
            [
                ("2026-02-25", 1000.0, 80735.41, 815.41, 59.2),
                ("2026-02-27", 1000.0, 81919.41, 815.41, 59.2),
            ],
        )
        self.conn.commit()

        out = cashflows_auto(days=30)
        rows = out.get("rows") or []
        self.assertEqual(len(rows), 1)
        r = rows[0]
        self.assertEqual(r.get("display_kind"), "fx_revaluation_usd_cash")
        self.assertEqual(r.get("reason_code"), "FX_REVALUATION_USD_CASH")
        self.assertAlmostEqual(float(r.get("external_raw_ars") or 0.0), 1184.0, places=6)
        self.assertAlmostEqual(float(r.get("external_final_ars") or 0.0), 0.0, places=6)
        self.assertAlmostEqual(float(r.get("fx_revaluation_ars") or 0.0), 1184.0, places=6)

    def test_imported_external_flow_is_prioritized(self):
        self.conn.executemany(
            "INSERT INTO portfolio_snapshots(snapshot_date,total_value,cash_disponible_ars) VALUES(?,?,?)",
            [("2026-02-10", 1000.0, 100.0), ("2026-02-11", 1000.0, 100.0)],
        )
        self.conn.execute(
            """
            INSERT INTO account_cash_movements(movement_id,occurred_at,movement_date,currency,amount,kind,description,source,raw_json,created_at)
            VALUES(?,?,?,?,?,?,?,?,?,?)
            """,
            (
                "m1",
                "2026-02-11T11:00:00",
                "2026-02-11",
                "ARS",
                250.0,
                "external_deposit",
                "Aporte detectado",
                "test",
                "{}",
                "2026-02-11T11:00:00",
            ),
        )
        self.conn.commit()

        out = cashflows_auto(days=30)
        rows = out.get("rows") or []
        self.assertEqual(len(rows), 1)
        r = rows[0]
        self.assertEqual(r.get("display_kind"), "external_deposit_probable")
        self.assertEqual(r.get("reason_code"), "IMPORTED_EXTERNAL_PRIORITY")
        self.assertAlmostEqual(float(r.get("external_final_ars") or 0.0), 250.0, places=6)
        self.assertAlmostEqual(float(r.get("imported_external_ars") or 0.0), 250.0, places=6)

    def test_zero_net_not_listed(self):
        self.conn.executemany(
            "INSERT INTO portfolio_snapshots(snapshot_date,total_value,cash_disponible_ars) VALUES(?,?,?)",
            [("2026-02-10", 1000.0, 100.0), ("2026-02-11", 1000.0, 70.0)],
        )
        self.conn.execute(
            """
            INSERT INTO orders(order_number,status,symbol,side,side_norm,quantity,price,operated_amount,currency,created_at,updated_at,operated_at)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (1, "terminada", "AAA", "Compra", "buy", None, None, 30.0, "peso_Argentino", "2026-02-11T10:00:00", None, None),
        )
        self.conn.commit()

        out = cashflows_auto(days=30)
        self.assertEqual(out.get("rows"), [])

    def test_invalid_days_returns_400(self):
        out1 = cashflows_auto(days=0)
        out2 = cashflows_auto(days=366)
        self.assertIsInstance(out1, JSONResponse)
        self.assertEqual(out1.status_code, 400)
        self.assertIsInstance(out2, JSONResponse)
        self.assertEqual(out2.status_code, 400)

    def test_no_snapshots_or_single_snapshot(self):
        out0 = cashflows_auto(days=30)
        self.assertEqual(out0.get("rows"), [])

        self.conn.execute(
            "INSERT INTO portfolio_snapshots(snapshot_date,total_value,cash_disponible_ars) VALUES(?,?,?)",
            ("2026-02-11", 1000.0, 100.0),
        )
        self.conn.commit()

        out1 = cashflows_auto(days=30)
        self.assertEqual(out1.get("rows"), [])

    def test_default_window_30_days(self):
        self.conn.executemany(
            "INSERT INTO portfolio_snapshots(snapshot_date,total_value,cash_disponible_ars) VALUES(?,?,?)",
            [
                ("2026-01-10", 1000.0, 100.0),
                ("2026-01-25", 1000.0, 100.0),
                ("2026-02-20", 1020.0, 120.0),
            ],
        )
        self.conn.commit()

        out = cashflows_auto()
        self.assertEqual(out.get("from"), "2026-01-21")
        rows = out.get("rows") or []
        # Delta +20 (below threshold), no FX/imported movement => omitted in v2.
        self.assertEqual(rows, [])


if __name__ == "__main__":
    unittest.main()
