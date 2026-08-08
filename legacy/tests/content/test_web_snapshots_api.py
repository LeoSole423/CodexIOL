import unittest

from fastapi.responses import JSONResponse

from iol_web.routes_api import snapshots
from tests_support import WebDbTestCase, SCHEMA_SNAPSHOTS, SCHEMA_ORDERS


class TestWebSnapshotsApi(WebDbTestCase):
    schema_sql = SCHEMA_SNAPSHOTS + SCHEMA_ORDERS

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
                ("2026-02-11", 1500.0, 600.0),
                ("2026-02-12", 1600.0, 600.0),
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
                ("2026-02-23", 1500.0, 102.0, 600.0),
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
                ("2026-02-23", 1490.0, 600.0, 600.0),
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
        self.assertEqual(r23.get("display_kind"), "external_deposit_probable")
        self.assertEqual(r23.get("reason_code"), "SETTLEMENT_SMOOTHED")

        self.assertAlmostEqual(float(r24["flow_total_ars"]), 0.0, places=6)
        self.assertAlmostEqual(float(r24["applied_flow_ars"]), 0.0, places=6)
        self.assertEqual(r24.get("display_kind"), "settlement_carryover")
        self.assertEqual(r24.get("reason_code"), "SETTLEMENT_CARRYOVER")

        self.assertAlmostEqual(float(r23["total_value"]), 992.0, places=6)
        self.assertAlmostEqual(float(r24["total_value"]), 1002.0, places=6)

    def test_invalid_mode_returns_400(self):
        out = snapshots(date_from=None, date_to=None, mode="weird")
        self.assertIsInstance(out, JSONResponse)
        self.assertEqual(out.status_code, 400)


if __name__ == "__main__":
    unittest.main()
