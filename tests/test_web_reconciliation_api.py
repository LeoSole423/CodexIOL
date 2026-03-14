import unittest

from iol_web.routes_api import reconciliation_apply, reconciliation_open
from tests_support import InitDbTestCase


class TestWebReconciliationApi(InitDbTestCase):
    def setUp(self):
        super().setUp()
        conn = self.connect()
        conn.executemany(
            """
            INSERT INTO portfolio_snapshots(snapshot_date,total_value,cash_total_ars,cash_disponible_ars)
            VALUES(?,?,?,?)
            """,
            [
                ("2026-03-05", 100000.0, 5000.0, 5000.0),
                ("2026-03-06", 120000.0, 25000.0, 25000.0),
            ],
        )
        conn.execute(
            """
            INSERT INTO orders(order_number,status,symbol,side,side_norm,operated_amount,currency,operated_at)
            VALUES(?,?,?,?,?,?,?,?)
            """,
            (1, "terminada", "AAA", "Compra", "buy", 5000.0, "peso_Argentino", "2026-03-06T11:00:00"),
        )
        conn.commit()
        conn.close()

    def test_open_and_apply(self):
        payload = reconciliation_open(as_of="2026-03-06")
        rows = payload.get("rows") or []
        self.assertEqual(len(rows), 1)
        proposal_id = int(rows[0]["id"])

        out = reconciliation_apply({"proposal_id": proposal_id})
        self.assertTrue(bool(out.get("ok")))

        payload_after = reconciliation_open(as_of="2026-03-06")
        self.assertEqual(len(payload_after.get("rows") or []), 0)


if __name__ == "__main__":
    unittest.main()
