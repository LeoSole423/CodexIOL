import unittest

from iol_web.db import orders_flow_summary
from tests_support import cleanup_temp_sqlite_db, create_temp_sqlite_db


TEST_SCHEMA = """
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
);
"""


class TestOrdersFlowSummary(unittest.TestCase):
    def test_flow_summary_includes_income(self):
        conn, path = create_temp_sqlite_db(TEST_SCHEMA)
        try:
            rows = [
                (1, "terminada", "AAA", "Compra", "buy", None, None, 100.0, None, "2026-02-10T10:00:00", None, None),
                (2, "terminada", "AAA", "Venta", "sell", None, None, 60.0, None, "2026-02-10T11:00:00", None, None),
                (3, "terminada", "AAA", "Pago de Dividendos", None, None, None, 5.0, None, "2026-02-10T12:00:00", None, None),
                (4, "terminada", "AAA", "Pago de Renta", None, None, None, 2.0, None, "2026-02-10T13:00:00", None, None),
                (5, "terminada", "AAA", "Operacion rara", None, None, None, 8.0, None, "2026-02-10T14:00:00", None, None),
                (6, "terminada", "AAA", "Compra", "buy", None, None, None, None, "2026-02-10T15:00:00", None, None),
                (7, "cancelada", "AAA", "Compra", "buy", None, None, 999.0, None, "2026-02-10T16:00:00", None, None),
            ]
            conn.executemany(
                """
                INSERT INTO orders(order_number,status,symbol,side,side_norm,quantity,price,operated_amount,currency,created_at,updated_at,operated_at)
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                rows,
            )
            conn.commit()

            amounts, stats = orders_flow_summary(
                conn,
                dt_from="2026-02-10T09:00:00",
                dt_to="2026-02-10T23:59:59",
                currency="peso_Argentino",
            )

            self.assertAlmostEqual(amounts["buy_amount"], 100.0)
            self.assertAlmostEqual(amounts["sell_amount"], 60.0)
            self.assertAlmostEqual(amounts["income_amount"], 7.0)
            self.assertAlmostEqual(amounts["fee_amount"], 0.0)
            self.assertEqual(stats["total"], 6)
            self.assertEqual(stats["classified"], 2)
            self.assertEqual(stats["income_classified"], 2)
            self.assertEqual(stats["fee_classified"], 0)
            self.assertEqual(stats["unclassified"], 1)
            self.assertEqual(stats["amount_missing"], 1)
            self.assertEqual(stats["income_missing_deduped"], 0)
            self.assertEqual(stats["ignored"], 0)
        finally:
            cleanup_temp_sqlite_db(conn, path)

    def test_dedupes_missing_income_duplicate_and_counts_fee(self):
        conn, path = create_temp_sqlite_db(TEST_SCHEMA)
        try:
            rows = [
                (1, "terminada", "SPY US$", "Pago de Dividendos", None, None, None, 0.34, None, "2026-02-03T17:35:24.820", None, None),
                (2, "terminada", "SPY", "Pago de Dividendos", None, None, None, None, None, "2026-02-03T17:35:24.827", None, None),
                (3, "terminada", "AAA", "Comision", None, None, None, 10.0, None, "2026-02-03T18:00:00", None, None),
            ]
            conn.executemany(
                """
                INSERT INTO orders(order_number,status,symbol,side,side_norm,quantity,price,operated_amount,currency,created_at,updated_at,operated_at)
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                rows,
            )
            conn.commit()

            amounts, stats = orders_flow_summary(
                conn,
                dt_from="2026-02-03T00:00:00",
                dt_to="2026-02-03T23:59:59",
                currency="peso_Argentino",
            )

            self.assertAlmostEqual(amounts["income_amount"], 0.34)
            self.assertAlmostEqual(amounts["fee_amount"], 10.0)
            self.assertEqual(stats["amount_missing"], 0)
            self.assertEqual(stats["income_missing_deduped"], 1)
            self.assertEqual(stats["fee_classified"], 1)
        finally:
            cleanup_temp_sqlite_db(conn, path)


if __name__ == "__main__":
    unittest.main()
