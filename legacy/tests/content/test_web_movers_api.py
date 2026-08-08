import unittest

from iol_web.routes_api import movers
from tests_support import WebDbTestCase, SCHEMA_SNAPSHOTS, SCHEMA_ASSETS, SCHEMA_ORDERS


def _seed_snapshots_and_assets(conn):
    conn.executemany(
        "INSERT INTO portfolio_snapshots(snapshot_date,total_value) VALUES(?,?)",
        [("2026-02-06", 1000.0), ("2026-02-13", 900.0)],
    )
    conn.execute(
        """
        INSERT INTO portfolio_assets(
          snapshot_date,symbol,description,market,type,currency,plazo,quantity,last_price,ppc,total_value,
          daily_var_pct,daily_var_points,gain_pct,gain_amount,committed
        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        ("2026-02-06", "AAA", "A", "bcba", "ACCIONES", "peso_Argentino", "t1", 1.0, 100.0, 100.0, 100.0, 0.0, 0.0, 0.0, 0.0, 0.0),
    )
    conn.commit()


class TestMoversApiWarnings(WebDbTestCase):
    schema_sql = SCHEMA_SNAPSHOTS + SCHEMA_ASSETS + SCHEMA_ORDERS

    def test_only_ignored_orders_does_not_mark_incomplete(self):
        _seed_snapshots_and_assets(self.conn)
        self.conn.execute(
            """
            INSERT INTO orders(order_number,status,symbol,side,side_norm,quantity,price,operated_amount,currency,created_at,updated_at,operated_at)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (1, "terminada", "AAA", "Pago de Dividendos", None, None, None, 10.0, None, "2026-02-10T10:00:00", None, None),
        )
        self.conn.commit()

        out = movers(kind="period", period="weekly", metric="pnl", currency="peso_Argentino", limit=10)

        self.assertEqual(out.get("warnings"), [])
        stats = out.get("orders_stats") or {}
        self.assertEqual(stats.get("total"), 1)
        self.assertEqual(stats.get("ignored"), 1)
        self.assertEqual(stats.get("unclassified"), 0)
        self.assertEqual(stats.get("amount_missing"), 0)

    def test_unclassified_or_missing_amount_marks_incomplete(self):
        _seed_snapshots_and_assets(self.conn)
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

        out = movers(kind="period", period="weekly", metric="pnl", currency="peso_Argentino", limit=10)

        self.assertIn("ORDERS_INCOMPLETE", out.get("warnings") or [])
        stats = out.get("orders_stats") or {}
        self.assertEqual(stats.get("total"), 2)
        self.assertEqual(stats.get("unclassified"), 1)
        self.assertEqual(stats.get("amount_missing"), 1)
        self.assertEqual(stats.get("ignored"), 0)

    def test_weekly_excludes_orders_on_base_snapshot_day(self):
        _seed_snapshots_and_assets(self.conn)
        self.conn.execute(
            """
            INSERT INTO orders(order_number,status,symbol,side,side_norm,quantity,price,operated_amount,currency,created_at,updated_at,operated_at)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (99, "terminada", "ZZZ", "Venta", "sell", None, None, 5000.0, "peso_Argentino", "2026-02-06T10:00:00", None, None),
        )
        self.conn.commit()

        out = movers(kind="period", period="weekly", metric="pnl", currency="all", limit=100)
        symbols = {r.get("symbol") for r in (out.get("gainers") or []) + (out.get("losers") or [])}
        self.assertNotIn("ZZZ", symbols)


if __name__ == "__main__":
    unittest.main()
