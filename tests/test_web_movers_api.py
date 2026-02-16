import os
import sqlite3
import tempfile
import unittest

from iol_web.routes_api import movers


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


class TestMoversApiWarnings(unittest.TestCase):
    def test_only_ignored_orders_does_not_mark_incomplete(self):
        conn, path = _mk_db()
        prev_env = os.environ.get("IOL_DB_PATH")
        try:
            _seed_snapshots_and_assets(conn)
            conn.execute(
                """
                INSERT INTO orders(order_number,status,symbol,side,side_norm,quantity,price,operated_amount,currency,created_at,updated_at,operated_at)
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (1, "terminada", "AAA", "Pago de Dividendos", None, None, None, 10.0, None, "2026-02-10T10:00:00", None, None),
            )
            conn.commit()
            os.environ["IOL_DB_PATH"] = path

            out = movers(kind="period", period="weekly", metric="pnl", currency="peso_Argentino", limit=10)

            self.assertEqual(out.get("warnings"), [])
            stats = out.get("orders_stats") or {}
            self.assertEqual(stats.get("total"), 1)
            self.assertEqual(stats.get("ignored"), 1)
            self.assertEqual(stats.get("unclassified"), 0)
            self.assertEqual(stats.get("amount_missing"), 0)
        finally:
            if prev_env is None:
                os.environ.pop("IOL_DB_PATH", None)
            else:
                os.environ["IOL_DB_PATH"] = prev_env
            _cleanup(conn, path)

    def test_unclassified_or_missing_amount_marks_incomplete(self):
        conn, path = _mk_db()
        prev_env = os.environ.get("IOL_DB_PATH")
        try:
            _seed_snapshots_and_assets(conn)
            conn.executemany(
                """
                INSERT INTO orders(order_number,status,symbol,side,side_norm,quantity,price,operated_amount,currency,created_at,updated_at,operated_at)
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                [
                    (1, "terminada", "AAA", "Operacion rara", None, None, None, 100.0, None, "2026-02-10T10:00:00", None, None),
                    (2, "terminada", "AAA", "Compra", "buy", None, None, None, None, "2026-02-10T11:00:00", None, None),
                ],
            )
            conn.commit()
            os.environ["IOL_DB_PATH"] = path

            out = movers(kind="period", period="weekly", metric="pnl", currency="peso_Argentino", limit=10)

            self.assertIn("ORDERS_INCOMPLETE", out.get("warnings") or [])
            stats = out.get("orders_stats") or {}
            self.assertEqual(stats.get("total"), 2)
            self.assertEqual(stats.get("unclassified"), 1)
            self.assertEqual(stats.get("amount_missing"), 1)
            self.assertEqual(stats.get("ignored"), 0)
        finally:
            if prev_env is None:
                os.environ.pop("IOL_DB_PATH", None)
            else:
                os.environ["IOL_DB_PATH"] = prev_env
            _cleanup(conn, path)


if __name__ == "__main__":
    unittest.main()

