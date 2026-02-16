import os
import sqlite3
import tempfile
import unittest

from iol_web.db import orders_cashflows_by_symbol


def _mk_db():
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
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


class TestOrdersCashflowsBySymbol(unittest.TestCase):
    def test_maps_fci_amortization_and_ignored(self):
        conn, path = _mk_db()
        try:
            rows = [
                # sell flow from side fallback (side_norm is NULL)
                (1, "terminada", "ADBAICA", "Rescate FCI", None, 3372.4678, None, 116387.37, None, "2026-02-09T19:11:00", None, None),
                # buy flow from side fallback (side_norm is NULL, with accent)
                (2, "terminada", "ADRDOLA", "Suscripción FCI", None, 23156.0395, None, 390481.43, None, "2026-02-10T20:23:00", None, None),
                # sell flow from amortization
                (3, "terminada", "T13F6", "Pago de Amortización", None, None, None, 241368.39, None, "2026-02-13T10:57:18", None, None),
                # ignored flows
                (4, "terminada", "SPY", "Pago de Dividendos", None, None, None, 123.45, None, "2026-02-10T12:00:00", None, None),
                (5, "terminada", "AL30", "Pago de Renta", None, None, None, None, None, "2026-02-10T13:00:00", None, None),
                # buy with missing amount
                (6, "terminada", "MISSING", "Compra", "buy", None, None, None, None, "2026-02-10T14:00:00", None, None),
                # unknown side
                (7, "terminada", "UNK", "Operacion rara", None, None, None, 100.0, None, "2026-02-10T15:00:00", None, None),
                # should be ignored by status
                (8, "cancelada", "ADBAICA", "Rescate FCI", None, None, None, 999.0, None, "2026-02-10T16:00:00", None, None),
            ]
            conn.executemany(
                """
                INSERT INTO orders(
                  order_number,status,symbol,side,side_norm,quantity,price,operated_amount,currency,created_at,updated_at,operated_at
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                rows,
            )
            conn.commit()

            cashflows, stats = orders_cashflows_by_symbol(
                conn,
                dt_from="2026-02-06T00:00:00",
                dt_to="2026-02-13T23:59:59",
                currency="peso_Argentino",
            )

            self.assertAlmostEqual(cashflows["ADBAICA"]["sell_amount"], 116387.37)
            self.assertAlmostEqual(cashflows["ADRDOLA"]["buy_amount"], 390481.43)
            self.assertAlmostEqual(cashflows["T13F6"]["sell_amount"], 241368.39)

            self.assertEqual(stats["total"], 7)
            self.assertEqual(stats["classified"], 3)
            self.assertEqual(stats["ignored"], 2)
            self.assertEqual(stats["unclassified"], 1)
            self.assertEqual(stats["amount_missing"], 1)
        finally:
            _cleanup(conn, path)


if __name__ == "__main__":
    unittest.main()

