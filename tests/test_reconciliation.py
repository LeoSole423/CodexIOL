import os
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from iol_cli.db import connect, init_db
from iol_reconciliation.service import apply_proposal, get_open_payload, run_reconciliation
from iol_web.inflation_ar import InflationFetchResult
from iol_web.routes_api import quality


def _mk_db():
    tmp = tempfile.TemporaryDirectory()
    db_path = os.path.join(tmp.name, "reconciliation.db")
    conn = connect(db_path)
    init_db(conn)
    return tmp, conn, db_path


class TestReconciliationService(unittest.TestCase):
    def setUp(self):
        self._prev_db = os.environ.get("IOL_DB_PATH")

    def tearDown(self):
        if self._prev_db is None:
            os.environ.pop("IOL_DB_PATH", None)
        else:
            os.environ["IOL_DB_PATH"] = self._prev_db

    def test_detects_external_deposit_import_proposal(self):
        tmp, conn, db_path = _mk_db()
        try:
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
            conn.commit()

            payload = run_reconciliation(conn, as_of="2026-03-06", days=10, force=True)
            proposals = payload.get("proposals") or []
            self.assertEqual(len(proposals), 1)
            proposal = proposals[0]
            self.assertEqual(str(proposal.get("resolution_type")), "import")
            self.assertEqual(str(proposal.get("suggested_kind")), "deposit")
        finally:
            conn.close()
            tmp.cleanup()

    def test_detects_internal_fee_without_external_cashflow(self):
        tmp, conn, db_path = _mk_db()
        try:
            conn.executemany(
                """
                INSERT INTO portfolio_snapshots(snapshot_date,total_value,cash_total_ars,cash_disponible_ars)
                VALUES(?,?,?,?)
                """,
                [
                    ("2026-02-10", 2000000.0, 100000.0, 100000.0),
                    ("2026-02-11", 2000000.0, 80000.0, 80000.0),
                ],
            )
            conn.executemany(
                """
                INSERT INTO orders(order_number,status,symbol,side,side_norm,operated_amount,currency,operated_at)
                VALUES(?,?,?,?,?,?,?,?)
                """,
                [
                    (20, "terminada", "AAA", "Compra", "buy", 1000000.0, "peso_Argentino", "2026-02-11T10:00:00"),
                    (21, "terminada", "AAA", "Venta", "sell", 980500.0, "peso_Argentino", "2026-02-11T11:00:00"),
                ],
            )
            conn.commit()

            payload = run_reconciliation(conn, as_of="2026-02-11", days=10, force=True)
            proposals = payload.get("proposals") or []
            self.assertEqual(len(proposals), 1)
            proposal = proposals[0]
            self.assertEqual(str(proposal.get("resolution_type")), "ignore_internal")
            self.assertEqual(str(proposal.get("issue_code")), "OPERATIONAL_FEE_OR_TAX")
        finally:
            conn.close()
            tmp.cleanup()

    def test_apply_manual_adjustment_improves_quality(self):
        tmp, conn, db_path = _mk_db()
        try:
            conn.executemany(
                """
                INSERT INTO portfolio_snapshots(snapshot_date,total_value,cash_total_ars,cash_disponible_ars,retrieved_at)
                VALUES(?,?,?,?,?)
                """,
                [
                    ("2026-03-05", 100000.0, 5000.0, 5000.0, "2026-03-05T20:00:00Z"),
                    ("2026-03-06", 120000.0, 25000.0, 25000.0, "2026-03-06T20:00:00Z"),
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
            os.environ["IOL_DB_PATH"] = db_path

            run_reconciliation(conn, as_of="2026-03-06", days=10, force=True)
            open_payload = get_open_payload(conn, as_of="2026-03-06", ensure=False)
            proposals = open_payload.get("rows") or []
            self.assertEqual(len(proposals), 1)
            proposal = proposals[0]
            self.assertEqual(str(proposal.get("resolution_type")), "manual_adjustment")

            apply_out = apply_proposal(conn, int(proposal["id"]))
            self.assertTrue(bool(apply_out.get("ok")))

            mocked = InflationFetchResult(
                series_id="mock",
                fetched_at=0.0,
                stale=False,
                data=[("2026-03-01", 0.02)],
                source="mock",
            )
            with patch("iol_web.routes_api.get_inflation_series", return_value=mocked):
                out = quality()
            rows = {str(r.get("id")): r for r in (out.get("rows") or [])}
            self.assertEqual(str(rows["quality_inference"]["kind"]), "ok")
            self.assertIn("manual", str(rows["cashflow_imports"]["value"]).lower())
        finally:
            conn.close()
            tmp.cleanup()


if __name__ == "__main__":
    unittest.main()
