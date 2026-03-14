import json
import os
import tempfile
import unittest

from typer.testing import CliRunner

from iol_cli.cli import app
from iol_cli.db import connect, init_db


def _base_env(db_path: str) -> dict:
    env = os.environ.copy()
    env["IOL_USERNAME"] = "user"
    env["IOL_PASSWORD"] = "pass"
    env["IOL_DB_PATH"] = db_path
    env["IOL_API_URL"] = "https://api.invertironline.com"
    return env


class TestCliReconciliation(unittest.TestCase):
    def setUp(self):
        self.runner = CliRunner()
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.tmp.name, "reconcile.db")
        self.env = _base_env(self.db_path)
        conn = connect(self.db_path)
        init_db(conn)
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

    def tearDown(self):
        self.tmp.cleanup()

    def test_run_list_and_apply(self):
        run_out = self.runner.invoke(app, ["reconcile", "run", "--as-of", "2026-03-06"], env=self.env)
        self.assertEqual(run_out.exit_code, 0, msg=run_out.output)
        payload = json.loads(run_out.stdout)
        self.assertEqual(len(payload.get("proposals") or []), 1)
        proposal_id = int(payload["proposals"][0]["id"])

        open_out = self.runner.invoke(app, ["reconcile", "list-open", "--as-of", "2026-03-06"], env=self.env)
        self.assertEqual(open_out.exit_code, 0, msg=open_out.output)
        open_payload = json.loads(open_out.stdout)
        self.assertEqual(len(open_payload.get("rows") or []), 1)

        apply_out = self.runner.invoke(
            app,
            ["reconcile", "apply", "--proposal-id", str(proposal_id)],
            env=self.env,
        )
        self.assertEqual(apply_out.exit_code, 0, msg=apply_out.output)
        applied = json.loads(apply_out.stdout)
        self.assertTrue(bool(applied.get("ok")))


if __name__ == "__main__":
    unittest.main()
