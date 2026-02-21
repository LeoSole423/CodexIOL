import os
import sqlite3
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


class TestCliCashflow(unittest.TestCase):
    def setUp(self):
        self.runner = CliRunner()
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.tmp.name, "cashflow.db")
        self.env = _base_env(self.db_path)
        conn = connect(self.db_path)
        init_db(conn)
        conn.close()

    def tearDown(self):
        self.tmp.cleanup()

    def _rows(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            return conn.execute(
                "SELECT id, flow_date, kind, amount_ars, note FROM manual_cashflow_adjustments ORDER BY id ASC"
            ).fetchall()
        finally:
            conn.close()

    def test_add_list_delete(self):
        r1 = self.runner.invoke(
            app,
            ["cashflow", "add", "--date", "2026-02-18", "--kind", "deposit", "--amount", "500"],
            env=self.env,
        )
        self.assertEqual(r1.exit_code, 0, msg=r1.output)

        r2 = self.runner.invoke(
            app,
            ["cashflow", "add", "--date", "2026-02-19", "--kind", "withdraw", "--amount", "20", "--note", "retiro"],
            env=self.env,
        )
        self.assertEqual(r2.exit_code, 0, msg=r2.output)

        r3 = self.runner.invoke(
            app,
            ["cashflow", "add", "--date", "2026-02-19", "--kind", "correction", "--amount", "-10"],
            env=self.env,
        )
        self.assertEqual(r3.exit_code, 0, msg=r3.output)

        rows = self._rows()
        self.assertEqual(len(rows), 3)
        self.assertAlmostEqual(float(rows[0]["amount_ars"]), 500.0)
        self.assertAlmostEqual(float(rows[1]["amount_ars"]), -20.0)
        self.assertAlmostEqual(float(rows[2]["amount_ars"]), -10.0)

        listed = self.runner.invoke(
            app,
            ["cashflow", "list", "--from", "2026-02-18", "--to", "2026-02-19"],
            env=self.env,
        )
        self.assertEqual(listed.exit_code, 0, msg=listed.output)
        self.assertIn("deposit", listed.output)
        self.assertIn("withdraw", listed.output)
        self.assertIn("correction", listed.output)

        target_id = int(rows[0]["id"])
        deleted = self.runner.invoke(app, ["cashflow", "delete", "--id", str(target_id)], env=self.env)
        self.assertEqual(deleted.exit_code, 0, msg=deleted.output)

        rows_after = self._rows()
        self.assertEqual(len(rows_after), 2)
        ids = [int(r["id"]) for r in rows_after]
        self.assertNotIn(target_id, ids)

    def test_validation_negative_withdraw(self):
        res = self.runner.invoke(
            app,
            ["cashflow", "add", "--date", "2026-02-18", "--kind", "withdraw", "--amount", "-10"],
            env=self.env,
        )
        self.assertNotEqual(res.exit_code, 0)
        self.assertIn("--amount", res.output)


if __name__ == "__main__":
    unittest.main()
