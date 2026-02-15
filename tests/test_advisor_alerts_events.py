import os
import sqlite3
import tempfile
import unittest

from typer.testing import CliRunner

from iol_cli.cli import app


def _base_env(db_path: str) -> dict:
    env = os.environ.copy()
    env["IOL_USERNAME"] = "user"
    env["IOL_PASSWORD"] = "pass"
    env["IOL_DB_PATH"] = db_path
    env["IOL_API_URL"] = "https://api.invertironline.com"
    return env


class TestAdvisorAlertsEvents(unittest.TestCase):
    def setUp(self):
        self.runner = CliRunner()
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.tmp.name, "test_alerts.db")
        self.env = _base_env(self.db_path)

    def tearDown(self):
        self.tmp.cleanup()

    def _fetchone(self, sql: str, params=()):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            return conn.execute(sql, params).fetchone()
        finally:
            conn.close()

    def test_alert_create_list_close_and_seguimiento(self):
        res_create = self.runner.invoke(
            app,
            [
                "advisor",
                "alert",
                "create",
                "--type",
                "concentration",
                "--title",
                "Peso alto SPY",
                "--description",
                "SPY supera umbral objetivo",
                "--severity",
                "high",
                "--symbol",
                "SPY",
                "--snapshot-date",
                "2026-02-10",
                "--due-date",
                "2026-02-15",
            ],
            env=self.env,
        )
        self.assertEqual(res_create.exit_code, 0, msg=res_create.output)
        row = self._fetchone("SELECT id, status, severity, symbol, due_date FROM advisor_alerts ORDER BY id DESC LIMIT 1")
        self.assertIsNotNone(row)
        self.assertEqual(row["status"], "open")
        self.assertEqual(row["severity"], "high")
        alert_id = int(row["id"])

        res_list = self.runner.invoke(app, ["advisor", "alert", "list", "--status", "open"], env=self.env)
        self.assertEqual(res_list.exit_code, 0, msg=res_list.output)
        self.assertIn("Peso alto SPY", res_list.output)

        res_close = self.runner.invoke(
            app,
            ["advisor", "alert", "close", "--id", str(alert_id), "--reason", "Rebalanceo ejecutado"],
            env=self.env,
        )
        self.assertEqual(res_close.exit_code, 0, msg=res_close.output)
        closed_row = self._fetchone("SELECT status, closed_reason, closed_at FROM advisor_alerts WHERE id = ?", (alert_id,))
        self.assertEqual(closed_row["status"], "closed")
        self.assertEqual(closed_row["closed_reason"], "Rebalanceo ejecutado")
        self.assertIsNotNone(closed_row["closed_at"])

        out_path = os.path.join(self.tmp.name, "Seguimiento.md")
        res_seguimiento = self.runner.invoke(
            app,
            ["advisor", "seguimiento", "--out", out_path],
            env=self.env,
        )
        self.assertEqual(res_seguimiento.exit_code, 0, msg=res_seguimiento.output)
        self.assertTrue(os.path.exists(out_path))
        with open(out_path, "r", encoding="utf-8") as f:
            text = f.read()
        self.assertIn("advisor_alerts", text)
        self.assertIn("Sin alertas abiertas.", text)

    def test_event_add_requires_existing_alert_id(self):
        res = self.runner.invoke(
            app,
            [
                "advisor",
                "event",
                "add",
                "--type",
                "portfolio",
                "--title",
                "Intento sin alerta",
                "--alert-id",
                "999",
            ],
            env=self.env,
        )
        self.assertNotEqual(res.exit_code, 0)
        self.assertIn("Alert ID not found.", res.output)

    def test_event_add_and_list_filters(self):
        res_create = self.runner.invoke(
            app,
            [
                "advisor",
                "alert",
                "create",
                "--type",
                "drift",
                "--title",
                "Desvio allocation",
                "--description",
                "Bloque equity supera target",
            ],
            env=self.env,
        )
        self.assertEqual(res_create.exit_code, 0, msg=res_create.output)
        alert_row = self._fetchone("SELECT id FROM advisor_alerts ORDER BY id DESC LIMIT 1")
        self.assertIsNotNone(alert_row)
        alert_id = int(alert_row["id"])

        res_add = self.runner.invoke(
            app,
            [
                "advisor",
                "event",
                "add",
                "--type",
                "portfolio",
                "--title",
                "Compra parcial ACWI",
                "--description",
                "Se compra bloque inicial",
                "--symbol",
                "ACWI",
                "--snapshot-date",
                "2026-02-10",
                "--alert-id",
                str(alert_id),
            ],
            env=self.env,
        )
        self.assertEqual(res_add.exit_code, 0, msg=res_add.output)

        res_add_free = self.runner.invoke(
            app,
            [
                "advisor",
                "event",
                "add",
                "--type",
                "note",
                "--title",
                "Observacion libre",
                "--description",
                "Sin alerta asociada",
            ],
            env=self.env,
        )
        self.assertEqual(res_add_free.exit_code, 0, msg=res_add_free.output)

        event_row = self._fetchone(
            "SELECT event_type, symbol, alert_id FROM advisor_events ORDER BY id DESC LIMIT 1"
        )
        self.assertEqual(event_row["event_type"], "note")
        self.assertIsNone(event_row["symbol"])
        self.assertIsNone(event_row["alert_id"])

        event_row_linked = self._fetchone(
            "SELECT event_type, symbol, alert_id FROM advisor_events WHERE event_type='portfolio' ORDER BY id DESC LIMIT 1"
        )
        self.assertEqual(event_row_linked["event_type"], "portfolio")
        self.assertEqual(event_row_linked["symbol"], "ACWI")
        self.assertEqual(int(event_row_linked["alert_id"]), alert_id)

        res_list = self.runner.invoke(
            app,
            ["advisor", "event", "list", "--type", "portfolio", "--symbol", "ACWI"],
            env=self.env,
        )
        self.assertEqual(res_list.exit_code, 0, msg=res_list.output)
        self.assertIn("Compra parcial ACWI", res_list.output)

    def test_validations_and_export_whitelist(self):
        res_bad_severity = self.runner.invoke(
            app,
            [
                "advisor",
                "alert",
                "create",
                "--type",
                "risk",
                "--title",
                "x",
                "--description",
                "y",
                "--severity",
                "critical",
            ],
            env=self.env,
        )
        self.assertNotEqual(res_bad_severity.exit_code, 0)

        res_bad_date = self.runner.invoke(
            app,
            [
                "advisor",
                "event",
                "add",
                "--type",
                "note",
                "--title",
                "fecha invalida",
                "--snapshot-date",
                "10-02-2026",
            ],
            env=self.env,
        )
        self.assertNotEqual(res_bad_date.exit_code, 0)

        res_export_alerts = self.runner.invoke(
            app, ["data", "export", "--table", "advisor_alerts", "--format", "json"], env=self.env
        )
        self.assertEqual(res_export_alerts.exit_code, 0, msg=res_export_alerts.output)

        res_export_events = self.runner.invoke(
            app, ["data", "export", "--table", "advisor_events", "--format", "json"], env=self.env
        )
        self.assertEqual(res_export_events.exit_code, 0, msg=res_export_events.output)


if __name__ == "__main__":
    unittest.main()
