import json
import os
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from typer.testing import CliRunner

from iol_cli.cli import app
from iol_cli.db import connect, init_db
from iol_web.inflation_ar import InflationFetchResult


def _base_env(db_path: str) -> dict:
    env = os.environ.copy()
    env["IOL_USERNAME"] = "user"
    env["IOL_PASSWORD"] = "pass"
    env["IOL_DB_PATH"] = db_path
    env["IOL_API_URL"] = "https://api.invertironline.com"
    return env


class _FakeClient:
    def __init__(self):
        self._quotes = {
            "SPY": {
                "ultimoPrecio": 100.0,
                "puntas": [{"precioCompra": 99.0, "precioVenta": 101.0}],
                "variacionPorcentual": 0.5,
                "cantidadOperaciones": 20,
                "volumenOperado": 100000.0,
            },
            "ACWI": {
                "ultimoPrecio": 50.0,
                "puntas": [{"precioCompra": 49.8, "precioVenta": 50.2}],
                "variacionPorcentual": 0.3,
                "cantidadOperaciones": 15,
                "volumenOperado": 80000.0,
            },
        }

    def get_panel_quotes(self, instrument: str, panel: str, country: str):
        return {
            "titulos": [
                {
                    "simbolo": "SPY",
                    "ultimoPrecio": 100.0,
                    "variacionPorcentual": 0.5,
                    "cantidadOperaciones": 20,
                    "volumenOperado": 100000.0,
                    "puntas": [{"precioCompra": 99.0, "precioVenta": 101.0}],
                },
                {
                    "simbolo": "ACWI",
                    "ultimoPrecio": 50.0,
                    "variacionPorcentual": 0.3,
                    "cantidadOperaciones": 15,
                    "volumenOperado": 80000.0,
                    "puntas": [{"precioCompra": 49.8, "precioVenta": 50.2}],
                },
            ]
        }

    def get_quote(self, market: str, symbol: str):
        return self._quotes[symbol]


class TestAdvisorAutopilot(unittest.TestCase):
    def setUp(self):
        self.runner = CliRunner()
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.tmp.name, "advisor_autopilot.db")
        self.env = _base_env(self.db_path)
        conn = connect(self.db_path)
        init_db(conn)
        conn.close()

    def tearDown(self):
        self.tmp.cleanup()

    def _conn(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _seed_portfolio(self, snapshot_date="2026-03-11"):
        conn = self._conn()
        try:
            conn.execute(
                """
                INSERT OR REPLACE INTO portfolio_snapshots(
                  snapshot_date,total_value,currency,titles_value,cash_total_ars,cash_disponible_ars,cash_disponible_usd,retrieved_at,source
                ) VALUES(?,?,?,?,?,?,?,?,?)
                """,
                (
                    snapshot_date,
                    1000000.0,
                    "peso_Argentino",
                    950000.0,
                    50000.0,
                    25000.0,
                    10.0,
                    "2026-03-11T20:00:00Z",
                    "test",
                ),
            )
            conn.executemany(
                """
                INSERT OR REPLACE INTO portfolio_assets(
                  snapshot_date,symbol,description,market,type,currency,plazo,quantity,last_price,ppc,total_value,daily_var_pct,daily_var_points,gain_pct,gain_amount,committed
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                [
                    (snapshot_date, "SPY", "SPY", "bcba", "CEDEARS", "peso_Argentino", "t1", 10, 100.0, 95.0, 100000.0, 0.5, 500.0, 5.0, 5000.0, 0.0),
                    (snapshot_date, "ACWI", "ACWI", "bcba", "CEDEARS", "peso_Argentino", "t1", 5, 50.0, 48.0, 250000.0, 0.3, 750.0, 4.0, 10000.0, 0.0),
                ],
            )
            conn.commit()
        finally:
            conn.close()

    def test_weekly_autopilot_creates_briefing_and_daily_reuses(self):
        self._seed_portfolio()
        mocked_inflation = InflationFetchResult(
            series_id="mock",
            fetched_at=0.0,
            stale=False,
            data=[("2026-03-01", 0.02)],
            source="mock",
        )
        with patch("iol_cli.cli._get_client", return_value=_FakeClient()), patch(
            "iol_cli.cli.collect_symbol_evidence", return_value=([], [])
        ):
            with patch("iol_web.routes_api.get_inflation_series", return_value=mocked_inflation):
                weekly = self.runner.invoke(
                    app,
                    ["advisor", "autopilot", "run", "--cadence", "weekly", "--budget-ars", "100000", "--top", "5"],
                    env=self.env,
                )
                self.assertEqual(weekly.exit_code, 0, msg=weekly.output)
                weekly_payload = json.loads(weekly.output)
                self.assertEqual(weekly_payload["briefing"]["cadence"], "weekly")
                first_recommendation = (weekly_payload["briefing"].get("recommendations") or [None])[0]
                self.assertIsNotNone(first_recommendation)
                self.assertIn("action_bucket", first_recommendation)
                self.assertIn("short_reason", first_recommendation)
                self.assertIn("is_blocking", first_recommendation)
                self.assertIn("priority_rank", first_recommendation)

                daily1 = self.runner.invoke(
                    app,
                    ["advisor", "autopilot", "run", "--cadence", "daily"],
                    env=self.env,
                )
                self.assertEqual(daily1.exit_code, 0, msg=daily1.output)
                daily1_payload = json.loads(daily1.output)
                self.assertFalse(bool(daily1_payload["reused"]))

                daily2 = self.runner.invoke(
                    app,
                    ["advisor", "autopilot", "run", "--cadence", "daily"],
                    env=self.env,
                )
                self.assertEqual(daily2.exit_code, 0, msg=daily2.output)
                daily2_payload = json.loads(daily2.output)
                self.assertTrue(bool(daily2_payload["reused"]))
                self.assertIn("evaluation", daily2_payload)
                self.assertIn("comparison_after", daily2_payload)

        conn = self._conn()
        try:
            rows = conn.execute(
                "SELECT cadence, COUNT(*) AS n FROM advisor_briefings GROUP BY cadence ORDER BY cadence"
            ).fetchall()
            counts = {str(r["cadence"]): int(r["n"]) for r in rows}
            self.assertEqual(counts.get("weekly"), 1)
            self.assertEqual(counts.get("daily"), 1)
            variants = conn.execute("SELECT COUNT(*) AS n FROM advisor_model_variants").fetchone()
            self.assertGreaterEqual(int(variants["n"] or 0), 2)
            regressions = conn.execute("SELECT COUNT(*) AS n FROM advisor_run_regressions").fetchone()
            self.assertGreaterEqual(int(regressions["n"] or 0), 2)
        finally:
            conn.close()
