import os
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from typer.testing import CliRunner

from iol_cli.cli import app
from iol_cli.db import connect, init_db
from iol_cli.opportunities import allocate_with_caps


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
        if symbol not in self._quotes:
            raise RuntimeError(f"missing quote for {symbol}")
        return self._quotes[symbol]


class TestAdvisorOpportunities(unittest.TestCase):
    def setUp(self):
        self.runner = CliRunner()
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.tmp.name, "opportunities.db")
        self.env = _base_env(self.db_path)
        conn = connect(self.db_path)
        init_db(conn)
        conn.close()

    def tearDown(self):
        self.tmp.cleanup()

    def _conn(self):
        c = sqlite3.connect(self.db_path)
        c.row_factory = sqlite3.Row
        return c

    def _seed_portfolio(self, snapshot_date="2026-02-10"):
        conn = self._conn()
        try:
            conn.execute(
                """
                INSERT OR REPLACE INTO portfolio_snapshots(
                  snapshot_date,total_value,currency,titles_value,cash_disponible_ars,cash_disponible_usd,retrieved_at,source
                ) VALUES(?,?,?,?,?,?,?,?)
                """,
                (snapshot_date, 1000000.0, "peso_Argentino", 900000.0, 100000.0, 0.0, "2026-02-10T17:00:00Z", "test"),
            )
            conn.execute(
                """
                INSERT OR REPLACE INTO portfolio_assets(
                  snapshot_date,symbol,description,market,type,currency,plazo,quantity,last_price,ppc,total_value,daily_var_pct
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (snapshot_date, "SPY", "SPY", "bcba", "CEDEARS", "peso_Argentino", "t1", 10, 100.0, 95.0, 100000.0, 0.5),
            )
            conn.commit()
        finally:
            conn.close()

    def test_allocate_with_caps_respects_caps_and_total(self):
        alloc = allocate_with_caps({"A": 1.0, "B": 1.0}, {"A": 40.0, "B": 100.0})
        self.assertLessEqual(alloc["A"], 40.0 + 1e-9)
        self.assertAlmostEqual(alloc["A"] + alloc["B"], 100.0, places=6)

    def test_evidence_add_and_list(self):
        add = self.runner.invoke(
            app,
            [
                "advisor",
                "evidence",
                "add",
                "--symbol",
                "SPY",
                "--query",
                "SPY expense ratio",
                "--source-name",
                "Issuer",
                "--source-url",
                "https://example.com/spy",
                "--claim",
                "Expense ratio remains low",
                "--confidence",
                "high",
                "--date-confidence",
                "high",
            ],
            env=self.env,
        )
        self.assertEqual(add.exit_code, 0, msg=add.output)
        listed = self.runner.invoke(app, ["advisor", "evidence", "list", "--symbol", "SPY"], env=self.env)
        self.assertEqual(listed.exit_code, 0, msg=listed.output)
        self.assertIn("Expense ratio remains low", listed.output)

    def test_evidence_fetch_from_context_inserts_rows(self):
        self._seed_portfolio(snapshot_date="2026-02-10")
        fake_rows = [
            {
                "symbol": "SPY",
                "query": "SPY filings/news",
                "source_name": "TestSource",
                "source_url": "https://example.com/spy-news",
                "published_date": "2026-02-09",
                "retrieved_at_utc": "2026-02-10T12:00:00Z",
                "claim": "Headline claim",
                "confidence": "medium",
                "date_confidence": "high",
                "notes": "test",
                "conflict_key": "SPY:test",
            }
        ]
        with patch("iol_cli.cli.collect_symbol_evidence", return_value=(fake_rows, [])) as mock_collect:
            out = self.runner.invoke(
                app,
                [
                    "advisor",
                    "evidence",
                    "fetch",
                    "--as-of",
                    "2026-02-10",
                    "--max-symbols",
                    "1",
                ],
                env=self.env,
            )
        self.assertEqual(out.exit_code, 0, msg=out.output)
        self.assertGreaterEqual(mock_collect.call_count, 1)

        conn = self._conn()
        try:
            row = conn.execute(
                """
                SELECT symbol, source_name, claim
                FROM advisor_evidence
                WHERE symbol = 'SPY'
                ORDER BY id DESC
                LIMIT 1
                """
            ).fetchone()
            self.assertIsNotNone(row)
            self.assertEqual(row["source_name"], "TestSource")
            self.assertEqual(row["claim"], "Headline claim")
        finally:
            conn.close()

    def test_snapshot_universe_upsert_no_duplicates(self):
        self._seed_portfolio(snapshot_date="2026-02-10")
        fake = _FakeClient()
        with patch("iol_cli.cli._get_client", return_value=fake):
            out1 = self.runner.invoke(
                app,
                ["advisor", "opportunities", "snapshot-universe", "--as-of", "2026-02-10", "--universe", "bcba_cedears"],
                env=self.env,
            )
            self.assertEqual(out1.exit_code, 0, msg=out1.output)
            out2 = self.runner.invoke(
                app,
                ["advisor", "opportunities", "snapshot-universe", "--as-of", "2026-02-10", "--universe", "bcba_cedears"],
                env=self.env,
            )
            self.assertEqual(out2.exit_code, 0, msg=out2.output)

        conn = self._conn()
        try:
            r = conn.execute(
                """
                SELECT COUNT(*) AS n
                FROM market_symbol_snapshots
                WHERE snapshot_date='2026-02-10' AND symbol='SPY' AND source='quote'
                """
            ).fetchone()
            self.assertEqual(int(r["n"]), 1)
        finally:
            conn.close()

    def test_run_mode_rebuy_buy_the_dip(self):
        self._seed_portfolio(snapshot_date="2026-02-10")
        conn = self._conn()
        try:
            for i in range(20):
                day = f"2026-01-{12 + i:02d}" if i < 20 else "2026-02-10"
                price = 120.0 - i  # drawdown vs max
                conn.execute(
                    """
                    INSERT INTO market_symbol_snapshots(
                      snapshot_date,symbol,market,last_price,bid,ask,spread_pct,daily_var_pct,operations_count,volume_amount,source
                    ) VALUES(?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (day, "SPY", "bcba", price, price - 0.5, price + 0.5, 1.0, -0.2, 10.0, 50000.0, "quote"),
                )
            conn.execute(
                """
                INSERT INTO advisor_evidence(
                  created_at,symbol,query,source_name,source_url,published_date,retrieved_at_utc,claim,confidence,date_confidence,notes,conflict_key
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    "2026-02-10T00:00:00Z",
                    "SPY",
                    "SPY outlook",
                    "Issuer",
                    "https://example.com",
                    "2026-02-01",
                    "2026-02-10T12:00:00Z",
                    "Guidance stable",
                    "high",
                    "high",
                    None,
                    "guidance",
                ),
            )
            conn.commit()
        finally:
            conn.close()

        with patch("iol_cli.cli.collect_symbol_evidence", return_value=([], [])) as mock_collect:
            out = self.runner.invoke(
                app,
                [
                    "advisor",
                    "opportunities",
                    "run",
                    "--mode",
                    "rebuy",
                    "--as-of",
                    "2026-02-10",
                    "--budget-ars",
                    "100000",
                    "--top",
                    "5",
                ],
                env=self.env,
            )
        self.assertEqual(out.exit_code, 0, msg=out.output)
        self.assertGreaterEqual(mock_collect.call_count, 1)
        self.assertIn('"candidate_type": "rebuy"', out.output)

    def test_hard_filter_spread_and_report(self):
        self._seed_portfolio(snapshot_date="2026-02-10")
        conn = self._conn()
        try:
            conn.execute(
                """
                INSERT INTO market_symbol_snapshots(
                  snapshot_date,symbol,market,last_price,bid,ask,spread_pct,daily_var_pct,operations_count,volume_amount,source
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?)
                """,
                ("2026-02-10", "AAA", "bcba", 100.0, 90.0, 110.0, 20.0, 0.0, 30.0, 100000.0, "quote"),
            )
            conn.execute(
                """
                INSERT INTO market_symbol_snapshots(
                  snapshot_date,symbol,market,last_price,bid,ask,spread_pct,daily_var_pct,operations_count,volume_amount,source
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?)
                """,
                ("2026-02-10", "BBB", "bcba", 100.0, 99.5, 100.5, 1.0, 0.0, 30.0, 100000.0, "quote"),
            )
            conn.commit()
        finally:
            conn.close()

        with patch("iol_cli.cli.collect_symbol_evidence", return_value=([], [])) as mock_collect:
            run = self.runner.invoke(
                app,
                [
                    "advisor",
                    "opportunities",
                    "run",
                    "--mode",
                    "new",
                    "--as-of",
                    "2026-02-10",
                    "--budget-ars",
                    "100000",
                    "--top",
                    "10",
                ],
                env=self.env,
            )
        self.assertEqual(run.exit_code, 0, msg=run.output)
        self.assertGreaterEqual(mock_collect.call_count, 1)
        conn = self._conn()
        try:
            row = conn.execute(
                "SELECT id FROM advisor_opportunity_runs ORDER BY id DESC LIMIT 1"
            ).fetchone()
            self.assertIsNotNone(row)
            rid = int(row["id"])
        finally:
            conn.close()

        out_md = os.path.join(self.tmp.name, "Oportunidades.md")
        report = self.runner.invoke(
            app, ["advisor", "opportunities", "report", "--run-id", str(rid), "--out", out_md], env=self.env
        )
        self.assertEqual(report.exit_code, 0, msg=report.output)
        self.assertTrue(os.path.exists(out_md))
        with open(out_md, "r", encoding="utf-8") as f:
            text = f.read()
        self.assertIn("Top candidatos operables", text)


if __name__ == "__main__":
    unittest.main()
