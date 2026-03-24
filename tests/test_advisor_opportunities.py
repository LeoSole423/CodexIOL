import json
import os
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from typer.testing import CliRunner

from iol_cli.cli import app
from iol_cli.db import connect, init_db
from iol_cli.opportunities import allocate_with_caps
from tests_support import base_cli_env


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
        self.env = base_cli_env(self.db_path)
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
                    "--web-min-trusted-refs",
                    "0",
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
        self.assertIn("## Operables", text)
        self.assertIn("## Watchlist por falta de evidencia", text)
        self.assertIn("## Rechazados por riesgo/liquidez", text)
        self.assertIn("refs=0", text)
        self.assertIn("consensus=`insufficient`", text)

    def test_conflict_marks_manual_review(self):
        self._seed_portfolio(snapshot_date="2026-02-10")
        conn = self._conn()
        try:
            for i in range(20):
                day = f"2026-01-{12 + i:02d}"
                price = 120.0 - i
                conn.execute(
                    """
                    INSERT INTO market_symbol_snapshots(
                      snapshot_date,symbol,market,last_price,bid,ask,spread_pct,daily_var_pct,operations_count,volume_amount,source
                    ) VALUES(?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (day, "SPY", "bcba", price, price - 0.5, price + 0.5, 1.0, -0.2, 10.0, 50000.0, "quote"),
                )
            now = "2026-02-10T12:00:00Z"
            notes_bull = json.dumps(
                {
                    "expert_name": "Reuters Editorial",
                    "org": "Reuters",
                    "source_tier": "reuters",
                    "stance": "bullish",
                    "topic": "market_outlook",
                    "run_stage": "rerank",
                },
                ensure_ascii=True,
            )
            notes_bear = json.dumps(
                {
                    "expert_name": "Reuters Editorial",
                    "org": "Reuters",
                    "source_tier": "reuters",
                    "stance": "bearish",
                    "topic": "market_outlook",
                    "run_stage": "rerank",
                },
                ensure_ascii=True,
            )
            conn.execute(
                """
                INSERT INTO advisor_evidence(
                  created_at,symbol,query,source_name,source_url,published_date,retrieved_at_utc,claim,confidence,date_confidence,notes,conflict_key
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    now,
                    "SPY",
                    "SPY Reuters outlook",
                    "Reuters",
                    "https://www.reuters.com/a",
                    "2026-02-10",
                    now,
                    "Analyst sees upside",
                    "high",
                    "high",
                    notes_bull,
                    "SPY:reuters",
                ),
            )
            conn.execute(
                """
                INSERT INTO advisor_evidence(
                  created_at,symbol,query,source_name,source_url,published_date,retrieved_at_utc,claim,confidence,date_confidence,notes,conflict_key
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    now,
                    "SPY",
                    "SPY Reuters outlook",
                    "Reuters",
                    "https://www.reuters.com/b",
                    "2026-02-10",
                    now,
                    "Analyst warns downside",
                    "high",
                    "high",
                    notes_bear,
                    "SPY:reuters",
                ),
            )
            conn.commit()
        finally:
            conn.close()

        with patch("iol_cli.cli.collect_symbol_evidence", return_value=([], [])):
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
                    "--web-min-trusted-refs",
                    "0",
                ],
                env=self.env,
            )
        self.assertEqual(out.exit_code, 0, msg=out.output)
        conn = self._conn()
        try:
            row = conn.execute(
                """
                SELECT decision_gate, consensus_state, trusted_refs_count
                FROM advisor_opportunity_candidates
                WHERE symbol='SPY'
                ORDER BY id DESC
                LIMIT 1
                """
            ).fetchone()
            self.assertIsNotNone(row)
            self.assertEqual(str(row["decision_gate"]), "manual_review")
            self.assertEqual(str(row["consensus_state"]), "conflict")
            self.assertGreaterEqual(int(row["trusted_refs_count"] or 0), 2)
        finally:
            conn.close()

    def test_run_new_excludes_crypto_candidates_by_default(self):
        self._seed_portfolio(snapshot_date="2026-02-10")
        conn = self._conn()
        try:
            notes_reuters = json.dumps(
                {
                    "expert_name": "Reuters Editorial",
                    "org": "Reuters",
                    "source_tier": "reuters",
                    "stance": "bullish",
                    "topic": "market_outlook",
                    "run_stage": "rerank",
                },
                ensure_ascii=True,
            )
            conn.execute(
                """
                INSERT INTO market_symbol_snapshots(
                  snapshot_date,symbol,market,last_price,bid,ask,spread_pct,daily_var_pct,operations_count,volume_amount,source
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?)
                """,
                ("2026-02-10", "ETHA", "bcba", 50.0, 49.8, 50.2, 0.8, 0.0, 20.0, 150000.0, "quote"),
            )
            conn.execute(
                """
                INSERT INTO market_symbol_snapshots(
                  snapshot_date,symbol,market,last_price,bid,ask,spread_pct,daily_var_pct,operations_count,volume_amount,source
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?)
                """,
                ("2026-02-10", "AAPL", "bcba", 100.0, 99.5, 100.5, 1.0, 0.0, 25.0, 250000.0, "quote"),
            )
            conn.execute(
                """
                INSERT INTO advisor_evidence(
                  created_at,symbol,query,source_name,source_url,published_date,retrieved_at_utc,claim,confidence,date_confidence,notes,conflict_key
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    "2026-02-10T12:00:00Z",
                    "AAPL",
                    "AAPL outlook",
                    "Reuters",
                    "https://www.reuters.com/aapl",
                    "2026-02-10",
                    "2026-02-10T12:00:00Z",
                    "Technology demand remains resilient",
                    "high",
                    "high",
                    notes_reuters,
                    "AAPL:reuters",
                ),
            )
            conn.commit()
        finally:
            conn.close()

        with patch("iol_cli.cli.collect_symbol_evidence", return_value=([], [])):
            out = self.runner.invoke(
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
                    "5",
                    "--web-min-trusted-refs",
                    "0",
                ],
                env=self.env,
            )
        self.assertEqual(out.exit_code, 0, msg=out.output)
        payload = json.loads(out.output)
        syms = [str(r.get("symbol") or "") for r in (payload.get("top_operable") or [])]
        self.assertIn("AAPL", syms)
        self.assertNotIn("ETHA", syms)

        conn = self._conn()
        try:
            row = conn.execute(
                """
                SELECT filters_passed, risk_flags_json
                FROM advisor_opportunity_candidates
                WHERE symbol='ETHA'
                ORDER BY id DESC
                LIMIT 1
                """
            ).fetchone()
            self.assertIsNotNone(row)
            self.assertEqual(int(row["filters_passed"]), 0)
            self.assertIn("CRYPTO_EXCLUDED", str(row["risk_flags_json"] or ""))
        finally:
            conn.close()

    def test_sector_diversification_in_top_operable(self):
        self._seed_portfolio(snapshot_date="2026-02-10")
        conn = self._conn()
        try:
            for sym, ops, vol, price in (
                ("AAPL", 25.0, 250000.0, 100.0),
                ("ADBE", 22.0, 220000.0, 100.0),
                ("AVGO", 20.0, 210000.0, 100.0),
                ("ABBV", 5.0, 60000.0, 100.0),
            ):
                conn.execute(
                    """
                    INSERT INTO market_symbol_snapshots(
                      snapshot_date,symbol,market,last_price,bid,ask,spread_pct,daily_var_pct,operations_count,volume_amount,source
                    ) VALUES(?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    ("2026-02-10", sym, "bcba", price, price - 0.5, price + 0.5, 1.0, 0.0, ops, vol, "quote"),
                )
            now = "2026-02-10T12:00:00Z"
            claims = {
                "AAPL": "Technology software demand outlook",
                "ADBE": "Cloud software momentum remains strong",
                "AVGO": "Semiconductor demand remains resilient",
                "ABBV": "Pharmaceutical drug pipeline update",
            }
            for sym, claim in claims.items():
                notes = json.dumps(
                    {
                        "expert_name": "Reuters Editorial",
                        "org": "Reuters",
                        "source_tier": "reuters",
                        "stance": "bullish",
                        "topic": "market_outlook",
                        "run_stage": "rerank",
                    },
                    ensure_ascii=True,
                )
                conn.execute(
                    """
                    INSERT INTO advisor_evidence(
                      created_at,symbol,query,source_name,source_url,published_date,retrieved_at_utc,claim,confidence,date_confidence,notes,conflict_key
                    ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        now,
                        sym,
                        f"{sym} outlook",
                        "Reuters",
                        f"https://www.reuters.com/{sym.lower()}",
                        "2026-02-10",
                        now,
                        claim,
                        "high",
                        "high",
                        notes,
                        f"{sym}:reuters",
                    ),
                )
            conn.commit()
        finally:
            conn.close()

        with patch("iol_cli.cli.collect_symbol_evidence", return_value=([], [])):
            out = self.runner.invoke(
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
                    "3",
                    "--web-min-trusted-refs",
                    "0",
                    "--max-per-sector",
                    "2",
                ],
                env=self.env,
            )
        self.assertEqual(out.exit_code, 0, msg=out.output)
        payload = json.loads(out.output)
        top = payload.get("top_operable") or []
        self.assertEqual(len(top), 3)
        top_syms = [str(r.get("symbol") or "") for r in top]
        self.assertIn("ABBV", top_syms)
        tech_count = sum(1 for r in top if str(r.get("sector_bucket") or "") == "technology")
        self.assertLessEqual(tech_count, 2)

    def test_new_candidate_without_fresh_trusted_refs_goes_to_watchlist(self):
        self._seed_portfolio(snapshot_date="2026-02-10")
        conn = self._conn()
        try:
            conn.execute(
                """
                INSERT INTO market_symbol_snapshots(
                  snapshot_date,symbol,market,last_price,bid,ask,spread_pct,daily_var_pct,operations_count,volume_amount,source
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?)
                """,
                ("2026-02-10", "AAPL", "bcba", 100.0, 99.5, 100.5, 1.0, 0.0, 25.0, 250000.0, "quote"),
            )
            conn.commit()
        finally:
            conn.close()

        with patch("iol_cli.cli.collect_symbol_evidence", return_value=([], [])):
            out = self.runner.invoke(
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
                    "5",
                ],
                env=self.env,
            )
        self.assertEqual(out.exit_code, 0, msg=out.output)
        payload = json.loads(out.output)
        self.assertEqual(payload.get("top_operable") or [], [])
        watchlist = payload.get("watchlist") or []
        self.assertTrue(any(str(r.get("symbol") or "") == "AAPL" for r in watchlist))

    def test_mode_both_includes_sell_signal_for_deteriorating_holding(self):
        self._seed_portfolio(snapshot_date="2026-02-10")
        conn = self._conn()
        try:
            for i in range(20):
                day = f"2026-01-{12 + i:02d}"
                price = 140.0 - (i * 3.0)
                conn.execute(
                    """
                    INSERT INTO market_symbol_snapshots(
                      snapshot_date,symbol,market,last_price,bid,ask,spread_pct,daily_var_pct,operations_count,volume_amount,source
                    ) VALUES(?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (day, "SPY", "bcba", price, price - 0.5, price + 0.5, 1.0, -1.0, 18.0, 120000.0, "quote"),
                )
            conn.commit()
        finally:
            conn.close()

        with patch("iol_cli.cli.collect_symbol_evidence", return_value=([], [])):
            out = self.runner.invoke(
                app,
                [
                    "advisor",
                    "opportunities",
                    "run",
                    "--mode",
                    "both",
                    "--variant",
                    "active",
                    "--as-of",
                    "2026-02-10",
                    "--budget-ars",
                    "100000",
                    "--top",
                    "5",
                    "--web-min-trusted-refs",
                    "0",
                ],
                env=self.env,
            )
        self.assertEqual(out.exit_code, 0, msg=out.output)
        payload = json.loads(out.output)
        top = payload.get("top_operable") or []
        self.assertTrue(any(str(r.get("symbol") or "") == "SPY" and str(r.get("signal_side") or "") == "sell" for r in top))

        conn = self._conn()
        try:
            row = conn.execute(
                """
                SELECT signal_side, signal_family, score_version, holding_context_json, score_features_json
                FROM advisor_opportunity_candidates
                WHERE symbol='SPY'
                ORDER BY id DESC
                LIMIT 1
                """
            ).fetchone()
            self.assertIsNotNone(row)
            self.assertEqual(str(row["signal_side"]), "sell")
            self.assertIn(str(row["signal_family"]), ("trim", "exit"))
            self.assertTrue(str(row["score_version"]))
            self.assertIn("age_days", str(row["holding_context_json"] or ""))
            self.assertIn("thesis_deterioration", str(row["score_features_json"] or ""))
        finally:
            conn.close()

    def test_evaluate_and_scorecard_create_outcomes(self):
        self._seed_portfolio(snapshot_date="2026-02-10")
        conn = self._conn()
        try:
            for idx, (snap, spy, aapl) in enumerate(
                [
                    ("2026-02-10", 100.0, 100.0),
                    ("2026-02-11", 98.0, 103.0),
                    ("2026-02-15", 92.0, 108.0),
                    ("2026-03-02", 85.0, 112.0),
                ]
            ):
                conn.execute(
                    """
                    INSERT INTO market_symbol_snapshots(
                      snapshot_date,symbol,market,last_price,bid,ask,spread_pct,daily_var_pct,operations_count,volume_amount,source
                    ) VALUES(?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (snap, "SPY", "bcba", spy, spy - 0.5, spy + 0.5, 1.0, -0.5, 25.0, 140000.0, "quote"),
                )
                conn.execute(
                    """
                    INSERT INTO market_symbol_snapshots(
                      snapshot_date,symbol,market,last_price,bid,ask,spread_pct,daily_var_pct,operations_count,volume_amount,source
                    ) VALUES(?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (snap, "AAPL", "bcba", aapl, aapl - 0.5, aapl + 0.5, 1.0, 0.5, 25.0, 180000.0, "quote"),
                )
            conn.commit()
        finally:
            conn.close()

        with patch("iol_cli.cli.collect_symbol_evidence", return_value=([], [])):
            run = self.runner.invoke(
                app,
                [
                    "advisor",
                    "opportunities",
                    "run",
                    "--mode",
                    "both",
                    "--variant",
                    "active",
                    "--as-of",
                    "2026-02-10",
                    "--budget-ars",
                    "100000",
                    "--top",
                    "5",
                    "--web-min-trusted-refs",
                    "0",
                ],
                env=self.env,
            )
        self.assertEqual(run.exit_code, 0, msg=run.output)

        evaluated = self.runner.invoke(
            app,
            ["advisor", "opportunities", "evaluate", "--as-of", "2026-03-02"],
            env=self.env,
        )
        self.assertEqual(evaluated.exit_code, 0, msg=evaluated.output)
        eval_payload = json.loads(evaluated.output)
        self.assertGreater(int(eval_payload.get("inserted") or 0), 0)

        scorecard = self.runner.invoke(
            app,
            ["advisor", "opportunities", "scorecard", "--as-of", "2026-03-02", "--window-days", "90"],
            env=self.env,
        )
        self.assertEqual(scorecard.exit_code, 0, msg=scorecard.output)
        score_payload = json.loads(scorecard.output)
        self.assertIn("active_scorecard", score_payload)
        self.assertGreaterEqual(int((score_payload.get("active_scorecard") or {}).get("sample_count") or 0), 1)

        conn = self._conn()
        try:
            row = conn.execute(
                """
                SELECT signal_side, horizon, eval_status, forward_return_pct, excess_return_pct
                FROM advisor_signal_outcomes
                WHERE symbol='SPY'
                ORDER BY id ASC
                LIMIT 1
                """
            ).fetchone()
            self.assertIsNotNone(row)
            self.assertEqual(str(row["signal_side"]), "sell")
            self.assertEqual(str(row["eval_status"]), "ok")
            self.assertIsNotNone(row["forward_return_pct"])
            self.assertIsNotNone(row["excess_return_pct"])
        finally:
            conn.close()


class TestResolveConflicts(unittest.TestCase):
    """Tests for resolve_conflicts() — the post-processing layer that eliminates
    simultaneous buy/sell signals for the same symbol."""

    def _make_candidate(self, symbol, signal_side, score, cur_weight=10.0, status="operable"):
        from iol_cli.opportunities import OpportunityCandidate
        return OpportunityCandidate(
            symbol=symbol,
            candidate_type="rebuy" if signal_side == "buy" else "trim",
            signal_side=signal_side,
            signal_family="rebuy" if signal_side == "buy" else "exit",
            score_version="baseline_v1",
            score_total=score,
            score_risk=score,
            score_value=score,
            score_momentum=score,
            score_catalyst=score,
            entry_low=None,
            entry_high=None,
            suggested_weight_pct=None,
            suggested_amount_ars=None,
            reason_summary=f"{symbol}:{signal_side}",
            risk_flags_json="[]",
            filters_passed=1,
            current_weight_pct=cur_weight,
            expert_signal_score=50.0,
            trusted_refs_count=0,
            consensus_state="insufficient",
            decision_gate="auto",
            candidate_status=status,
            evidence_summary_json="{}",
            liquidity_score=80.0,
            sector_bucket="financials",
            is_crypto_proxy=0,
            holding_context_json="{}",
            score_features_json="{}",
        )

    def test_underweight_suppresses_sell(self):
        """current_weight < target - 2pp → sell suppressed, buy survives."""
        from iol_cli.opportunities import resolve_conflicts
        # EEM at 0% with target 8% → clearly underweight (-8pp)
        buy = self._make_candidate("EEM", "buy", score=38.0, cur_weight=0.0)
        sell = self._make_candidate("EEM", "sell", score=63.0, cur_weight=0.0)
        result = resolve_conflicts([buy, sell], target_weights_by_symbol={"EEM": 8.0})
        by_side = {c.signal_side: c for c in result if c.symbol == "EEM"}
        self.assertEqual(by_side["buy"].candidate_status, "operable")
        self.assertEqual(by_side["sell"].candidate_status, "suppressed")
        self.assertIn("target_underweight", by_side["sell"].reason_summary)

    def test_overweight_suppresses_buy(self):
        """current_weight > target + 2pp → buy suppressed, sell survives."""
        from iol_cli.opportunities import resolve_conflicts
        buy = self._make_candidate("ACWI", "buy", score=70.0, cur_weight=22.0)
        sell = self._make_candidate("ACWI", "sell", score=45.0, cur_weight=22.0)
        result = resolve_conflicts([buy, sell], target_weights_by_symbol={"ACWI": 17.0})
        by_side = {c.signal_side: c for c in result if c.symbol == "ACWI"}
        self.assertEqual(by_side["sell"].candidate_status, "operable")
        self.assertEqual(by_side["buy"].candidate_status, "suppressed")
        self.assertIn("target_overweight", by_side["buy"].reason_summary)

    def test_on_track_suppresses_both(self):
        """current_weight within ±2pp of target → both suppressed."""
        from iol_cli.opportunities import resolve_conflicts
        buy = self._make_candidate("GLD", "buy", score=70.0, cur_weight=15.5)
        sell = self._make_candidate("GLD", "sell", score=45.0, cur_weight=15.5)
        result = resolve_conflicts([buy, sell], target_weights_by_symbol={"GLD": 16.0})
        by_side = {c.signal_side: c for c in result if c.symbol == "GLD"}
        self.assertEqual(by_side["buy"].candidate_status, "suppressed")
        self.assertEqual(by_side["sell"].candidate_status, "suppressed")
        self.assertIn("target_on_track", by_side["buy"].reason_summary)

    def test_score_dominant_buy_wins(self):
        """No target: buy score >> sell score (diff >= 20) → buy kept."""
        from iol_cli.opportunities import resolve_conflicts
        buy = self._make_candidate("EEM", "buy", score=70.0)
        sell = self._make_candidate("EEM", "sell", score=40.0)
        result = resolve_conflicts([buy, sell])
        by_side = {c.signal_side: c for c in result if c.symbol == "EEM"}
        self.assertEqual(by_side["buy"].candidate_status, "operable")
        self.assertEqual(by_side["sell"].candidate_status, "suppressed")
        self.assertIn("score_dominant_buy", by_side["sell"].reason_summary)

    def test_score_dominant_sell_wins(self):
        """No target: sell score >> buy score (diff >= 20) → sell kept."""
        from iol_cli.opportunities import resolve_conflicts
        buy = self._make_candidate("IBIT", "buy", score=35.0)
        sell = self._make_candidate("IBIT", "sell", score=65.0)
        result = resolve_conflicts([buy, sell])
        by_side = {c.signal_side: c for c in result if c.symbol == "IBIT"}
        self.assertEqual(by_side["sell"].candidate_status, "operable")
        self.assertEqual(by_side["buy"].candidate_status, "suppressed")
        self.assertIn("score_dominant_sell", by_side["buy"].reason_summary)

    def test_tie_suppresses_both(self):
        """No target: scores too close (diff < 20) → both suppressed."""
        from iol_cli.opportunities import resolve_conflicts
        buy = self._make_candidate("AL30", "buy", score=55.0)
        sell = self._make_candidate("AL30", "sell", score=50.0)
        result = resolve_conflicts([buy, sell])
        by_side = {c.signal_side: c for c in result if c.symbol == "AL30"}
        self.assertEqual(by_side["buy"].candidate_status, "suppressed")
        self.assertEqual(by_side["sell"].candidate_status, "suppressed")
        self.assertIn("suppressed_tie", by_side["buy"].reason_summary)

    def test_no_conflict_passes_through(self):
        """Symbol with only buy signal is unchanged."""
        from iol_cli.opportunities import resolve_conflicts
        buy = self._make_candidate("TO26", "buy", score=60.0)
        result = resolve_conflicts([buy])
        self.assertEqual(result[0].candidate_status, "operable")
        self.assertEqual(result[0].signal_side, "buy")

    def test_multiple_symbols_resolved_independently(self):
        """Each symbol resolves independently without cross-contamination."""
        from iol_cli.opportunities import resolve_conflicts
        # EEM at 0% vs target 8% → clearly underweight
        eem_buy = self._make_candidate("EEM", "buy", score=38.0, cur_weight=0.0)
        eem_sell = self._make_candidate("EEM", "sell", score=63.0, cur_weight=0.0)
        # GLD at 12% vs target 16% → clearly underweight
        gld_buy = self._make_candidate("GLD", "buy", score=70.0, cur_weight=12.0)
        gld_sell = self._make_candidate("GLD", "sell", score=30.0, cur_weight=12.0)
        result = resolve_conflicts(
            [eem_buy, eem_sell, gld_buy, gld_sell],
            target_weights_by_symbol={"EEM": 8.0, "GLD": 16.0},
        )
        by_sym_side = {(c.symbol, c.signal_side): c for c in result}
        # EEM underweight → buy survives
        self.assertEqual(by_sym_side[("EEM", "buy")].candidate_status, "operable")
        self.assertEqual(by_sym_side[("EEM", "sell")].candidate_status, "suppressed")
        # GLD underweight → buy survives
        self.assertEqual(by_sym_side[("GLD", "buy")].candidate_status, "operable")
        self.assertEqual(by_sym_side[("GLD", "sell")].candidate_status, "suppressed")


class TestConcentrationWithTarget(unittest.TestCase):
    """Tests for target-aware CONCENTRATION_MAX in build_candidates()."""

    def _minimal_build(self, symbol, cur_weight_pct, target_weights=None):
        """Run build_candidates() with a single symbol and return the candidate."""
        from iol_cli.opportunities import build_candidates
        portfolio_total = 3_000_000.0
        holding_value = portfolio_total * cur_weight_pct / 100.0
        metrics = {
            symbol: {
                "last_price": 100.0,
                "bid": 99.0,
                "ask": 101.0,
                "spread_pct": 0.5,
                "operations_count": 20,
                "volume_amount": 500_000.0,
            }
        }
        series = {symbol: [("2026-03-01", 105.0), ("2026-03-10", 100.0), ("2026-03-20", 100.0)]}
        return build_candidates(
            as_of="2026-03-20",
            mode="both",
            budget_ars=200_000.0,
            top_n=10,
            portfolio_total_ars=portfolio_total,
            holdings_value_by_symbol={symbol: holding_value},
            latest_metrics=metrics,
            series_by_symbol=series,
            evidence_by_symbol={symbol: []},
            holdings_context_by_symbol={symbol: {"gain_pct": 5.0, "age_days": 30}},
            target_weights_by_symbol=target_weights,
        )

    def test_spy_at_target_no_concentration_max(self):
        """SPY at 30.8% with target 32% should NOT trigger CONCENTRATION_MAX."""
        candidates = self._minimal_build("SPY", 30.8, target_weights={"SPY": 32.0})
        for c in candidates:
            if c.symbol == "SPY":
                self.assertNotIn("CONCENTRATION_MAX", c.risk_flags_json,
                    msg=f"SPY at 30.8% with target 32% incorrectly flagged CONCENTRATION_MAX: {c.reason_summary}")

    def test_spy_far_above_target_triggers_concentration_max(self):
        """SPY at 40% with target 32% (40 > 32+5=37) should trigger CONCENTRATION_MAX."""
        candidates = self._minimal_build("SPY", 40.0, target_weights={"SPY": 32.0})
        sell_candidates = [c for c in candidates if c.symbol == "SPY" and c.signal_side == "sell"]
        # At 40% weight, the buy should be rejected via CONCENTRATION_MAX
        buy_candidates = [c for c in candidates if c.symbol == "SPY" and c.signal_side == "buy"]
        for c in buy_candidates:
            self.assertIn("CONCENTRATION_MAX", c.risk_flags_json)

    def test_overweight_by_3pp_adds_overweight_flag(self):
        """ACWI at 20% with target 17% (overweight by 3pp) adds OVERWEIGHT_TARGET, not CONCENTRATION_MAX."""
        candidates = self._minimal_build("ACWI", 20.0, target_weights={"ACWI": 17.0})
        for c in candidates:
            if c.symbol == "ACWI" and c.signal_side == "buy":
                self.assertNotIn("CONCENTRATION_MAX", c.risk_flags_json)
                self.assertIn("OVERWEIGHT_TARGET", c.risk_flags_json)


class TestEngineSignalsToEvidence(unittest.TestCase):
    """Tests for engine_signals_to_evidence() in the adapter."""

    def _make_regime(self, regime, score=70.0):
        from iol_engines.signals import RegimeSignal
        return RegimeSignal(
            as_of="2026-03-20",
            regime=regime,
            confidence=0.8,
            regime_score=score,
            favored_asset_classes=["equity"] if regime == "bull" else ["gold", "cash"],
            defensive_weight_adjustment=0.0 if regime == "bull" else -0.1,
            breadth_score=60.0,
            volatility_regime="normal",
        )

    def _make_macro(self, global_risk_on=50.0, ar_stress=50.0):
        from iol_engines.signals import MacroSignal
        return MacroSignal(as_of="2026-03-20", argentina_macro_stress=ar_stress, global_risk_on=global_risk_on)

    def _make_sm(self, symbol, direction, conviction=75.0):
        from iol_engines.signals import SmartMoneySignal
        return SmartMoneySignal(
            as_of="2026-03-20",
            symbol=symbol,
            net_institutional_direction=direction,
            conviction_score=conviction,
        )

    def test_bull_regime_generates_bullish_for_equity(self):
        from iol_engines.opportunity.adapter import engine_signals_to_evidence
        rows = engine_signals_to_evidence(
            regime=self._make_regime("bull"),
            macro=None,
            smart_money=None,
            as_of="2026-03-20",
            portfolio_symbols=["SPY", "ACWI", "GLD"],
        )
        spy_rows = [r for r in rows if r["symbol"] == "SPY"]
        self.assertTrue(len(spy_rows) >= 1)
        import json as _json
        for r in spy_rows:
            notes = _json.loads(r["notes"])
            self.assertEqual(notes["stance"], "bullish")
            self.assertEqual(notes["source_tier"], "official")

    def test_bear_regime_generates_bearish_for_equity(self):
        from iol_engines.opportunity.adapter import engine_signals_to_evidence
        rows = engine_signals_to_evidence(
            regime=self._make_regime("bear"),
            macro=None,
            smart_money=None,
            as_of="2026-03-20",
            portfolio_symbols=["SPY", "GLD"],
        )
        spy_rows = [r for r in rows if r["symbol"] == "SPY"]
        self.assertTrue(len(spy_rows) >= 1)
        import json as _json
        for r in spy_rows:
            self.assertEqual(_json.loads(r["notes"])["stance"], "bearish")

    def test_bear_regime_bullish_for_defensive(self):
        from iol_engines.opportunity.adapter import engine_signals_to_evidence
        rows = engine_signals_to_evidence(
            regime=self._make_regime("crisis"),
            macro=None,
            smart_money=None,
            as_of="2026-03-20",
            portfolio_symbols=["GLD", "SPY"],
        )
        import json as _json
        gld_rows = [r for r in rows if r["symbol"] == "GLD"]
        stances = [_json.loads(r["notes"])["stance"] for r in gld_rows]
        self.assertIn("bullish", stances)

    def test_smart_money_accumulate_generates_bullish(self):
        from iol_engines.opportunity.adapter import engine_signals_to_evidence
        rows = engine_signals_to_evidence(
            regime=None,
            macro=None,
            smart_money=[self._make_sm("SPY", "accumulate", conviction=80.0)],
            as_of="2026-03-20",
            portfolio_symbols=["SPY"],
        )
        spy_rows = [r for r in rows if r["symbol"] == "SPY"]
        self.assertEqual(len(spy_rows), 1)
        import json as _json
        self.assertEqual(_json.loads(spy_rows[0]["notes"])["stance"], "bullish")
        self.assertEqual(spy_rows[0]["confidence"], "high")

    def test_smart_money_distribute_generates_bearish(self):
        from iol_engines.opportunity.adapter import engine_signals_to_evidence
        rows = engine_signals_to_evidence(
            regime=None,
            macro=None,
            smart_money=[self._make_sm("GLD", "distribute", conviction=75.0)],
            as_of="2026-03-20",
            portfolio_symbols=["GLD"],
        )
        gld_rows = [r for r in rows if r["symbol"] == "GLD"]
        self.assertEqual(len(gld_rows), 1)
        import json as _json
        self.assertEqual(_json.loads(gld_rows[0]["notes"])["stance"], "bearish")

    def test_low_conviction_smart_money_ignored(self):
        from iol_engines.opportunity.adapter import engine_signals_to_evidence
        rows = engine_signals_to_evidence(
            regime=None,
            macro=None,
            smart_money=[self._make_sm("SPY", "accumulate", conviction=45.0)],
            as_of="2026-03-20",
            portfolio_symbols=["SPY"],
        )
        self.assertEqual(len(rows), 0)

    def test_high_ar_stress_bearish_for_sovereign(self):
        from iol_engines.opportunity.adapter import engine_signals_to_evidence
        rows = engine_signals_to_evidence(
            regime=None,
            macro=self._make_macro(ar_stress=80.0),
            smart_money=None,
            as_of="2026-03-20",
            portfolio_symbols=["TO26", "AL30", "SPY"],
        )
        import json as _json
        ar_rows = [r for r in rows if r["symbol"] in ("TO26", "AL30")]
        self.assertTrue(len(ar_rows) >= 1)
        for r in ar_rows:
            self.assertEqual(_json.loads(r["notes"])["stance"], "bearish")

    def test_no_signals_returns_empty(self):
        from iol_engines.opportunity.adapter import engine_signals_to_evidence
        rows = engine_signals_to_evidence(
            regime=None, macro=None, smart_money=None,
            as_of="2026-03-20", portfolio_symbols=["SPY", "GLD"],
        )
        self.assertEqual(rows, [])


if __name__ == "__main__":
    unittest.main()
