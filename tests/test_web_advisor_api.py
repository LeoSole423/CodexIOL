import json
import unittest

from iol_web.routes_api import advisor_history, advisor_latest, advisor_opportunities_latest
from tests_support import InitDbTestCase


class TestWebAdvisorApi(InitDbTestCase):
    def test_advisor_endpoints_return_latest_briefing_and_run(self):
        conn = self.connect()
        try:
            conn.execute(
                """
                INSERT INTO advisor_opportunity_runs(
                  created_at_utc,as_of,mode,universe,budget_ars,top_n,status,error_message,config_json,pipeline_warnings_json,run_metrics_json
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?)
                """,
                ("2026-03-11T21:00:00Z", "2026-03-11", "both", "bcba_cedears", 100000.0, 5, "ok", None, "{}", "[]", "{}"),
            )
            run_id = int(conn.execute("SELECT id FROM advisor_opportunity_runs ORDER BY id DESC LIMIT 1").fetchone()["id"])
            conn.execute(
                """
                INSERT INTO advisor_opportunity_candidates(
                  run_id,symbol,candidate_type,score_total,score_risk,score_value,score_momentum,score_catalyst,
                  entry_low,entry_high,suggested_weight_pct,suggested_amount_ars,reason_summary,risk_flags_json,filters_passed,
                  expert_signal_score,trusted_refs_count,consensus_state,decision_gate,candidate_status,evidence_summary_json
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    run_id,
                    "SPY",
                    "new",
                    72.5,
                    70.0,
                    65.0,
                    80.0,
                    75.0,
                    99.0,
                    101.0,
                    100.0,
                    100000.0,
                    "reason",
                    "[]",
                    1,
                    60.0,
                    2,
                    "aligned",
                    "auto",
                    "operable",
                    json.dumps({"fresh_trusted_refs_count": 2}, ensure_ascii=True),
                ),
            )
            conn.execute(
                """
                INSERT INTO advisor_briefings(
                  created_at_utc,as_of,cadence,status,source_policy,title,summary_md,recommendations_json,watchlist_json,
                  quality_json,market_notes_json,links_json,opportunity_run_id,advisor_log_id
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    "2026-03-11T21:05:00Z",
                    "2026-03-11",
                    "daily",
                    "warn",
                    "strict_official_reuters",
                    "Asesor daily",
                    "# Briefing daily\n",
                    json.dumps([{"status": "conditional", "title": "SPY", "reason": "test", "quality_flags": []}], ensure_ascii=True),
                    json.dumps([{"status": "watchlist", "title": "ACWI", "reason": "watch", "quality_flags": []}], ensure_ascii=True),
                    json.dumps({"rows": [{"id": "quality_inference", "kind": "warn", "label": "Calidad"}]}, ensure_ascii=True),
                    json.dumps({"snapshot_date": "2026-03-11"}, ensure_ascii=True),
                    json.dumps({"latest_reports": {"analysis": "reports/latest/AnalisisPortafolio.md"}}, ensure_ascii=True),
                    run_id,
                    9,
                ),
            )
            conn.commit()
        finally:
            conn.close()

        latest = advisor_latest("daily")
        self.assertEqual(latest["cadence"], "daily")
        self.assertEqual((latest["briefing"] or {}).get("status"), "warn")

        history = advisor_history(limit=10)
        self.assertEqual(len(history["rows"]), 1)
        self.assertEqual(history["rows"][0]["cadence"], "daily")

        latest_run = advisor_opportunities_latest()
        self.assertIsNotNone(latest_run["run"])
        self.assertEqual(latest_run["run"]["top_operable"][0]["symbol"], "SPY")
