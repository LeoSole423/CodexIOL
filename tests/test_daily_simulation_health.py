"""Tests for live daily simulation run hygiene."""
import os
import tempfile
import unittest

from iol_cli.db import connect, init_db


def _mk_db():
    tmp = tempfile.TemporaryDirectory()
    db_path = os.path.join(tmp.name, "daily_sim_test.db")
    conn = connect(db_path)
    init_db(conn)
    return tmp, conn


class TestDailySimulationHealth(unittest.TestCase):
    def test_find_or_create_live_run_stales_duplicate_running_runs(self):
        from iol_engines.simulation.bot_config import get_preset
        from iol_engines.simulation.runner import _create_run_row, _find_or_create_live_run

        tmp, conn = _mk_db()
        try:
            config = get_preset("balanced")
            old_id = _create_run_row(conn, config, "2025-01-01", "2025-01-01", 100_000.0, mode="live")
            keep_id = _create_run_row(conn, config, "2025-01-01", "2025-01-01", 100_000.0, mode="live")

            run_id = _find_or_create_live_run(conn, config, "2025-01-15", 100_000.0)

            self.assertEqual(run_id, keep_id)
            rows = conn.execute(
                "SELECT id, status FROM simulation_runs WHERE id IN (?, ?) ORDER BY id",
                (old_id, keep_id),
            ).fetchall()
            self.assertEqual(rows[0]["status"], "stale")
            self.assertEqual(rows[1]["status"], "running")
        finally:
            conn.close()
            tmp.cleanup()

    def test_cost_model_version_creates_clean_live_run(self):
        from iol_engines.simulation.bot_config import get_preset
        from iol_engines.simulation.runner import _create_run_row, _find_or_create_live_run

        tmp, conn = _mk_db()
        try:
            config = get_preset("balanced")
            old_id = _create_run_row(conn, config, "2025-01-01", "2025-01-01", 100_000.0, mode="live")

            new_id = _find_or_create_live_run(
                conn, config, "2025-01-15", 100_000.0, cost_model_version="costs-v1"
            )

            self.assertNotEqual(new_id, old_id)
            rows = {
                r["id"]: r
                for r in conn.execute(
                    "SELECT id, status, cost_model_version FROM simulation_runs WHERE id IN (?, ?)",
                    (old_id, new_id),
                ).fetchall()
            }
            self.assertEqual(rows[old_id]["status"], "stale")
            self.assertEqual(rows[new_id]["status"], "running")
            self.assertEqual(rows[new_id]["cost_model_version"], "costs-v1")
        finally:
            conn.close()
            tmp.cleanup()

    def test_live_step_queues_signal_and_executes_next_day_open(self):
        from iol_engines.simulation.runner import run_live_step

        tmp, conn = _mk_db()
        try:
            conn.executemany(
                """
                INSERT OR REPLACE INTO market_symbol_snapshots
                    (snapshot_date, symbol, market, last_price, volume_amount, source)
                VALUES (?, 'AAPL', 'bcba', ?, 1000000, 'test')
                """,
                [("2025-01-02", 100.0), ("2025-01-03", 112.0)],
            )
            conn.execute(
                """
                INSERT OR REPLACE INTO symbol_daily_ohlcv
                    (symbol, trade_date, open, high, low, close, source, updated_at)
                VALUES ('AAPL', '2025-01-03', 110.0, 113.0, 109.0, 112.0, 'test', '2025-01-03T00:00:00Z')
                """
            )
            conn.execute(
                """
                INSERT INTO advisor_opportunity_runs
                    (created_at_utc, as_of, mode, universe, budget_ars, top_n, status, config_json)
                VALUES ('2025-01-02T00:00:00Z', '2025-01-02', 'both', 'test', 100000, 10, 'done', '{}')
                """
            )
            run_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
            conn.execute(
                """
                INSERT INTO advisor_opportunity_candidates
                    (run_id, symbol, candidate_type, signal_side, signal_family,
                     score_total, score_risk, score_value, score_momentum, score_catalyst,
                     suggested_amount_ars, reason_summary, filters_passed, candidate_status)
                VALUES (?, 'AAPL', 'test', 'buy', 'momentum', 80, 80, 80, 80, 80,
                        10000, 'test buy', 1, 'operable')
                """,
                (run_id,),
            )
            conn.commit()

            ids_day_1 = run_live_step(conn, ["balanced"], "2025-01-02", 100_000.0, verbose=False)
            live_run_id = ids_day_1[0]
            self.assertEqual(
                conn.execute("SELECT COUNT(*) FROM simulation_trades WHERE run_id=?", (live_run_id,)).fetchone()[0],
                0,
            )
            self.assertEqual(
                conn.execute(
                    "SELECT COUNT(*) FROM simulation_pending_orders WHERE run_id=? AND signal_date='2025-01-02'",
                    (live_run_id,),
                ).fetchone()[0],
                1,
            )

            run_live_step(conn, ["balanced"], "2025-01-03", 100_000.0, verbose=False)
            trade = conn.execute(
                "SELECT price, cost_model_json FROM simulation_trades WHERE run_id=? ORDER BY id LIMIT 1",
                (live_run_id,),
            ).fetchone()
            self.assertIsNotNone(trade)
            self.assertEqual(trade["price"], 110.0)
            self.assertIn("symbol_daily_ohlcv.open", trade["cost_model_json"])
            status = conn.execute(
                "SELECT status, execute_date FROM simulation_pending_orders WHERE run_id=? AND signal_date='2025-01-02'",
                (live_run_id,),
            ).fetchone()
            self.assertEqual(status["status"], "executed")
            self.assertEqual(status["execute_date"], "2025-01-03")
            total_trades = conn.execute(
                "SELECT total_trades FROM simulation_runs WHERE id=?",
                (live_run_id,),
            ).fetchone()
            self.assertEqual(total_trades["total_trades"], 1)
        finally:
            conn.close()
            tmp.cleanup()


if __name__ == "__main__":
    unittest.main()
