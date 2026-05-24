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


if __name__ == "__main__":
    unittest.main()
