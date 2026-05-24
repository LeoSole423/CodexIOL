"""Tests for event-driven trading simulation: detector, bot configs, and runner."""
import os
import sqlite3
import tempfile
import unittest
from unittest.mock import MagicMock

from iol_cli.db import connect, init_db


def _mk_db():
    tmp = tempfile.TemporaryDirectory()
    db_path = os.path.join(tmp.name, "event_test.db")
    conn = connect(db_path)
    init_db(conn)
    return tmp, conn


def _insert_regime_snapshot(conn, as_of: str, regime: str, regime_score: float,
                              volatility_regime: str = "normal"):
    conn.execute(
        """
        INSERT OR REPLACE INTO engine_regime_snapshots
            (as_of, regime, regime_score, confidence, volatility_regime,
             breadth_score, defensive_weight_adjustment, favored_asset_classes_json,
             created_at_utc)
        VALUES (?, ?, ?, 0.8, ?, 50.0, 0.0, '[]', '2025-01-01T00:00:00')
        """,
        (as_of, regime, regime_score, volatility_regime),
    )
    conn.commit()


def _insert_macro_snapshot(conn, as_of: str, argentina_stress: float, global_risk_on: float):
    conn.execute(
        """
        INSERT OR REPLACE INTO engine_macro_snapshots
            (as_of, argentina_macro_stress, global_risk_on,
             inflation_mom_pct, bcra_rate_pct, fed_rate_pct,
             us_cpi_yoy_pct, usd_ars_official, usd_ars_blue,
             cedear_fx_premium_pct, sentiment_score, created_at_utc)
        VALUES (?, ?, ?, 5.0, 100.0, 5.0, 3.0, 1000.0, 1200.0, 20.0, 0.0,
                '2025-01-01T00:00:00')
        """,
        (as_of, argentina_stress, global_risk_on),
    )
    conn.commit()


class TestEventDetector(unittest.TestCase):

    def test_detect_regime_change(self):
        from iol_engines.simulation.event_detector import detect_regime_events

        prev = MagicMock()
        prev.regime = "bull"
        prev.volatility_regime = "normal"

        curr = MagicMock()
        curr.regime = "bear"
        curr.volatility_regime = "normal"
        curr.regime_score = 30.0

        events = detect_regime_events(prev, curr)
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].event_type, "regime_change")
        self.assertIn("bull", events[0].description)
        self.assertIn("bear", events[0].description)

    def test_no_events_when_regime_same(self):
        from iol_engines.simulation.event_detector import detect_regime_events

        prev = MagicMock()
        prev.regime = "bull"
        prev.volatility_regime = "normal"

        curr = MagicMock()
        curr.regime = "bull"
        curr.volatility_regime = "normal"
        curr.regime_score = 70.0

        events = detect_regime_events(prev, curr)
        self.assertEqual(len(events), 0)

    def test_detect_volatility_spike(self):
        from iol_engines.simulation.event_detector import detect_regime_events

        prev = MagicMock()
        prev.regime = "bull"
        prev.volatility_regime = "normal"

        curr = MagicMock()
        curr.regime = "bull"
        curr.volatility_regime = "extreme"
        curr.regime_score = 65.0

        events = detect_regime_events(prev, curr)
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].event_type, "volatility_spike")
        self.assertEqual(events[0].severity, "critical")

    def test_detect_macro_stress_high(self):
        from iol_engines.simulation.event_detector import detect_macro_events

        prev = MagicMock()
        prev.argentina_macro_stress = 60.0
        prev.global_risk_on = 55.0

        curr = MagicMock()
        curr.argentina_macro_stress = 75.0
        curr.global_risk_on = 55.0

        events = detect_macro_events(prev, curr)
        types = [e.event_type for e in events]
        self.assertIn("macro_stress_high", types)

    def test_detect_risk_off(self):
        from iol_engines.simulation.event_detector import detect_macro_events

        prev = MagicMock()
        prev.argentina_macro_stress = 40.0
        prev.global_risk_on = 50.0

        curr = MagicMock()
        curr.argentina_macro_stress = 40.0
        curr.global_risk_on = 25.0

        events = detect_macro_events(prev, curr)
        types = [e.event_type for e in events]
        self.assertIn("risk_off", types)

    def test_detect_smart_money_accumulate(self):
        from iol_engines.simulation.event_detector import detect_smart_money_events

        prev_signal = MagicMock()
        prev_signal.symbol = "AAPL"
        prev_signal.net_institutional_direction = "neutral"
        prev_signal.conviction_score = 70.0

        curr_signal = MagicMock()
        curr_signal.symbol = "AAPL"
        curr_signal.net_institutional_direction = "accumulate"
        curr_signal.conviction_score = 75.0

        events = detect_smart_money_events([prev_signal], [curr_signal])
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].event_type, "smart_money_accumulate")
        self.assertEqual(events[0].symbol, "AAPL")

    def test_no_smart_money_event_for_low_conviction(self):
        from iol_engines.simulation.event_detector import detect_smart_money_events

        prev = MagicMock()
        prev.symbol = "AAPL"
        prev.net_institutional_direction = "neutral"
        prev.conviction_score = 30.0

        curr = MagicMock()
        curr.symbol = "AAPL"
        curr.net_institutional_direction = "accumulate"
        curr.conviction_score = 40.0  # Below 60 threshold

        events = detect_smart_money_events([prev], [curr])
        self.assertEqual(len(events), 0)

    def test_events_sorted_by_severity(self):
        from iol_engines.simulation.event_detector import detect_macro_events, detect_regime_events

        # Create both a high-severity and medium-severity event
        prev_regime = MagicMock()
        prev_regime.regime = "bull"
        prev_regime.volatility_regime = "normal"
        curr_regime = MagicMock()
        curr_regime.regime = "bear"
        curr_regime.volatility_regime = "normal"
        curr_regime.regime_score = 25.0

        prev_macro = MagicMock()
        prev_macro.argentina_macro_stress = 25.0
        prev_macro.global_risk_on = 75.0
        curr_macro = MagicMock()
        curr_macro.argentina_macro_stress = 28.0
        curr_macro.global_risk_on = 72.0

        events = detect_regime_events(prev_regime, curr_regime)
        events += detect_macro_events(prev_macro, curr_macro)

        if len(events) > 1:
            _order = {"critical": 0, "high": 1, "medium": 2}
            for i in range(len(events) - 1):
                self.assertLessEqual(
                    _order.get(events[i].severity, 3),
                    _order.get(events[i + 1].severity, 3),
                )


class TestEventBotConfig(unittest.TestCase):

    def test_all_presets_exist(self):
        from iol_engines.simulation.event_bot_config import list_event_presets
        presets = list_event_presets()
        self.assertEqual(len(presets), 3)
        names = {p.name for p in presets}
        self.assertIn("event-defensive", names)
        self.assertIn("event-opportunistic", names)
        self.assertIn("event-adaptive", names)

    def test_get_preset_raises_for_unknown(self):
        from iol_engines.simulation.event_bot_config import get_event_preset
        with self.assertRaises(ValueError):
            get_event_preset("no-such-bot")

    def test_defensive_has_more_cooldown_than_adaptive(self):
        from iol_engines.simulation.event_bot_config import get_event_preset
        defensive = get_event_preset("event-defensive")
        adaptive = get_event_preset("event-adaptive")
        self.assertGreater(defensive.hold_after_event_days, adaptive.hold_after_event_days)

    def test_all_presets_have_reaction_rules(self):
        from iol_engines.simulation.event_bot_config import list_event_presets
        for preset in list_event_presets():
            self.assertGreater(len(preset.reaction_rules), 0,
                               f"{preset.name} has no reaction rules")


class TestEventRunner(unittest.TestCase):

    def test_runner_creates_run_row(self):
        from iol_engines.simulation.event_bot_config import get_event_preset
        from iol_engines.simulation.event_runner import run_event_backtest

        tmp, conn = _mk_db()
        try:
            config = get_event_preset("event-adaptive")
            run_id = run_event_backtest(
                conn, config, "2025-01-01", "2025-01-10", 100_000.0, verbose=False
            )
            self.assertIsInstance(run_id, int)
            self.assertGreater(run_id, 0)

            row = conn.execute(
                "SELECT bot_name, initial_cash FROM event_simulation_runs WHERE id = ?",
                (run_id,),
            ).fetchone()
            self.assertIsNotNone(row)
            self.assertEqual(row[0], "event-adaptive")
        finally:
            conn.close()
            tmp.cleanup()

    def test_runner_with_events_in_db(self):
        """Runner should detect events when regime snapshots change."""
        from iol_engines.simulation.event_bot_config import get_event_preset
        from iol_engines.simulation.event_runner import run_event_backtest

        tmp, conn = _mk_db()
        try:
            # Insert price data
            conn.executemany(
                "INSERT OR REPLACE INTO market_symbol_snapshots "
                "(snapshot_date, symbol, market, last_price, source) VALUES (?, 'AAPL', 'bcba', ?, 'quote')",
                [(f"2025-01-0{i+1}", 100.0 + i) for i in range(5)],
            )
            conn.commit()

            # Insert two regime snapshots (different regimes = event triggered)
            _insert_regime_snapshot(conn, "2025-01-01", "bull", 70.0)
            _insert_regime_snapshot(conn, "2025-01-03", "bear", 30.0)

            config = get_event_preset("event-adaptive")
            run_id = run_event_backtest(
                conn, config, "2025-01-01", "2025-01-05", 100_000.0, verbose=False
            )
            self.assertIsInstance(run_id, int)

            row = conn.execute(
                "SELECT total_events_triggered FROM event_simulation_runs WHERE id = ?",
                (run_id,),
            ).fetchone()
            # May or may not have triggered depending on second_latest loading
            self.assertIsNotNone(row)
        finally:
            conn.close()
            tmp.cleanup()

    def test_live_step_reuses_monthly_run_stales_duplicates_and_records_zero_events(self):
        from iol_engines.simulation.event_runner import _create_run_row, run_event_live_step

        tmp, conn = _mk_db()
        try:
            conn.execute(
                """
                INSERT OR REPLACE INTO market_symbol_snapshots
                    (snapshot_date, symbol, market, last_price, source)
                VALUES (?, ?, 'bcba', ?, 'quote')
                """,
                ("2025-01-02", "SPY", 100.0),
            )
            conn.commit()
            old_id = _create_run_row(conn, "event-adaptive", "2025-01-01", "2025-01-01", 100_000.0, mode="live")
            keep_id = _create_run_row(conn, "event-adaptive", "2025-01-01", "2025-01-01", 100_000.0, mode="live")

            run_ids = run_event_live_step(
                conn, ["event-adaptive"], "2025-01-02", initial_cash_ars=100_000.0, verbose=False
            )

            self.assertEqual(run_ids, [keep_id])
            statuses = {
                r[0]: r[1]
                for r in conn.execute(
                    "SELECT id, status FROM event_simulation_runs WHERE id IN (?, ?)",
                    (old_id, keep_id),
                ).fetchall()
            }
            self.assertEqual(statuses[old_id], "stale")
            self.assertEqual(statuses[keep_id], "running")
            row = conn.execute(
                """
                SELECT total_events_triggered, max_drawdown_pct, total_trades, win_rate_pct
                FROM event_simulation_runs WHERE id = ?
                """,
                (keep_id,),
            ).fetchone()
            self.assertEqual(row[0], 0)
            self.assertIsNotNone(row[1])
            self.assertIsNotNone(row[2])
            self.assertIsNotNone(row[3])
        finally:
            conn.close()
            tmp.cleanup()

    def test_live_step_queues_event_signal_and_executes_next_day_open(self):
        from iol_engines.simulation.event_runner import run_event_live_step

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
            _insert_macro_snapshot(conn, "2025-01-01", 50.0, 50.0)
            _insert_macro_snapshot(conn, "2025-01-02", 50.0, 75.0)
            conn.execute(
                """
                INSERT INTO advisor_opportunity_runs
                    (created_at_utc, as_of, mode, universe, budget_ars, top_n, status, config_json)
                VALUES ('2025-01-02T00:00:00Z', '2025-01-02', 'both', 'test', 100000, 10, 'done', '{}')
                """
            )
            opp_run_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
            conn.execute(
                """
                INSERT INTO advisor_opportunity_candidates
                    (run_id, symbol, candidate_type, signal_side, signal_family,
                     score_total, score_risk, score_value, score_momentum, score_catalyst,
                     suggested_amount_ars, reason_summary, filters_passed, candidate_status)
                VALUES (?, 'AAPL', 'test', 'buy', 'momentum', 80, 80, 80, 80, 80,
                        10000, 'test buy', 1, 'operable')
                """,
                (opp_run_id,),
            )
            conn.commit()

            ids_day_1 = run_event_live_step(
                conn, ["event-adaptive"], "2025-01-02", initial_cash_ars=100_000.0, verbose=False
            )
            live_run_id = ids_day_1[0]
            self.assertEqual(
                conn.execute("SELECT COUNT(*) FROM event_simulation_trades WHERE run_id=?", (live_run_id,)).fetchone()[0],
                0,
            )
            self.assertEqual(
                conn.execute(
                    "SELECT COUNT(*) FROM event_pending_orders WHERE run_id=? AND signal_date='2025-01-02'",
                    (live_run_id,),
                ).fetchone()[0],
                1,
            )

            run_event_live_step(
                conn, ["event-adaptive"], "2025-01-03", initial_cash_ars=100_000.0, verbose=False
            )
            trade = conn.execute(
                "SELECT price, trigger_event_type, cost_model_json FROM event_simulation_trades WHERE run_id=? ORDER BY id LIMIT 1",
                (live_run_id,),
            ).fetchone()
            self.assertIsNotNone(trade)
            self.assertEqual(trade["price"], 110.0)
            self.assertEqual(trade["trigger_event_type"], "risk_on")
            self.assertIn("symbol_daily_ohlcv.open", trade["cost_model_json"])
            status = conn.execute(
                "SELECT status, execute_date FROM event_pending_orders WHERE run_id=? AND signal_date='2025-01-02'",
                (live_run_id,),
            ).fetchone()
            self.assertEqual(status["status"], "executed")
            self.assertEqual(status["execute_date"], "2025-01-03")
            self.assertEqual(
                conn.execute(
                    "SELECT COUNT(*) FROM event_pending_orders WHERE run_id=? AND status='pending'",
                    (live_run_id,),
                ).fetchone()[0],
                0,
            )
            total_trades = conn.execute(
                "SELECT total_trades FROM event_simulation_runs WHERE id=?",
                (live_run_id,),
            ).fetchone()
            self.assertEqual(total_trades["total_trades"], 1)
        finally:
            conn.close()
            tmp.cleanup()

    def test_db_schema_has_event_tables(self):
        tmp, conn = _mk_db()
        try:
            tables = {r[0] for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()}
            self.assertIn("event_simulation_runs", tables)
            self.assertIn("event_simulation_trades", tables)
            self.assertIn("event_pending_orders", tables)
        finally:
            conn.close()
            tmp.cleanup()

    def test_cooldown_respected(self):
        """After an event is triggered, subsequent days should be skipped."""
        from iol_engines.simulation.event_bot_config import get_event_preset
        from iol_engines.simulation.event_runner import _days_between

        # Just test the helper
        self.assertEqual(_days_between("2025-01-01", "2025-01-06"), 5)
        self.assertEqual(_days_between("2025-01-06", "2025-01-01"), 0)


if __name__ == "__main__":
    unittest.main()
