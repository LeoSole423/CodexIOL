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
    def _insert_market_price(self, conn, day, symbol="AAPL", price=100.0):
        conn.execute(
            """
            INSERT OR REPLACE INTO market_symbol_snapshots
                (snapshot_date, symbol, market, last_price, volume_amount, source)
            VALUES (?, ?, 'bcba', ?, 1000000, 'test')
            """,
            (day, symbol, price),
        )

    def _insert_open_price(self, conn, day, symbol="AAPL", price=100.0):
        conn.execute(
            """
            INSERT OR REPLACE INTO symbol_daily_ohlcv
                (symbol, trade_date, open, high, low, close, source, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, 'test', ?)
            """,
            (symbol, day, price, price, price, price, f"{day}T00:00:00Z"),
        )

    def _insert_opportunity(
        self,
        conn,
        day,
        symbol="AAPL",
        side="buy",
        family="momentum",
        score=80.0,
        status="operable",
        amount=10000.0,
    ):
        conn.execute(
            """
            INSERT INTO advisor_opportunity_runs
                (created_at_utc, as_of, mode, universe, budget_ars, top_n, status, config_json)
            VALUES (?, ?, 'both', 'test', 100000, 10, 'done', '{}')
            """,
            (f"{day}T00:00:00Z", day),
        )
        opp_run_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        conn.execute(
            """
            INSERT INTO advisor_opportunity_candidates
                (run_id, symbol, candidate_type, signal_side, signal_family,
                 score_total, score_risk, score_value, score_momentum, score_catalyst,
                 suggested_amount_ars, reason_summary, filters_passed, candidate_status)
            VALUES (?, ?, 'test', ?, ?, ?, ?, ?, ?, ?, ?, 'test signal', 1, ?)
            """,
            (opp_run_id, symbol, side, family, score, score, score, score, score, amount, status),
        )
        return opp_run_id

    def _create_live_position(self, conn, entry_date="2026-05-01", symbol="AAPL", price=100.0, qty=100.0):
        from iol_engines.simulation.bot_config import get_preset
        from iol_engines.simulation.cost_model import ExecutionCostModel
        from iol_engines.simulation.runner import _create_run_row

        cost_model = ExecutionCostModel()
        run_id = _create_run_row(
            conn,
            get_preset("balanced"),
            entry_date,
            entry_date,
            100_000.0,
            mode="live",
            cost_model_version=cost_model.version,
        )
        amount = price * qty
        self._insert_market_price(conn, entry_date, symbol, price)
        conn.execute(
            """
            INSERT INTO simulation_trades
                (run_id, trade_date, symbol, action, quantity, price,
                 amount_ars, portfolio_value_after, cost_model_json)
            VALUES (?, ?, ?, 'buy', ?, ?, ?, 100000, ?)
            """,
            (
                run_id,
                entry_date,
                symbol,
                qty,
                price,
                amount,
                f'{{"net_cash_impact_ars": {amount}, "price_source": "symbol_daily_ohlcv.open"}}',
            ),
        )
        conn.commit()
        return run_id, cost_model

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

    def test_live_step_ignores_watchlist_candidates(self):
        from iol_engines.simulation.runner import run_live_step

        tmp, conn = _mk_db()
        try:
            conn.execute(
                """
                INSERT OR REPLACE INTO market_symbol_snapshots
                    (snapshot_date, symbol, market, last_price, volume_amount, source)
                VALUES ('2025-01-02', 'AAPL', 'bcba', 100, 1000000, 'test')
                """
            )
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
                VALUES (?, 'AAPL', 'test', 'buy', 'momentum', 95, 95, 95, 95, 95,
                        10000, 'watchlist buy', 1, 'watchlist')
                """,
                (opp_run_id,),
            )
            conn.commit()

            ids = run_live_step(conn, ["balanced"], "2025-01-02", 100_000.0, verbose=False)
            live_run_id = ids[0]

            self.assertEqual(
                conn.execute("SELECT COUNT(*) FROM simulation_trades WHERE run_id=?", (live_run_id,)).fetchone()[0],
                0,
            )
            self.assertEqual(
                conn.execute("SELECT COUNT(*) FROM simulation_pending_orders WHERE run_id=?", (live_run_id,)).fetchone()[0],
                0,
            )
        finally:
            conn.close()
            tmp.cleanup()

    def test_daily_live_metrics_uses_realized_exit_pnl_for_win_rate(self):
        from iol_engines.simulation.bot_config import get_preset
        from iol_engines.simulation.runner import _create_run_row, _daily_live_metrics

        tmp, conn = _mk_db()
        try:
            run_id = _create_run_row(
                conn,
                get_preset("balanced"),
                "2026-05-01",
                "2026-05-02",
                100_000.0,
                mode="live",
            )
            conn.executemany(
                """
                INSERT INTO simulation_trades
                    (run_id, trade_date, symbol, action, quantity, price,
                     amount_ars, portfolio_value_after, cost_model_json)
                VALUES (?, ?, ?, ?, 1, ?, ?, ?, ?)
                """,
                [
                    (run_id, "2026-05-01", "AAPL", "buy", 100, 100, 99_900, "{}"),
                    (
                        run_id,
                        "2026-05-02",
                        "AAPL",
                        "exit",
                        110,
                        110,
                        100_500,
                        '{"realized_pnl_ars": 500.0}',
                    ),
                ],
            )
            metrics = _daily_live_metrics(conn, run_id, "2026-05-01", "2026-05-02", 100_000.0, 100_500.0)

            self.assertEqual(metrics["closed_trades"], 1)
            self.assertEqual(metrics["win_rate_pct"], 100.0)
            self.assertEqual(metrics["total_trades"], 2)
        finally:
            conn.close()
            tmp.cleanup()

    def test_live_step_cancels_pending_buy_below_min_lot(self):
        from iol_engines.simulation.bot_config import get_preset
        from iol_engines.simulation.cost_model import ExecutionCostModel
        from iol_engines.simulation.runner import _create_run_row, run_live_step

        tmp, conn = _mk_db()
        try:
            cost_model = ExecutionCostModel()
            config = get_preset("balanced")
            run_id = _create_run_row(
                conn,
                config,
                "2026-05-01",
                "2026-05-27",
                100_000.0,
                mode="live",
                cost_model_version=cost_model.version,
            )
            conn.execute(
                """
                INSERT OR REPLACE INTO market_symbol_snapshots
                    (snapshot_date, symbol, market, last_price, volume_amount, source)
                VALUES ('2026-05-28', 'EXP', 'bcba', 1000, 1000000, 'test')
                """
            )
            conn.execute(
                """
                INSERT OR REPLACE INTO symbol_daily_ohlcv
                    (symbol, trade_date, open, high, low, close, source, updated_at)
                VALUES ('EXP', '2026-05-28', 1000, 1000, 1000, 1000, 'test',
                        '2026-05-28T00:00:00Z')
                """
            )
            conn.execute(
                """
                INSERT INTO simulation_pending_orders
                    (run_id, signal_date, symbol, side, action, amount_ars,
                     signal_price, status, created_at)
                VALUES (?, '2026-05-27', 'EXP', 'buy', 'buy', 500,
                        1000, 'pending', '2026-05-27T00:00:00Z')
                """,
                (run_id,),
            )
            conn.commit()

            run_live_step(
                conn,
                ["balanced"],
                "2026-05-28",
                100_000.0,
                verbose=False,
                cost_model=cost_model,
            )

            order = conn.execute(
                """
                SELECT status, execute_date, execute_price, cancel_reason
                FROM simulation_pending_orders
                WHERE run_id=?
                """,
                (run_id,),
            ).fetchone()
            self.assertEqual(order["status"], "cancelled")
            self.assertEqual(order["execute_date"], "2026-05-28")
            self.assertEqual(order["execute_price"], 1000)
            self.assertEqual(order["cancel_reason"], "unfillable_min_lot")
            trades = conn.execute(
                "SELECT COUNT(*) FROM simulation_trades WHERE run_id=?",
                (run_id,),
            ).fetchone()[0]
            self.assertEqual(trades, 0)
        finally:
            conn.close()
            tmp.cleanup()

    def test_daily_exit_policy_queues_stop_loss_and_executes_next_day(self):
        from iol_engines.simulation.runner import run_live_step

        tmp, conn = _mk_db()
        try:
            run_id, cost_model = self._create_live_position(conn)
            self._insert_market_price(conn, "2026-05-02", price=95.0)
            self._insert_market_price(conn, "2026-05-03", price=94.0)
            self._insert_open_price(conn, "2026-05-03", price=94.0)

            run_live_step(conn, ["balanced"], "2026-05-02", 100_000.0, verbose=False, cost_model=cost_model)

            order = conn.execute(
                """
                SELECT side, action, quantity, reason, engine_source
                FROM simulation_pending_orders
                WHERE run_id=? AND signal_date='2026-05-02'
                """,
                (run_id,),
            ).fetchone()
            self.assertEqual(order["side"], "sell")
            self.assertEqual(order["action"], "exit")
            self.assertEqual(order["quantity"], 100.0)
            self.assertEqual(order["reason"], "daily_exit:stop_loss")
            self.assertEqual(order["engine_source"], "daily_exit_policy")

            run_live_step(conn, ["balanced"], "2026-05-03", 100_000.0, verbose=False, cost_model=cost_model)
            trade = conn.execute(
                """
                SELECT action, price, reason, cost_model_json
                FROM simulation_trades
                WHERE run_id=? AND trade_date='2026-05-03'
                """,
                (run_id,),
            ).fetchone()
            self.assertEqual(trade["action"], "exit")
            self.assertEqual(trade["price"], 94.0)
            self.assertEqual(trade["reason"], "daily_exit:stop_loss")
            self.assertIn("realized_pnl_ars", trade["cost_model_json"])
        finally:
            conn.close()
            tmp.cleanup()

    def test_daily_exit_policy_take_profit_updates_closed_trade_metrics(self):
        from iol_engines.simulation.runner import run_live_step

        tmp, conn = _mk_db()
        try:
            run_id, cost_model = self._create_live_position(conn)
            self._insert_market_price(conn, "2026-05-02", price=109.0)
            self._insert_market_price(conn, "2026-05-03", price=110.0)
            self._insert_open_price(conn, "2026-05-03", price=110.0)

            run_live_step(conn, ["balanced"], "2026-05-02", 100_000.0, verbose=False, cost_model=cost_model)
            order = conn.execute(
                "SELECT action, reason FROM simulation_pending_orders WHERE run_id=? AND signal_date='2026-05-02'",
                (run_id,),
            ).fetchone()
            self.assertEqual(order["action"], "exit")
            self.assertEqual(order["reason"], "daily_exit:take_profit")

            run_live_step(conn, ["balanced"], "2026-05-03", 100_000.0, verbose=False, cost_model=cost_model)
            metrics = conn.execute("SELECT metrics_json FROM simulation_runs WHERE id=?", (run_id,)).fetchone()
            self.assertIn('"closed_trades": 1', metrics["metrics_json"])
            self.assertIn('"win_rate_pct": 100.0', metrics["metrics_json"])
        finally:
            conn.close()
            tmp.cleanup()

    def test_daily_exit_policy_time_stop_without_advisor_sell(self):
        from iol_engines.simulation.runner import run_live_step

        tmp, conn = _mk_db()
        try:
            run_id, cost_model = self._create_live_position(conn)
            for day in ["2026-05-02", "2026-05-03", "2026-05-04", "2026-05-05", "2026-05-06", "2026-05-07", "2026-05-08"]:
                self._insert_market_price(conn, day, price=101.0)

            run_live_step(conn, ["balanced"], "2026-05-08", 100_000.0, verbose=False, cost_model=cost_model)

            order = conn.execute(
                "SELECT action, reason FROM simulation_pending_orders WHERE run_id=? AND signal_date='2026-05-08'",
                (run_id,),
            ).fetchone()
            self.assertEqual(order["action"], "exit")
            self.assertEqual(order["reason"], "daily_exit:time_stop")
        finally:
            conn.close()
            tmp.cleanup()

    def test_daily_exit_policy_score_deterioration_on_suppressed_candidate(self):
        from iol_engines.simulation.runner import run_live_step

        tmp, conn = _mk_db()
        try:
            run_id, cost_model = self._create_live_position(conn)
            self._insert_market_price(conn, "2026-05-02", price=101.0)
            self._insert_opportunity(conn, "2026-05-02", side="buy", score=20.0, status="suppressed")

            run_live_step(conn, ["balanced"], "2026-05-02", 100_000.0, verbose=False, cost_model=cost_model)

            order = conn.execute(
                "SELECT action, reason FROM simulation_pending_orders WHERE run_id=? AND signal_date='2026-05-02'",
                (run_id,),
            ).fetchone()
            self.assertEqual(order["action"], "exit")
            self.assertEqual(order["reason"], "daily_exit:score_deterioration")
        finally:
            conn.close()
            tmp.cleanup()

    def test_daily_exit_policy_does_not_duplicate_pending_sell(self):
        from iol_engines.simulation.bot_config import get_preset
        from iol_engines.simulation.runner import _queue_daily_exit_orders, _reconstruct_portfolio

        tmp, conn = _mk_db()
        try:
            run_id, cost_model = self._create_live_position(conn)
            portfolio, _ = _reconstruct_portfolio(conn, run_id, 100_000.0, cost_model)
            pending_orders = [{
                "symbol": "AAPL",
                "side": "sell",
                "action": "exit",
                "quantity": 100.0,
                "signal_price": 95.0,
                "reason": "daily_exit:stop_loss",
                "engine_source": "daily_exit_policy",
            }]

            queued = _queue_daily_exit_orders(
                conn,
                run_id,
                "2026-05-02",
                get_preset("balanced"),
                portfolio,
                {"AAPL": 95.0},
                [],
                pending_orders,
            )

            self.assertEqual(queued, set())
            self.assertEqual(len(pending_orders), 1)
        finally:
            conn.close()
            tmp.cleanup()

    def test_daily_exit_policy_does_not_rebuy_symbol_marked_for_exit(self):
        from iol_engines.simulation.runner import run_live_step

        tmp, conn = _mk_db()
        try:
            run_id, cost_model = self._create_live_position(conn)
            self._insert_market_price(conn, "2026-05-02", price=95.0)
            self._insert_opportunity(conn, "2026-05-02", side="buy", score=90.0, status="operable")

            run_live_step(conn, ["balanced"], "2026-05-02", 100_000.0, verbose=False, cost_model=cost_model)

            orders = conn.execute(
                """
                SELECT side, action, reason
                FROM simulation_pending_orders
                WHERE run_id=? AND signal_date='2026-05-02'
                ORDER BY id
                """,
                (run_id,),
            ).fetchall()
            self.assertEqual(len(orders), 1)
            self.assertEqual(orders[0]["side"], "sell")
            self.assertEqual(orders[0]["action"], "exit")
        finally:
            conn.close()
            tmp.cleanup()


if __name__ == "__main__":
    unittest.main()
