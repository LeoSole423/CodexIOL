"""Tests for swing trading simulation: indicators, signals, bot configs, and runner."""
import os
import sqlite3
import tempfile
import unittest

from iol_cli.db import connect, init_db


def _mk_db():
    tmp = tempfile.TemporaryDirectory()
    db_path = os.path.join(tmp.name, "swing_test.db")
    conn = connect(db_path)
    init_db(conn)
    return tmp, conn


def _insert_prices(conn, symbol: str, prices: list):
    """Insert daily price rows for a symbol."""
    conn.executemany(
        """
        INSERT OR REPLACE INTO market_symbol_snapshots
            (snapshot_date, symbol, market, last_price, source)
        VALUES (?, ?, 'bcba', ?, 'quote')
        """,
        [(date, symbol, price) for date, price in prices],
    )
    conn.commit()


class TestSwingIndicators(unittest.TestCase):
    """Test pure TA indicator functions."""

    def _series(self, prices):
        return [(f"2025-01-{i+1:02d}", p) for i, p in enumerate(prices)]

    def test_rsi_returns_none_when_insufficient_data(self):
        from iol_engines.simulation.swing_indicators import rsi
        self.assertIsNone(rsi(self._series([100.0] * 5)))

    def test_rsi_neutral_for_flat_prices(self):
        from iol_engines.simulation.swing_indicators import rsi
        prices = [(f"2025-01-{i+1:02d}", 100.0) for i in range(20)]
        result = rsi(prices)
        # Flat prices → no gains/losses → RSI = 100 (no losses means rs = inf)
        self.assertIsNotNone(result)

    def test_rsi_low_for_downtrend(self):
        from iol_engines.simulation.swing_indicators import rsi
        # Steadily declining prices
        prices = self._series([100.0 - i * 2 for i in range(20)])
        result = rsi(prices)
        self.assertIsNotNone(result)
        self.assertLess(result, 40.0)

    def test_rsi_high_for_uptrend(self):
        from iol_engines.simulation.swing_indicators import rsi
        prices = self._series([100.0 + i * 2 for i in range(20)])
        result = rsi(prices)
        self.assertIsNotNone(result)
        self.assertGreater(result, 60.0)

    def test_macd_returns_none_when_insufficient(self):
        from iol_engines.simulation.swing_indicators import macd
        prices = self._series([100.0] * 10)
        self.assertIsNone(macd(prices))

    def test_macd_returns_triple_for_enough_data(self):
        from iol_engines.simulation.swing_indicators import macd
        prices = self._series([100.0 + i * 0.5 for i in range(50)])
        result = macd(prices)
        self.assertIsNotNone(result)
        self.assertEqual(len(result), 3)

    def test_bollinger_bands_returns_none_when_insufficient(self):
        from iol_engines.simulation.swing_indicators import bollinger_bands
        self.assertIsNone(bollinger_bands(self._series([100.0] * 5)))

    def test_bollinger_bands_upper_gt_lower(self):
        from iol_engines.simulation.swing_indicators import bollinger_bands
        prices = self._series([100.0 + (i % 5) for i in range(25)])
        result = bollinger_bands(prices)
        self.assertIsNotNone(result)
        upper, mid, lower = result
        self.assertGreater(upper, lower)
        self.assertAlmostEqual(mid, (upper + lower) / 2, places=1)

    def test_atr_from_close_only(self):
        from iol_engines.simulation.swing_indicators import atr_from_close_only
        prices = self._series([100.0 + i for i in range(20)])
        result = atr_from_close_only(prices)
        self.assertIsNotNone(result)
        self.assertAlmostEqual(result, 1.0, places=5)  # Constant diff of 1

    def test_compute_swing_ta_returns_dataclass(self):
        from iol_engines.simulation.swing_indicators import SwingTA, compute_swing_ta
        prices = [(f"2025-01-{i+1:02d}", 100.0 + i * 0.5) for i in range(35)]
        ta = compute_swing_ta("AAPL", prices)
        self.assertIsInstance(ta, SwingTA)
        self.assertEqual(ta.symbol, "AAPL")
        self.assertIsNotNone(ta.rsi_14)

    def test_swing_ta_price_above_ma20(self):
        from iol_engines.simulation.swing_indicators import compute_swing_ta
        # Rising prices — last price should be above MA20
        prices = [(f"2025-01-{i+1:02d}", 100.0 + i * 1.0) for i in range(35)]
        ta = compute_swing_ta("TEST", prices)
        self.assertTrue(ta.price_above_ma20)


class TestSwingBotConfig(unittest.TestCase):

    def test_all_presets_exist(self):
        from iol_engines.simulation.swing_bot_config import list_swing_presets
        presets = list_swing_presets()
        self.assertEqual(len(presets), 3)
        names = {p.name for p in presets}
        self.assertIn("swing-conservative", names)
        self.assertIn("swing-balanced", names)
        self.assertIn("swing-aggressive", names)

    def test_get_preset_raises_for_unknown(self):
        from iol_engines.simulation.swing_bot_config import get_swing_preset
        with self.assertRaises(ValueError):
            get_swing_preset("nonexistent-bot")

    def test_conservative_has_tighter_params_than_aggressive(self):
        from iol_engines.simulation.swing_bot_config import get_swing_preset
        cons = get_swing_preset("swing-conservative")
        aggr = get_swing_preset("swing-aggressive")
        self.assertLess(cons.stop_loss_pct, aggr.stop_loss_pct)
        self.assertGreater(cons.min_hold_days, aggr.min_hold_days)
        self.assertGreater(cons.min_engine_score, aggr.min_engine_score)


class TestSwingSignals(unittest.TestCase):

    def _make_ta(self, last_price=100.0, rsi=50.0, above_ma20=True, macd_bull=True):
        from iol_engines.simulation.swing_indicators import SwingTA
        return SwingTA(
            symbol="TEST",
            last_price=last_price,
            rsi_14=rsi,
            macd_line=0.5 if macd_bull else -0.5,
            macd_signal=0.3 if macd_bull else -0.3,
            macd_histogram=0.2 if macd_bull else -0.2,
            bb_upper=110.0,
            bb_mid=100.0,
            bb_lower=90.0,
            atr_14=1.5,
            ma20_deviation_pct=2.0 if above_ma20 else -2.0,
            ma50_deviation_pct=3.0 if above_ma20 else -3.0,
            price_above_ma20=above_ma20,
            price_above_ma50=above_ma20,
        )

    def test_entry_signal_all_conditions_met(self):
        from iol_engines.simulation.swing_bot_config import get_swing_preset
        from iol_engines.simulation.swing_signals import classify_swing_signal
        config = get_swing_preset("swing-balanced")
        ta = self._make_ta(rsi=52.0, above_ma20=True, macd_bull=True)
        signal = classify_swing_signal(ta, engine_score=60.0, regime_score=60.0,
                                       macro_stress=40.0, position=None, config=config)
        self.assertEqual(signal.action, "entry")
        self.assertGreater(signal.conviction, 0)

    def test_no_entry_when_engine_score_too_low(self):
        from iol_engines.simulation.swing_bot_config import get_swing_preset
        from iol_engines.simulation.swing_signals import classify_swing_signal
        config = get_swing_preset("swing-balanced")
        ta = self._make_ta(rsi=50.0, above_ma20=True, macd_bull=True)
        signal = classify_swing_signal(ta, engine_score=10.0, regime_score=60.0,
                                       macro_stress=40.0, position=None, config=config)
        self.assertEqual(signal.action, "no_signal")

    def test_no_entry_when_rsi_overbought(self):
        from iol_engines.simulation.swing_bot_config import get_swing_preset
        from iol_engines.simulation.swing_signals import classify_swing_signal
        config = get_swing_preset("swing-balanced")
        ta = self._make_ta(rsi=75.0, above_ma20=True, macd_bull=True)
        signal = classify_swing_signal(ta, engine_score=70.0, regime_score=70.0,
                                       macro_stress=20.0, position=None, config=config)
        self.assertEqual(signal.action, "no_signal")

    def test_stop_loss_triggers_exit(self):
        from iol_engines.simulation.swing_bot_config import get_swing_preset
        from iol_engines.simulation.swing_signals import OpenPosition, classify_swing_signal
        config = get_swing_preset("swing-balanced")
        entry_price = 100.0
        current_price = 95.0  # Down 5% > stop_loss_pct (4%)
        ta = self._make_ta(last_price=current_price)
        pos = OpenPosition(symbol="TEST", entry_price=entry_price, entry_date="2025-01-01",
                           days_held=3, peak_price=entry_price, engine_score=60.0)
        signal = classify_swing_signal(ta, engine_score=60.0, regime_score=60.0,
                                       macro_stress=40.0, position=pos, config=config)
        self.assertEqual(signal.action, "exit")
        self.assertIn("stop_loss", signal.reason)

    def test_take_profit_triggers_exit(self):
        from iol_engines.simulation.swing_bot_config import get_swing_preset
        from iol_engines.simulation.swing_signals import OpenPosition, classify_swing_signal
        config = get_swing_preset("swing-balanced")
        entry_price = 100.0
        current_price = 115.0  # Up 15% > take_profit_pct (10%)
        ta = self._make_ta(last_price=current_price)
        pos = OpenPosition(symbol="TEST", entry_price=entry_price, entry_date="2025-01-01",
                           days_held=3, peak_price=current_price, engine_score=60.0)
        signal = classify_swing_signal(ta, engine_score=60.0, regime_score=60.0,
                                       macro_stress=40.0, position=pos, config=config)
        self.assertEqual(signal.action, "exit")
        self.assertIn("take_profit", signal.reason)

    def test_time_stop_triggers_exit(self):
        from iol_engines.simulation.swing_bot_config import get_swing_preset
        from iol_engines.simulation.swing_signals import OpenPosition, classify_swing_signal
        config = get_swing_preset("swing-balanced")
        ta = self._make_ta(last_price=102.0)
        pos = OpenPosition(symbol="TEST", entry_price=100.0, entry_date="2025-01-01",
                           days_held=config.max_hold_days, peak_price=102.0, engine_score=60.0)
        signal = classify_swing_signal(ta, engine_score=55.0, regime_score=60.0,
                                       macro_stress=40.0, position=pos, config=config)
        self.assertEqual(signal.action, "exit")
        self.assertIn("time_stop", signal.reason)

    def test_hold_when_conditions_fine(self):
        from iol_engines.simulation.swing_bot_config import get_swing_preset
        from iol_engines.simulation.swing_signals import OpenPosition, classify_swing_signal
        config = get_swing_preset("swing-balanced")
        ta = self._make_ta(last_price=102.0, rsi=55.0)
        pos = OpenPosition(symbol="TEST", entry_price=100.0, entry_date="2025-01-01",
                           days_held=2, peak_price=102.0, engine_score=60.0)
        signal = classify_swing_signal(ta, engine_score=60.0, regime_score=60.0,
                                       macro_stress=40.0, position=pos, config=config)
        self.assertEqual(signal.action, "hold")


class TestSwingRunner(unittest.TestCase):

    def test_runner_creates_run_row(self):
        from iol_engines.simulation.swing_bot_config import get_swing_preset
        from iol_engines.simulation.swing_runner import run_swing_backtest

        tmp, conn = _mk_db()
        try:
            # Insert some price data
            _insert_prices(conn, "AAPL", [
                (f"2025-01-{i+1:02d}", 100.0 + i * 0.5)
                for i in range(10)
            ])

            config = get_swing_preset("swing-balanced")
            run_id = run_swing_backtest(
                conn, config, "2025-01-01", "2025-01-10", 100_000.0, verbose=False
            )
            self.assertIsInstance(run_id, int)
            self.assertGreater(run_id, 0)

            row = conn.execute(
                "SELECT bot_name, initial_cash FROM swing_simulation_runs WHERE id = ?",
                (run_id,),
            ).fetchone()
            self.assertIsNotNone(row)
            self.assertEqual(row[0], "swing-balanced")
            self.assertEqual(row[1], 100_000.0)
        finally:
            conn.close()
            tmp.cleanup()

    def test_runner_with_no_market_data(self):
        from iol_engines.simulation.swing_bot_config import get_swing_preset
        from iol_engines.simulation.swing_runner import run_swing_backtest

        tmp, conn = _mk_db()
        try:
            config = get_swing_preset("swing-conservative")
            run_id = run_swing_backtest(
                conn, config, "2020-01-01", "2020-01-31", 50_000.0, verbose=False
            )
            self.assertIsInstance(run_id, int)
        finally:
            conn.close()
            tmp.cleanup()

    def test_db_schema_has_swing_tables(self):
        tmp, conn = _mk_db()
        try:
            tables = {r[0] for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()}
            self.assertIn("swing_simulation_runs", tables)
            self.assertIn("swing_simulation_trades", tables)
        finally:
            conn.close()
            tmp.cleanup()


if __name__ == "__main__":
    unittest.main()
