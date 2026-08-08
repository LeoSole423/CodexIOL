import json
import os
import tempfile

from typer.testing import CliRunner

from iol_cli.cli import app
from iol_cli.db import connect, init_db
from iol_cli.simulation_audit import build_simulation_audit
from tests_support import base_cli_env


def _mk_db():
    tmp = tempfile.TemporaryDirectory()
    db_path = os.path.join(tmp.name, "simulation_audit_test.db")
    conn = connect(db_path)
    init_db(conn)
    return tmp, conn, db_path


def test_build_simulation_audit_reports_core_sections_and_cost_flags():
    tmp, conn, _ = _mk_db()
    try:
        conn.executemany(
            """
            INSERT OR REPLACE INTO market_symbol_snapshots
                (snapshot_date, symbol, market, last_price, volume_amount, source)
            VALUES (?, ?, 'bcba', ?, 1000000, 'test')
            """,
            [
                ("2026-05-18", "AAPL", 100.0),
                ("2026-05-19", "AAPL", 101.0),
                ("2026-05-20", "AAPL", 102.0),
                ("2026-05-21", "FALL", 200.0),
                ("2026-05-22", "OPEN", 300.0),
            ],
        )
        conn.executemany(
            """
            INSERT OR REPLACE INTO portfolio_snapshots
                (snapshot_date, total_value, currency, source)
            VALUES (?, ?, 'ARS', 'test')
            """,
            [("2026-05-18", 100000.0), ("2026-05-22", 103000.0)],
        )
        conn.executemany(
            """
            INSERT INTO advisor_signal_outcomes
                (candidate_id, run_id, signal_side, signal_family, symbol, as_of,
                 horizon, eval_status, forward_return_pct, excess_return_pct, hit, notes_json)
            VALUES (?, 1, 'buy', ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (1, "momentum", "AAPL", "2026-05-20", 1, "ok", 1.2, 0.4, 1, "{}"),
                (2, "momentum", "AAPL", "2026-05-18", 5, "ok", 4.0, 2.5, 1, "{}"),
                (
                    4,
                    "momentum",
                    "ZERO",
                    "2026-05-18",
                    5,
                    "ok",
                    0.0,
                    0.0,
                    0,
                    '{"entry_price":100.0,"end_price":100.0}',
                ),
                (
                    3,
                    "value",
                    "MISSING",
                    "2026-05-19",
                    5,
                    "missing_prices",
                    None,
                    None,
                    None,
                    '{"missing_reason":"no_forward_price"}',
                ),
            ],
        )
        conn.execute(
            """
            INSERT INTO engine_regime_snapshots
                (as_of, created_at_utc, regime, confidence, regime_score,
                 favored_asset_classes_json, defensive_weight_adjustment)
            VALUES ('2026-05-22', '2026-05-22T00:00:00Z', 'risk_on', 0.7,
                    0.6, '[]', 0.0)
            """
        )
        conn.execute(
            """
            INSERT INTO simulation_bot_configs (name, created_at_utc, description, config_json)
            VALUES ('balanced', '2026-05-22T00:00:00Z', 'test', '{}')
            """
        )
        bot_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        conn.execute(
            """
            INSERT INTO simulation_runs
                (created_at_utc, bot_config_id, date_from, date_to, status,
                 initial_value_ars, final_value_ars, total_return_pct, max_drawdown_pct,
                 win_rate_pct, total_trades, mode, cost_model_version)
            VALUES ('2026-05-22T00:00:00Z', ?, '2026-05-01', '2026-05-22',
                    'running', 100000, 101000, 1.0, 2.0, 50.0, 2, 'live', 'costs-v1')
            """,
            (bot_id,),
        )
        run_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        conn.execute(
            """
            INSERT INTO simulation_runs
                (created_at_utc, bot_config_id, date_from, date_to, status,
                 initial_value_ars, final_value_ars, total_return_pct, max_drawdown_pct,
                 win_rate_pct, total_trades, mode)
            VALUES ('2026-05-01T00:00:00Z', ?, '2026-04-01', '2026-04-30',
                    'stale', 100000, 99000, -1.0, NULL, NULL, NULL, 'live')
            """,
            (bot_id,),
        )
        conn.executemany(
            """
            INSERT INTO simulation_trades
                (run_id, trade_date, symbol, action, quantity, price, amount_ars,
                 gross_amount_ars, net_amount_ars, total_cost_ars, cost_model_json)
            VALUES (?, ?, ?, 'buy', 1, 100, 100, ?, ?, ?, ?)
            """,
            [
                (run_id, "2026-05-22", "AAPL", 1000.0, 1010.0, 10.0, '{"price_source":"symbol_daily_ohlcv.open"}'),
                (run_id, "2026-05-22", "FALL", 1000.0, 1000.0, None, "{}"),
            ],
        )
        conn.execute(
            """
            INSERT INTO simulation_pending_orders
                (run_id, signal_date, symbol, side, signal_price, status, created_at)
            VALUES (?, '2026-05-22', 'OPEN', 'buy', 300, 'pending',
                    '2026-05-22T00:00:00Z')
            """,
            (run_id,),
        )
        conn.execute(
            """
            INSERT OR REPLACE INTO symbol_daily_ohlcv
                (symbol, trade_date, open, high, low, close, source, updated_at)
            VALUES ('OPEN', '2026-05-24', 301, 302, 300, 301, 'test',
                    '2026-05-24T00:00:00Z')
            """
        )
        conn.commit()

        audit = build_simulation_audit(conn, as_of="2026-05-24", include_symbols=True)

        assert set(
            [
                "snapshot_coverage",
                "price_warmup",
                "signal_outcomes",
                "h5_segments",
                "h5_segments_dedup",
                "weekly_regime",
                "real_portfolio",
                "simulations",
                "net_cost_coverage",
                "t1_health",
                "event_detection_today",
                "data_quality_flags",
            ]
        ).issubset(audit.keys())
        assert audit["signal_outcomes"]["horizons"]["1"]["hit_rate_pct"] == 100.0
        assert audit["signal_outcomes"]["missing_by_reason"] == {"no_forward_price": 1}
        assert audit["signal_outcomes"]["ok_zero_suspect"] == 1
        assert audit["signal_outcomes"]["ok_zero_suspect_total_historical"] == 1
        assert audit["h5_segments_dedup"]
        assert audit["snapshot_coverage"]["market_snapshot_count"] == 5
        assert audit["snapshot_coverage"]["ohlcv_snapshot_count"] == 1
        assert audit["price_warmup"]["ohlcv"]["distinct_dates"] == 1
        assert len(audit["simulations"]["daily"]) == 1
        assert audit["simulations"]["daily"][0]["run_id"] == run_id
        assert audit["historical_stale"]["total"] == 1
        assert audit["historical_stale"]["by_family"]["daily"]["null_drawdown"] == 1
        daily = audit["simulations"]["daily"][0]
        assert daily["cost_model_version"] == "costs-v1"
        assert daily["closed_trades"] == 0
        assert daily["daily_exit_reasons"] == {}
        assert daily["total_costs_ars"] == 10.0
        assert "trade_costs_null" in daily["anomalies"]
        assert "cost_coverage_low" in daily["anomalies"]
        assert audit["t1_health"]["active_pending_total"] == 1
        assert audit["t1_health"]["open_ready"] >= 1
    finally:
        conn.close()
        tmp.cleanup()


def test_simulate_audit_cli_json_contains_stable_sections():
    tmp, conn, db_path = _mk_db()
    conn.close()
    try:
        result = CliRunner().invoke(
            app,
            ["simulate", "audit", "--as-of", "2026-05-24", "--json"],
            env=base_cli_env(db_path),
        )
        assert result.exit_code == 0, result.output
        payload = json.loads(result.output)
        assert payload["as_of"] == "2026-05-24"
        assert "snapshot_coverage" in payload
        assert "ohlcv_snapshot_count" in payload["snapshot_coverage"]
        assert "simulations" in payload
        assert "historical_stale" in payload
        assert "net_cost_coverage" in payload
        assert "t1_health" in payload
    finally:
        tmp.cleanup()
