import os
import tempfile

from iol_cli.commands_simulate import _t1_execution_health
from iol_cli.db import connect, init_db


def _mk_db():
    tmp = tempfile.TemporaryDirectory()
    db_path = os.path.join(tmp.name, "t1_health_test.db")
    conn = connect(db_path)
    init_db(conn)
    return tmp, conn


def test_t1_execution_health_reports_open_fallback_and_pending_counts():
    tmp, conn = _mk_db()
    try:
        conn.executemany(
            """
            INSERT OR REPLACE INTO market_symbol_snapshots
                (snapshot_date, symbol, market, last_price, source)
            VALUES (?, ?, 'bcba', ?, 'test')
            """,
            [
                ("2026-05-22", "OPEN", 100.0),
                ("2026-05-22", "FALL", 200.0),
            ],
        )
        conn.execute(
            """
            INSERT OR REPLACE INTO symbol_daily_ohlcv
                (symbol, trade_date, open, high, low, close, source, updated_at)
            VALUES ('OPEN', '2026-05-26', 101, 102, 100, 101, 'test',
                    '2026-05-26T00:00:00Z')
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
                 initial_value_ars, mode)
            VALUES ('2026-05-22T00:00:00Z', ?, '2026-05-01', '2026-05-22',
                    'running', 100000, 'live')
            """,
            (bot_id,),
        )
        running_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        conn.execute(
            """
            INSERT INTO simulation_runs
                (created_at_utc, bot_config_id, date_from, date_to, status,
                 initial_value_ars, mode)
            VALUES ('2026-05-01T00:00:00Z', ?, '2026-05-01', '2026-05-01',
                    'stale', 100000, 'live')
            """,
            (bot_id,),
        )
        stale_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        conn.executemany(
            """
            INSERT INTO simulation_pending_orders
                (run_id, signal_date, symbol, side, signal_price, status, created_at)
            VALUES (?, ?, ?, 'buy', 100, 'pending', '2026-05-22T00:00:00Z')
            """,
            [
                (running_id, "2026-05-22", "OPEN"),
                (running_id, "2026-05-22", "FALL"),
                (stale_id, "2026-05-01", "OLD"),
            ],
        )
        conn.commit()

        out = _t1_execution_health(conn, "2026-05-26")
        symbols = {row["symbol"]: row for row in out["symbols"]}

        assert out["active_pending_total"] == 2
        assert out["stale_pending_total"] == 1
        assert out["open_ready"] == 1
        assert out["fallback_needed"] >= 1
        assert "pending_t1_orders" in out["flags"]
        assert "stale_pending_orders" in out["flags"]
        assert symbols["OPEN"]["has_open_as_of"] is True
        assert symbols["FALL"]["would_fallback_on_as_of"] is True
    finally:
        conn.close()
        tmp.cleanup()
