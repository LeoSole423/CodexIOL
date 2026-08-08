import os
import tempfile

from iol_cli.db import connect, init_db
from iol_cli.snapshot import _collect_simulation_ohlcv_watchlist, _dedupe_watchlist


def _mk_db():
    tmp = tempfile.TemporaryDirectory()
    db_path = os.path.join(tmp.name, "snapshot_ohlcv_test.db")
    conn = connect(db_path)
    init_db(conn)
    return tmp, conn


def test_dedupe_watchlist_preserves_first_market():
    out = _dedupe_watchlist(
        [("ggal", "bcba"), ("AAPL", "bCBA")],
        [("GGAL", "other"), ("MSFT", "")],
    )

    assert out == [("GGAL", "bcba"), ("AAPL", "bCBA"), ("MSFT", "bCBA")]


def test_collect_simulation_ohlcv_watchlist_includes_pending_and_latest_candidates():
    tmp, conn = _mk_db()
    try:
        conn.execute(
            """
            INSERT OR REPLACE INTO market_symbol_snapshots
                (snapshot_date, symbol, market, last_price, source)
            VALUES ('2026-05-22', 'PEND', 'bcba', 100, 'test')
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
        run_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        conn.execute(
            """
            INSERT INTO simulation_pending_orders
                (run_id, signal_date, symbol, side, signal_price, status, created_at)
            VALUES (?, '2026-05-22', 'PEND', 'buy', 100, 'pending',
                    '2026-05-22T00:00:00Z')
            """,
            (run_id,),
        )
        conn.execute(
            """
            INSERT INTO advisor_opportunity_runs
                (created_at_utc, as_of, mode, universe, budget_ars, top_n, status, config_json)
            VALUES ('2026-05-22T00:00:00Z', '2026-05-22', 'both', 'test',
                    100000, 10, 'done', '{}')
            """
        )
        opp_run_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        conn.execute(
            """
            INSERT INTO advisor_opportunity_candidates
                (run_id, symbol, candidate_type, score_total, score_risk,
                 score_value, score_momentum, score_catalyst, reason_summary,
                 filters_passed, candidate_status)
            VALUES (?, 'CAND', 'test', 90, 90, 90, 90, 90, 'test', 1, 'operable')
            """,
            (opp_run_id,),
        )
        conn.commit()

        out = dict(_collect_simulation_ohlcv_watchlist(conn))

        assert out["PEND"] == "bcba"
        assert out["CAND"] == "bCBA"
    finally:
        conn.close()
        tmp.cleanup()
