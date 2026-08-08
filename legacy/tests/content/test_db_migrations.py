import os
import tempfile

from iol_cli.db import connect, init_db


def _mk_db():
    tmp = tempfile.TemporaryDirectory()
    db_path = os.path.join(tmp.name, "migration_test.db")
    conn = connect(db_path)
    init_db(conn)
    return tmp, conn


def test_init_db_cancels_pending_orders_for_stale_simulation_runs():
    tmp, conn = _mk_db()
    try:
        conn.execute(
            """
            INSERT INTO swing_simulation_runs
                (bot_name, date_from, date_to, initial_cash, mode, status, created_at)
            VALUES ('swing-balanced', '2026-05-01', '2026-05-01', 100000,
                    'live', 'stale', '2026-05-01T00:00:00Z')
            """
        )
        stale_run_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        conn.execute(
            """
            INSERT INTO swing_pending_orders
                (run_id, signal_date, symbol, side, signal_price, status, created_at)
            VALUES (?, '2026-05-01', 'GGAL', 'buy', 100, 'pending',
                    '2026-05-01T00:00:00Z')
            """,
            (stale_run_id,),
        )
        conn.commit()

        init_db(conn)

        status = conn.execute(
            "SELECT status, cancel_reason FROM swing_pending_orders WHERE run_id=?",
            (stale_run_id,),
        ).fetchone()
        assert status["status"] == "cancelled"
        assert status["cancel_reason"] == "stale_run"
    finally:
        conn.close()
        tmp.cleanup()
