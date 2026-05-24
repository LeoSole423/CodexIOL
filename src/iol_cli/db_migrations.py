MIGRATION_COLUMNS = {
    "portfolio_snapshots": {
        "titles_value": "REAL",
        "cash_total_ars": "REAL",
        "cash_disponible_ars": "REAL",
        "cash_disponible_usd": "REAL",
    },
    "orders": {
        "side_norm": "TEXT",
        "operated_at": "TEXT",
        "ordered_qty": "REAL",
        "executed_qty": "REAL",
        "limit_price": "REAL",
        "avg_price": "REAL",
        "operated_amount": "REAL",
        "currency": "TEXT",
    },
    "advisor_opportunity_runs": {
        "pipeline_warnings_json": "TEXT",
        "variant_id": "INTEGER",
        "score_version": "TEXT",
        "run_metrics_json": "TEXT",
    },
    "simulation_runs": {
        "mode": "TEXT",
        "engine_driven": "INTEGER",
        "avg_regime_score": "REAL",
        "regime_context_json": "TEXT",
        "win_rate_pct": "REAL",
        "total_trades": "INTEGER",
        "cost_model_version": "TEXT",
    },
    "swing_simulation_runs": {
        "status": "TEXT",
        "plan_json": "TEXT",
        "benchmark_symbol": "TEXT",
        "benchmark_return_pct": "REAL",
        "cost_model_version": "TEXT",
    },
    "event_simulation_runs": {
        "status": "TEXT",
        "plan_json": "TEXT",
        "benchmark_symbol": "TEXT",
        "benchmark_return_pct": "REAL",
        "cost_model_version": "TEXT",
    },
    "simulation_trades": {
        "gross_amount_ars": "REAL",
        "net_amount_ars": "REAL",
        "commission_ars": "REAL",
        "market_fee_ars": "REAL",
        "iva_ars": "REAL",
        "slippage_ars": "REAL",
        "total_cost_ars": "REAL",
        "execution_price": "REAL",
        "instrument_type": "TEXT",
        "cost_model_json": "TEXT",
    },
    "swing_simulation_trades": {
        "gross_amount_ars": "REAL",
        "net_amount_ars": "REAL",
        "commission_ars": "REAL",
        "market_fee_ars": "REAL",
        "iva_ars": "REAL",
        "slippage_ars": "REAL",
        "total_cost_ars": "REAL",
        "execution_price": "REAL",
        "instrument_type": "TEXT",
        "cost_model_json": "TEXT",
    },
    "event_simulation_trades": {
        "gross_amount_ars": "REAL",
        "net_amount_ars": "REAL",
        "commission_ars": "REAL",
        "market_fee_ars": "REAL",
        "iva_ars": "REAL",
        "slippage_ars": "REAL",
        "total_cost_ars": "REAL",
        "execution_price": "REAL",
        "instrument_type": "TEXT",
        "cost_model_json": "TEXT",
    },
    "account_cash_movements": {
        "symbol": "TEXT",
    },
    "advisor_opportunity_candidates": {
        "expert_signal_score": "REAL",
        "trusted_refs_count": "INTEGER",
        "consensus_state": "TEXT",
        "decision_gate": "TEXT",
        "candidate_status": "TEXT",
        "evidence_summary_json": "TEXT",
        "liquidity_score": "REAL",
        "sector_bucket": "TEXT",
        "is_crypto_proxy": "INTEGER",
        "signal_side": "TEXT",
        "signal_family": "TEXT",
        "score_version": "TEXT",
        "holding_context_json": "TEXT",
        "score_features_json": "TEXT",
    },
}


_CREATE_IF_MISSING = [
    """
    CREATE TABLE IF NOT EXISTS simulation_pending_orders (
        id           INTEGER PRIMARY KEY AUTOINCREMENT,
        run_id       INTEGER NOT NULL,
        signal_date  TEXT NOT NULL,
        symbol       TEXT NOT NULL,
        side         TEXT NOT NULL CHECK(side IN ('buy','sell')),
        action       TEXT,
        amount_ars   REAL,
        quantity     REAL,
        signal_price REAL NOT NULL,
        reason       TEXT,
        engine_source TEXT,
        status       TEXT NOT NULL DEFAULT 'pending'
                          CHECK(status IN ('pending','executed','cancelled')),
        execute_date  TEXT,
        execute_price REAL,
        created_at   TEXT NOT NULL,
        FOREIGN KEY(run_id) REFERENCES simulation_runs(id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS swing_pending_orders (
        id           INTEGER PRIMARY KEY AUTOINCREMENT,
        run_id       INTEGER NOT NULL,
        signal_date  TEXT NOT NULL,
        symbol       TEXT NOT NULL,
        side         TEXT NOT NULL CHECK(side IN ('buy','sell')),
        amount_ars   REAL,
        quantity     REAL,
        signal_price REAL NOT NULL,
        status       TEXT NOT NULL DEFAULT 'pending'
                          CHECK(status IN ('pending','executed','cancelled')),
        execute_date  TEXT,
        execute_price REAL,
        created_at   TEXT NOT NULL,
        FOREIGN KEY(run_id) REFERENCES swing_simulation_runs(id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS event_pending_orders (
        id           INTEGER PRIMARY KEY AUTOINCREMENT,
        run_id       INTEGER NOT NULL,
        signal_date  TEXT NOT NULL,
        symbol       TEXT NOT NULL,
        side         TEXT NOT NULL CHECK(side IN ('buy','sell')),
        action       TEXT NOT NULL,
        amount_ars   REAL,
        quantity     REAL,
        signal_price REAL NOT NULL,
        trigger_event_type TEXT NOT NULL,
        trigger_event_description TEXT,
        status       TEXT NOT NULL DEFAULT 'pending'
                          CHECK(status IN ('pending','executed','cancelled')),
        execute_date  TEXT,
        execute_price REAL,
        created_at   TEXT NOT NULL,
        FOREIGN KEY(run_id) REFERENCES event_simulation_runs(id)
    )
    """,
]


def apply_migrations(conn, ensure_columns) -> None:
    for table, columns in MIGRATION_COLUMNS.items():
        ensure_columns(conn, table, columns)
    for ddl in _CREATE_IF_MISSING:
        conn.execute(ddl)
    conn.commit()
