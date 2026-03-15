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


def apply_migrations(conn, ensure_columns) -> None:
    for table, columns in MIGRATION_COLUMNS.items():
        ensure_columns(conn, table, columns)
