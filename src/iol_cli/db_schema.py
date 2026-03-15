SCHEMA_STATEMENTS = [
    """
    CREATE TABLE IF NOT EXISTS portfolio_snapshots (
        snapshot_date TEXT PRIMARY KEY,
        total_value REAL,
        currency TEXT,
        retrieved_at TEXT,
        close_time TEXT,
        minutes_from_close INTEGER,
        source TEXT,
        titles_value REAL,
        cash_total_ars REAL,
        cash_disponible_ars REAL,
        cash_disponible_usd REAL,
        raw_json TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS portfolio_assets (
        snapshot_date TEXT,
        symbol TEXT,
        description TEXT,
        market TEXT,
        type TEXT,
        currency TEXT,
        plazo TEXT,
        quantity REAL,
        last_price REAL,
        ppc REAL,
        total_value REAL,
        daily_var_pct REAL,
        daily_var_points REAL,
        gain_pct REAL,
        gain_amount REAL,
        committed REAL,
        raw_json TEXT,
        PRIMARY KEY (snapshot_date, symbol)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS account_balances (
        snapshot_date TEXT,
        account_number TEXT,
        account_type TEXT,
        currency TEXT,
        disponible REAL,
        comprometido REAL,
        saldo REAL,
        titulos_valorizados REAL,
        total REAL,
        margen_descubierto REAL,
        status TEXT,
        raw_json TEXT,
        PRIMARY KEY (snapshot_date, account_type, currency)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS manual_cashflow_adjustments (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        flow_date TEXT NOT NULL,
        kind TEXT NOT NULL,
        amount_ars REAL NOT NULL,
        note TEXT,
        created_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS account_cash_movements (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        movement_id TEXT,
        occurred_at TEXT,
        movement_date TEXT NOT NULL,
        currency TEXT NOT NULL,
        amount REAL NOT NULL,
        kind TEXT NOT NULL,
        description TEXT,
        source TEXT,
        raw_json TEXT,
        created_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS orders (
        order_number INTEGER PRIMARY KEY,
        status TEXT,
        symbol TEXT,
        market TEXT,
        side TEXT,
        side_norm TEXT,
        quantity REAL,
        price REAL,
        plazo TEXT,
        order_type TEXT,
        created_at TEXT,
        updated_at TEXT,
        operated_at TEXT,
        ordered_qty REAL,
        executed_qty REAL,
        limit_price REAL,
        avg_price REAL,
        operated_amount REAL,
        currency TEXT,
        raw_json TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS sync_state (
        key TEXT PRIMARY KEY,
        value TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS snapshot_runs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        snapshot_date TEXT,
        retrieved_at TEXT,
        source TEXT,
        status TEXT,
        error_message TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS advisor_logs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        created_at TEXT NOT NULL,
        snapshot_date TEXT,
        prompt TEXT NOT NULL,
        response TEXT NOT NULL,
        env TEXT,
        base_url TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS advisor_alerts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        status TEXT NOT NULL,
        severity TEXT NOT NULL,
        alert_type TEXT NOT NULL,
        title TEXT NOT NULL,
        description TEXT NOT NULL,
        symbol TEXT,
        snapshot_date TEXT,
        due_date TEXT,
        closed_at TEXT,
        closed_reason TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS advisor_events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        created_at TEXT NOT NULL,
        event_type TEXT NOT NULL,
        title TEXT NOT NULL,
        description TEXT,
        symbol TEXT,
        snapshot_date TEXT,
        alert_id INTEGER,
        payload_json TEXT,
        FOREIGN KEY(alert_id) REFERENCES advisor_alerts(id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS advisor_evidence (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        created_at TEXT NOT NULL,
        symbol TEXT NOT NULL,
        query TEXT NOT NULL,
        source_name TEXT NOT NULL,
        source_url TEXT NOT NULL,
        published_date TEXT,
        retrieved_at_utc TEXT NOT NULL,
        claim TEXT NOT NULL,
        confidence TEXT NOT NULL,
        date_confidence TEXT NOT NULL,
        notes TEXT,
        conflict_key TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS market_symbol_snapshots (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        snapshot_date TEXT NOT NULL,
        symbol TEXT NOT NULL,
        market TEXT NOT NULL DEFAULT 'bcba',
        last_price REAL,
        bid REAL,
        ask REAL,
        spread_pct REAL,
        daily_var_pct REAL,
        operations_count REAL,
        volume_amount REAL,
        source TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS advisor_opportunity_runs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        created_at_utc TEXT NOT NULL,
        as_of TEXT NOT NULL,
        mode TEXT NOT NULL,
        universe TEXT NOT NULL,
        budget_ars REAL NOT NULL,
        top_n INTEGER NOT NULL,
        variant_id INTEGER,
        score_version TEXT,
        status TEXT NOT NULL,
        error_message TEXT,
        config_json TEXT NOT NULL,
        pipeline_warnings_json TEXT,
        run_metrics_json TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS advisor_opportunity_candidates (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        run_id INTEGER NOT NULL,
        symbol TEXT NOT NULL,
        candidate_type TEXT NOT NULL,
        signal_side TEXT,
        signal_family TEXT,
        score_version TEXT,
        score_total REAL NOT NULL,
        score_risk REAL NOT NULL,
        score_value REAL NOT NULL,
        score_momentum REAL NOT NULL,
        score_catalyst REAL NOT NULL,
        entry_low REAL,
        entry_high REAL,
        suggested_weight_pct REAL,
        suggested_amount_ars REAL,
        reason_summary TEXT NOT NULL,
        risk_flags_json TEXT,
        filters_passed INTEGER NOT NULL,
        expert_signal_score REAL,
        trusted_refs_count INTEGER,
        consensus_state TEXT,
        decision_gate TEXT,
        candidate_status TEXT,
        evidence_summary_json TEXT,
        liquidity_score REAL,
        sector_bucket TEXT,
        is_crypto_proxy INTEGER,
        holding_context_json TEXT,
        score_features_json TEXT,
        FOREIGN KEY(run_id) REFERENCES advisor_opportunity_runs(id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS advisor_briefings (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        created_at_utc TEXT NOT NULL,
        as_of TEXT NOT NULL,
        cadence TEXT NOT NULL,
        status TEXT NOT NULL,
        source_policy TEXT NOT NULL,
        title TEXT,
        summary_md TEXT NOT NULL,
        recommendations_json TEXT NOT NULL,
        watchlist_json TEXT NOT NULL,
        quality_json TEXT NOT NULL,
        market_notes_json TEXT NOT NULL,
        links_json TEXT,
        opportunity_run_id INTEGER,
        advisor_log_id INTEGER,
        FOREIGN KEY(opportunity_run_id) REFERENCES advisor_opportunity_runs(id),
        FOREIGN KEY(advisor_log_id) REFERENCES advisor_logs(id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS advisor_model_variants (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL UNIQUE,
        status TEXT NOT NULL,
        created_at_utc TEXT NOT NULL,
        config_json TEXT NOT NULL,
        objective TEXT NOT NULL,
        promoted_from_variant_id INTEGER,
        promoted_at_utc TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS advisor_signal_outcomes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        candidate_id INTEGER NOT NULL,
        run_id INTEGER NOT NULL,
        variant_id INTEGER,
        signal_side TEXT NOT NULL,
        signal_family TEXT NOT NULL,
        symbol TEXT NOT NULL,
        as_of TEXT NOT NULL,
        horizon INTEGER NOT NULL,
        eval_status TEXT NOT NULL,
        forward_return_pct REAL,
        excess_return_pct REAL,
        max_adverse_excursion_pct REAL,
        max_favorable_excursion_pct REAL,
        liquidity_penalty REAL,
        hit INTEGER,
        notes_json TEXT,
        FOREIGN KEY(candidate_id) REFERENCES advisor_opportunity_candidates(id),
        FOREIGN KEY(run_id) REFERENCES advisor_opportunity_runs(id),
        FOREIGN KEY(variant_id) REFERENCES advisor_model_variants(id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS advisor_run_regressions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        run_id INTEGER NOT NULL,
        cadence TEXT NOT NULL,
        variant_id INTEGER NOT NULL,
        baseline_variant_id INTEGER NOT NULL,
        window_days INTEGER NOT NULL,
        scorecard_json TEXT NOT NULL,
        gate_status TEXT NOT NULL,
        regression_flags_json TEXT,
        FOREIGN KEY(run_id) REFERENCES advisor_opportunity_runs(id),
        FOREIGN KEY(variant_id) REFERENCES advisor_model_variants(id),
        FOREIGN KEY(baseline_variant_id) REFERENCES advisor_model_variants(id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS reconciliation_runs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        created_at_utc TEXT NOT NULL,
        as_of TEXT NOT NULL,
        date_from TEXT NOT NULL,
        date_to TEXT NOT NULL,
        days INTEGER NOT NULL,
        status TEXT NOT NULL,
        summary_json TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS reconciliation_intervals (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        run_id INTEGER NOT NULL,
        interval_key TEXT NOT NULL,
        base_snapshot_date TEXT NOT NULL,
        end_snapshot_date TEXT NOT NULL,
        state TEXT NOT NULL,
        issue_code TEXT,
        confidence TEXT,
        impact_on_inference TEXT NOT NULL,
        analysis_json TEXT NOT NULL,
        FOREIGN KEY(run_id) REFERENCES reconciliation_runs(id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS reconciliation_proposals (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        run_id INTEGER NOT NULL,
        interval_id INTEGER NOT NULL,
        interval_key TEXT NOT NULL,
        issue_code TEXT NOT NULL,
        resolution_type TEXT NOT NULL,
        suggested_kind TEXT,
        suggested_amount_ars REAL,
        confidence TEXT NOT NULL,
        confidence_score REAL NOT NULL,
        reason TEXT NOT NULL,
        source_basis TEXT NOT NULL,
        impact_on_inference TEXT NOT NULL,
        status TEXT NOT NULL,
        created_at_utc TEXT NOT NULL,
        applied_at_utc TEXT,
        FOREIGN KEY(run_id) REFERENCES reconciliation_runs(id),
        FOREIGN KEY(interval_id) REFERENCES reconciliation_intervals(id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS reconciliation_resolutions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        proposal_id INTEGER NOT NULL,
        interval_id INTEGER NOT NULL,
        interval_key TEXT NOT NULL,
        issue_code TEXT NOT NULL,
        resolution_type TEXT NOT NULL,
        status TEXT NOT NULL,
        manual_cashflow_id INTEGER,
        note TEXT,
        created_at_utc TEXT NOT NULL,
        FOREIGN KEY(proposal_id) REFERENCES reconciliation_proposals(id),
        FOREIGN KEY(interval_id) REFERENCES reconciliation_intervals(id),
        FOREIGN KEY(manual_cashflow_id) REFERENCES manual_cashflow_adjustments(id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS batch_runs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        created_at_utc TEXT NOT NULL,
        plan_path TEXT NOT NULL,
        plan_hash TEXT NOT NULL,
        snapshot_date TEXT,
        status TEXT NOT NULL,
        error_message TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS batch_ops (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        run_id INTEGER NOT NULL,
        idx INTEGER NOT NULL,
        kind TEXT NOT NULL,
        action TEXT NOT NULL,
        symbol TEXT,
        payload_json TEXT,
        quote_json TEXT,
        result_json TEXT,
        status TEXT NOT NULL,
        iol_order_number INTEGER,
        error_message TEXT,
        created_at_utc TEXT NOT NULL,
        FOREIGN KEY(run_id) REFERENCES batch_runs(id)
    )
    """,
    # ── Multi-Engine Financial Advisor ──────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS engine_regime_snapshots (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        as_of TEXT NOT NULL,
        created_at_utc TEXT NOT NULL,
        regime TEXT NOT NULL,
        confidence REAL NOT NULL,
        regime_score REAL NOT NULL,
        favored_asset_classes_json TEXT NOT NULL,
        defensive_weight_adjustment REAL NOT NULL,
        breadth_score REAL,
        volatility_regime TEXT,
        notes TEXT,
        raw_inputs_json TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS engine_macro_snapshots (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        as_of TEXT NOT NULL,
        created_at_utc TEXT NOT NULL,
        inflation_mom_pct REAL,
        bcra_rate_pct REAL,
        usd_ars_official REAL,
        usd_ars_blue REAL,
        cedear_fx_premium_pct REAL,
        fed_rate_pct REAL,
        us_cpi_yoy_pct REAL,
        argentina_macro_stress REAL NOT NULL,
        global_risk_on REAL NOT NULL,
        sentiment_score REAL,
        upcoming_events_json TEXT,
        raw_sources_json TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS engine_smart_money_snapshots (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        as_of TEXT NOT NULL,
        created_at_utc TEXT NOT NULL,
        symbol TEXT NOT NULL,
        net_institutional_direction TEXT NOT NULL,
        conviction_score REAL NOT NULL,
        top_holders_added_json TEXT,
        top_holders_trimmed_json TEXT,
        latest_13f_date TEXT,
        notes TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS engine_strategy_runs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        created_at_utc TEXT NOT NULL,
        as_of TEXT NOT NULL,
        opportunity_run_id INTEGER,
        bot_config_id TEXT,
        regime_snapshot_id INTEGER,
        macro_snapshot_id INTEGER,
        portfolio_cash_ars REAL,
        portfolio_cash_usd REAL,
        defensive_overlay_applied INTEGER NOT NULL DEFAULT 0,
        actions_json TEXT NOT NULL,
        notes TEXT,
        FOREIGN KEY(opportunity_run_id) REFERENCES advisor_opportunity_runs(id),
        FOREIGN KEY(regime_snapshot_id) REFERENCES engine_regime_snapshots(id),
        FOREIGN KEY(macro_snapshot_id) REFERENCES engine_macro_snapshots(id)
    )
    """,
    # ── Engine Signal Outcomes (accuracy tracking) ───────────────────────────
    """
    CREATE TABLE IF NOT EXISTS engine_signal_outcomes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        engine_name TEXT NOT NULL,
        as_of TEXT NOT NULL,
        signal_summary TEXT NOT NULL,
        lookahead_days INTEGER NOT NULL,
        outcome_date TEXT,
        outcome_return_pct REAL,
        signal_correct INTEGER,
        notes TEXT
    )
    """,
    # ── Swing Trading Simulation ─────────────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS swing_simulation_runs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        bot_name TEXT NOT NULL,
        date_from TEXT NOT NULL,
        date_to TEXT NOT NULL,
        initial_cash REAL NOT NULL,
        final_value REAL,
        total_return_pct REAL,
        sharpe_ratio REAL,
        max_drawdown_pct REAL,
        win_rate_pct REAL,
        avg_hold_days REAL,
        total_trades INTEGER,
        mode TEXT NOT NULL DEFAULT 'backtest',
        created_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS swing_simulation_trades (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        run_id INTEGER NOT NULL,
        symbol TEXT NOT NULL,
        entry_date TEXT NOT NULL,
        exit_date TEXT,
        entry_price REAL NOT NULL,
        exit_price REAL,
        quantity REAL NOT NULL,
        amount_ars REAL NOT NULL,
        pnl_ars REAL,
        return_pct REAL,
        hold_days INTEGER,
        exit_reason TEXT,
        entry_signals_json TEXT,
        exit_signals_json TEXT,
        FOREIGN KEY(run_id) REFERENCES swing_simulation_runs(id)
    )
    """,
    # ── Event-Driven Simulation ───────────────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS event_simulation_runs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        bot_name TEXT NOT NULL,
        date_from TEXT NOT NULL,
        date_to TEXT NOT NULL,
        initial_cash REAL NOT NULL,
        final_value REAL,
        total_return_pct REAL,
        sharpe_ratio REAL,
        max_drawdown_pct REAL,
        win_rate_pct REAL,
        total_events_triggered INTEGER,
        total_trades INTEGER,
        mode TEXT NOT NULL DEFAULT 'backtest',
        created_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS event_simulation_trades (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        run_id INTEGER NOT NULL,
        symbol TEXT NOT NULL,
        trade_date TEXT NOT NULL,
        action TEXT NOT NULL,
        quantity REAL NOT NULL,
        price REAL NOT NULL,
        amount_ars REAL NOT NULL,
        pnl_ars REAL,
        trigger_event_type TEXT NOT NULL,
        trigger_event_description TEXT,
        portfolio_value_after REAL,
        FOREIGN KEY(run_id) REFERENCES event_simulation_runs(id)
    )
    """,
    # ── Simulation Framework ────────────────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS simulation_bot_configs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL UNIQUE,
        created_at_utc TEXT NOT NULL,
        description TEXT,
        config_json TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS simulation_runs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        created_at_utc TEXT NOT NULL,
        bot_config_id INTEGER NOT NULL,
        date_from TEXT NOT NULL,
        date_to TEXT NOT NULL,
        status TEXT NOT NULL,
        final_value_ars REAL,
        initial_value_ars REAL,
        total_return_pct REAL,
        sharpe_ratio REAL,
        max_drawdown_pct REAL,
        metrics_json TEXT,
        error_message TEXT,
        FOREIGN KEY(bot_config_id) REFERENCES simulation_bot_configs(id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS simulation_trades (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        run_id INTEGER NOT NULL,
        trade_date TEXT NOT NULL,
        symbol TEXT NOT NULL,
        action TEXT NOT NULL,
        quantity REAL,
        price REAL,
        amount_ars REAL,
        portfolio_value_after REAL,
        reason TEXT,
        engine_source TEXT,
        FOREIGN KEY(run_id) REFERENCES simulation_runs(id)
    )
    """,
]


INDEX_STATEMENTS = [
    "CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status)",
    "CREATE INDEX IF NOT EXISTS idx_orders_created ON orders(created_at)",
    "CREATE INDEX IF NOT EXISTS idx_orders_symbol ON orders(symbol)",
    "CREATE INDEX IF NOT EXISTS idx_advisor_logs_created ON advisor_logs(created_at)",
    "CREATE INDEX IF NOT EXISTS idx_advisor_alerts_status_due ON advisor_alerts(status, due_date)",
    "CREATE INDEX IF NOT EXISTS idx_advisor_alerts_symbol ON advisor_alerts(symbol)",
    "CREATE INDEX IF NOT EXISTS idx_advisor_events_created ON advisor_events(created_at)",
    "CREATE INDEX IF NOT EXISTS idx_advisor_events_alert ON advisor_events(alert_id)",
    "CREATE INDEX IF NOT EXISTS idx_evidence_symbol_date ON advisor_evidence(symbol, retrieved_at_utc)",
    "CREATE INDEX IF NOT EXISTS idx_market_snapshots_symbol_date ON market_symbol_snapshots(symbol, snapshot_date)",
    "CREATE UNIQUE INDEX IF NOT EXISTS uq_market_snapshots_day_symbol_source ON market_symbol_snapshots(snapshot_date, symbol, source)",
    "CREATE INDEX IF NOT EXISTS idx_opp_runs_asof ON advisor_opportunity_runs(as_of)",
    "CREATE INDEX IF NOT EXISTS idx_opp_runs_variant_asof ON advisor_opportunity_runs(variant_id, as_of)",
    "CREATE INDEX IF NOT EXISTS idx_opp_candidates_run_score ON advisor_opportunity_candidates(run_id, score_total DESC)",
    "CREATE INDEX IF NOT EXISTS idx_opp_candidates_signal ON advisor_opportunity_candidates(run_id, signal_side, signal_family, score_total DESC)",
    "CREATE INDEX IF NOT EXISTS idx_advisor_briefings_asof ON advisor_briefings(as_of, cadence, created_at_utc DESC)",
    "CREATE INDEX IF NOT EXISTS idx_advisor_briefings_status ON advisor_briefings(status, cadence)",
    "CREATE INDEX IF NOT EXISTS idx_model_variants_status ON advisor_model_variants(status)",
    "CREATE INDEX IF NOT EXISTS idx_signal_outcomes_variant_date ON advisor_signal_outcomes(variant_id, as_of, horizon)",
    "CREATE UNIQUE INDEX IF NOT EXISTS uq_signal_outcomes_candidate_horizon ON advisor_signal_outcomes(candidate_id, horizon)",
    "CREATE INDEX IF NOT EXISTS idx_run_regressions_variant_cadence ON advisor_run_regressions(variant_id, cadence, id DESC)",
    "CREATE INDEX IF NOT EXISTS idx_reconciliation_runs_asof ON reconciliation_runs(as_of, id DESC)",
    "CREATE INDEX IF NOT EXISTS idx_reconciliation_intervals_run_end ON reconciliation_intervals(run_id, end_snapshot_date DESC)",
    "CREATE INDEX IF NOT EXISTS idx_reconciliation_intervals_key ON reconciliation_intervals(interval_key)",
    "CREATE INDEX IF NOT EXISTS idx_reconciliation_proposals_open ON reconciliation_proposals(status, confidence_score DESC, id DESC)",
    "CREATE INDEX IF NOT EXISTS idx_reconciliation_proposals_interval ON reconciliation_proposals(interval_id, id DESC)",
    "CREATE INDEX IF NOT EXISTS idx_reconciliation_resolutions_interval ON reconciliation_resolutions(interval_key, issue_code, id DESC)",
    "CREATE INDEX IF NOT EXISTS idx_account_balances_date ON account_balances(snapshot_date)",
    "CREATE INDEX IF NOT EXISTS idx_manual_cashflow_flow_date ON manual_cashflow_adjustments(flow_date)",
    "CREATE INDEX IF NOT EXISTS idx_cash_movements_date ON account_cash_movements(movement_date)",
    "CREATE INDEX IF NOT EXISTS idx_cash_movements_kind ON account_cash_movements(kind)",
    "CREATE INDEX IF NOT EXISTS idx_cash_movements_currency ON account_cash_movements(currency)",
    "CREATE UNIQUE INDEX IF NOT EXISTS uq_cash_movements_movement_id ON account_cash_movements(movement_id) WHERE movement_id IS NOT NULL",
    "CREATE INDEX IF NOT EXISTS idx_batch_ops_run ON batch_ops(run_id)",
    # engine indexes
    "CREATE UNIQUE INDEX IF NOT EXISTS uq_regime_asof ON engine_regime_snapshots(as_of)",
    "CREATE UNIQUE INDEX IF NOT EXISTS uq_macro_asof ON engine_macro_snapshots(as_of)",
    "CREATE UNIQUE INDEX IF NOT EXISTS uq_smart_money_symbol_asof ON engine_smart_money_snapshots(symbol, as_of)",
    "CREATE INDEX IF NOT EXISTS idx_engine_strategy_asof ON engine_strategy_runs(as_of)",
    # simulation indexes
    "CREATE INDEX IF NOT EXISTS idx_sim_runs_bot ON simulation_runs(bot_config_id)",
    "CREATE INDEX IF NOT EXISTS idx_sim_trades_run ON simulation_trades(run_id, trade_date)",
    # swing simulation indexes
    "CREATE INDEX IF NOT EXISTS idx_swing_runs_bot ON swing_simulation_runs(bot_name)",
    "CREATE INDEX IF NOT EXISTS idx_swing_trades_run ON swing_simulation_trades(run_id, entry_date)",
    "CREATE INDEX IF NOT EXISTS idx_swing_trades_symbol ON swing_simulation_trades(symbol)",
    # event simulation indexes
    "CREATE INDEX IF NOT EXISTS idx_event_runs_bot ON event_simulation_runs(bot_name)",
    "CREATE INDEX IF NOT EXISTS idx_event_trades_run ON event_simulation_trades(run_id, trade_date)",
    "CREATE INDEX IF NOT EXISTS idx_event_trades_event_type ON event_simulation_trades(trigger_event_type)",
    # engine signal outcomes indexes
    "CREATE INDEX IF NOT EXISTS idx_engine_outcomes_engine_asof ON engine_signal_outcomes(engine_name, as_of)",
    "CREATE UNIQUE INDEX IF NOT EXISTS uq_engine_outcomes_engine_asof ON engine_signal_outcomes(engine_name, as_of)",
]
