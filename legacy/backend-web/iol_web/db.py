import os
import sqlite3

from iol_shared.portfolio_db import (
    Snapshot,
    add_manual_cashflow_adjustment,
    allocation,
    assets_for_snapshot,
    connect_ro,
    connect_rw,
    delete_manual_cashflow_adjustment,
    earliest_snapshot,
    first_snapshot_in_range,
    first_snapshot_of_year,
    last_snapshot_in_range,
    latest_snapshot,
    list_account_cash_movements,
    list_manual_cashflow_adjustments,
    manual_cashflow_sum,
    monthly_first_last_series,
    orders_cashflows_by_symbol,
    orders_flow_summary,
    resolve_db_path as shared_resolve_db_path,
    snapshot_before,
    snapshot_on_or_before,
    snapshots_series,
    table_columns as _table_columns,
)


def resolve_db_path() -> str:
    return shared_resolve_db_path(os.getenv("IOL_DB_PATH") or "data/iol_history.db")


def _connect_ro(db_path: str) -> sqlite3.Connection:
    return connect_ro(db_path)


def _connect_rw(db_path: str) -> sqlite3.Connection:
    return connect_rw(db_path)


def get_conn() -> sqlite3.Connection:
    return _connect_ro(resolve_db_path())


def get_conn_rw() -> sqlite3.Connection:
    return _connect_rw(resolve_db_path())
