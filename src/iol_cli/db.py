import os
import sqlite3
from typing import Optional


def resolve_db_path(db_path: str) -> str:
    if os.path.isabs(db_path):
        return db_path
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    return os.path.join(project_root, db_path)


def ensure_db_dir(db_path: str) -> None:
    dirname = os.path.dirname(db_path)
    if dirname and not os.path.exists(dirname):
        os.makedirs(dirname, exist_ok=True)


def connect(db_path: str) -> sqlite3.Connection:
    ensure_db_dir(db_path)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    cur.execute(
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
            cash_disponible_ars REAL,
            cash_disponible_usd REAL,
            raw_json TEXT
        )
        """
    )
    cur.execute(
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
        """
    )
    cur.execute(
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
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS orders (
            order_number INTEGER PRIMARY KEY,
            status TEXT,
            symbol TEXT,
            market TEXT,
            side TEXT,
            quantity REAL,
            price REAL,
            plazo TEXT,
            order_type TEXT,
            created_at TEXT,
            updated_at TEXT,
            raw_json TEXT
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS snapshot_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            snapshot_date TEXT,
            retrieved_at TEXT,
            source TEXT,
            status TEXT,
            error_message TEXT
        )
        """
    )
    cur.execute(
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
        """
    )
    cur.execute(
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
        """
    )
    cur.execute(
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
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_orders_created ON orders(created_at)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_advisor_logs_created ON advisor_logs(created_at)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_account_balances_date ON account_balances(snapshot_date)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_batch_ops_run ON batch_ops(run_id)")
    conn.commit()

    # Lightweight migrations for existing DBs created before these fields existed.
    ensure_columns(
        conn,
        "portfolio_snapshots",
        {
            "titles_value": "REAL",
            "cash_disponible_ars": "REAL",
            "cash_disponible_usd": "REAL",
        },
    )


def ensure_columns(conn: sqlite3.Connection, table: str, columns: dict) -> None:
    cur = conn.cursor()
    existing = {row[1] for row in cur.execute(f"PRAGMA table_info({table})").fetchall()}
    for name, ddl in columns.items():
        if name not in existing:
            cur.execute(f"ALTER TABLE {table} ADD COLUMN {name} {ddl}")
    conn.commit()
