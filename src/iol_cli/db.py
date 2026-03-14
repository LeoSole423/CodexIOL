import os
import sqlite3
from typing import Optional

from .db_migrations import apply_migrations
from .db_schema import INDEX_STATEMENTS, SCHEMA_STATEMENTS


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
    for statement in SCHEMA_STATEMENTS:
        cur.execute(statement)
    for statement in INDEX_STATEMENTS:
        cur.execute(statement)
    conn.commit()
    apply_migrations(conn, ensure_columns)


def ensure_columns(conn: sqlite3.Connection, table: str, columns: dict) -> None:
    cur = conn.cursor()
    existing = {row[1] for row in cur.execute(f"PRAGMA table_info({table})").fetchall()}
    for name, ddl in columns.items():
        if name not in existing:
            cur.execute(f"ALTER TABLE {table} ADD COLUMN {name} {ddl}")
    conn.commit()
