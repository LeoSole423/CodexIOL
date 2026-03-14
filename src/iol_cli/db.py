"""CLI database module.

Connection primitives live in iol_shared.db so the domain layer (iol_advisor)
can use them without depending on the CLI package.  This module re-exports
them for backward compatibility and adds init_db, which is CLI-specific
(it owns the full schema + migration lifecycle).
"""
import sqlite3

from iol_shared.db import connect, ensure_columns, ensure_db_dir, resolve_db_path  # noqa: F401

from .db_migrations import apply_migrations
from .db_schema import INDEX_STATEMENTS, SCHEMA_STATEMENTS


def init_db(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    for statement in SCHEMA_STATEMENTS:
        cur.execute(statement)
    for statement in INDEX_STATEMENTS:
        cur.execute(statement)
    conn.commit()
    apply_migrations(conn, ensure_columns)
