import os
import sqlite3
import tempfile
from typing import Optional, Tuple


def create_temp_sqlite_db(schema_sql: Optional[str] = None) -> Tuple[sqlite3.Connection, str]:
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    if schema_sql:
        conn.executescript(schema_sql)
        conn.commit()
    return conn, path


def cleanup_temp_sqlite_db(conn: sqlite3.Connection, path: str) -> None:
    conn.close()
    if os.path.exists(path):
        os.unlink(path)
