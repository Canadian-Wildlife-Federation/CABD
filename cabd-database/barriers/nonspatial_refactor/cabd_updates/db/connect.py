from __future__ import annotations

from contextlib import contextmanager
import psycopg2

@contextmanager
def connect(dsn: str):
    conn = psycopg2.connect(dsn)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
