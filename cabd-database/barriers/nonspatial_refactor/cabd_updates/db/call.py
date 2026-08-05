from __future__ import annotations

from typing import Any, Iterable

def call_scalar(conn, sql: str, params: Iterable[Any] | None = None):
    with conn.cursor() as cur:
        cur.execute(sql, params or [])
        row = cur.fetchone()
        return row[0] if row else None

def call_rows(conn, sql: str, params: Iterable[Any] | None = None):
    with conn.cursor() as cur:
        cur.execute(sql, params or [])
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]
