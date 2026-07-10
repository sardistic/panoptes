"""Shared runtime-state backend selection and PostgreSQL coordination.

SQLite remains the dependency-free local default. Set APB_STATE_DATABASE_URL to a
PostgreSQL DSN in a scaled deployment so snapshots, buffers, fused events, poller
leadership, and notification claims are shared across every service instance.
"""
from __future__ import annotations

import os
from contextlib import contextmanager
from typing import Iterator

_LEADER_LOCK_ID = 7_120_240_918
_leader_conn = None


def database_url() -> str | None:
    value = os.environ.get("APB_STATE_DATABASE_URL", "").strip()
    return value or None


def is_postgres() -> bool:
    url = database_url()
    if not url:
        return False
    if not url.startswith(("postgresql://", "postgres://")):
        raise ValueError("APB_STATE_DATABASE_URL must use postgresql:// or postgres://")
    return True


@contextmanager
def pg_connection() -> Iterator:
    """Yield one transaction-scoped psycopg connection with dictionary rows."""
    url = database_url()
    if not url:
        raise RuntimeError("APB_STATE_DATABASE_URL is not configured")
    import psycopg
    from psycopg.rows import dict_row

    with psycopg.connect(url, row_factory=dict_row, connect_timeout=10) as connection:
        yield connection


def acquire_poller_leadership() -> bool:
    """Hold a session advisory lock so only one scaled instance polls upstreams."""
    global _leader_conn
    if not is_postgres():
        return True
    if _leader_conn is not None:
        return True
    import psycopg

    connection = psycopg.connect(database_url(), autocommit=True, connect_timeout=10)
    row = connection.execute("SELECT pg_try_advisory_lock(%s) AS acquired",
                             (_LEADER_LOCK_ID,)).fetchone()
    if row and row[0]:
        _leader_conn = connection
        return True
    connection.close()
    return False


def release_poller_leadership() -> None:
    global _leader_conn
    if _leader_conn is None:
        return
    try:
        _leader_conn.execute("SELECT pg_advisory_unlock(%s)", (_LEADER_LOCK_ID,))
    finally:
        _leader_conn.close()
        _leader_conn = None
