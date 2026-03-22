"""
Database connection pool and query helpers.
Uses psycopg2 ThreadedConnectionPool for Railway PostgreSQL.
"""

import os
import logging
from contextlib import contextmanager

import psycopg2
from psycopg2.pool import ThreadedConnectionPool
from psycopg2.extras import RealDictCursor, execute_batch

logger = logging.getLogger(__name__)

# Global pool — initialized on first use
_pool: ThreadedConnectionPool = None


def init_pool(database_url: str = None, minconn: int = 2, maxconn: int = 10):
    """Initialize the connection pool. Call once at app startup."""
    global _pool
    url = database_url or os.getenv("DATABASE_URL")
    if not url:
        raise RuntimeError("DATABASE_URL not set")
    _pool = ThreadedConnectionPool(minconn, maxconn, url)
    logger.info(f"DB pool initialized (min={minconn}, max={maxconn})")


def get_pool() -> ThreadedConnectionPool:
    global _pool
    if _pool is None:
        init_pool()
    return _pool


@contextmanager
def get_conn():
    """Get a connection from the pool, auto-return on exit."""
    pool = get_pool()
    conn = pool.getconn()
    try:
        yield conn
    finally:
        pool.putconn(conn)


@contextmanager
def get_cursor(commit: bool = False):
    """
    Get a RealDictCursor. Auto-commits if commit=True, else auto-rollbacks on error.
    
    Usage:
        with get_cursor() as cur:
            cur.execute("SELECT ...")
            rows = cur.fetchall()
        
        with get_cursor(commit=True) as cur:
            cur.execute("INSERT ...")
    """
    with get_conn() as conn:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        try:
            yield cur
            if commit:
                conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cur.close()


def query(sql: str, params: tuple = None) -> list[dict]:
    """Execute SELECT, return list of dicts."""
    with get_cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchall()


def query_one(sql: str, params: tuple = None) -> dict | None:
    """Execute SELECT, return single dict or None."""
    with get_cursor() as cur:
        cur.execute(sql, params)
        row = cur.fetchone()
        return dict(row) if row else None


def execute(sql: str, params: tuple = None) -> int:
    """Execute INSERT/UPDATE/DELETE, return rowcount."""
    with get_cursor(commit=True) as cur:
        cur.execute(sql, params)
        return cur.rowcount


def execute_returning(sql: str, params: tuple = None) -> dict | None:
    """Execute INSERT/UPDATE with RETURNING clause, return the row."""
    with get_cursor(commit=True) as cur:
        cur.execute(sql, params)
        row = cur.fetchone()
        return dict(row) if row else None


def execute_many_batch(sql: str, params_list: list[tuple], page_size: int = 100) -> int:
    """Batch execute using psycopg2 execute_batch for performance."""
    with get_cursor(commit=True) as cur:
        execute_batch(cur, sql, params_list, page_size=page_size)
        return cur.rowcount


def close_pool():
    """Close all connections. Call on app shutdown."""
    global _pool
    if _pool:
        _pool.closeall()
        _pool = None
        logger.info("DB pool closed")
