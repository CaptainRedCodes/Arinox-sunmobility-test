import os
import ssl
import json
import socket
import asyncio
import logging
import asyncpg
from urllib.parse import urlparse, urlunparse
from typing import Optional

logger = logging.getLogger(__name__)

_pool: Optional[asyncpg.Pool] = None
_pool_readonly: Optional[asyncpg.Pool] = None

MAX_RETRIES = 3
RETRY_DELAY_S = 2


def _force_ipv4_dsn(dsn: str) -> str:
    """Resolve the DSN hostname to an IPv4 address to avoid
    [Errno 101] Network is unreachable on hosts without IPv6."""
    try:
        parsed = urlparse(dsn)
        hostname = parsed.hostname
        if not hostname:
            return dsn
        # Try to resolve to IPv4 explicitly
        infos = socket.getaddrinfo(hostname, None, socket.AF_INET, socket.SOCK_STREAM)
        if infos:
            ipv4_addr = infos[0][4][0]
            # Replace hostname with IPv4 but keep everything else
            # We need to preserve the port, user, password, etc.
            new_netloc = parsed.netloc.replace(hostname, ipv4_addr)
            new_parsed = parsed._replace(netloc=new_netloc)
            resolved_dsn = urlunparse(new_parsed)
            logger.info(f"Resolved {hostname} -> {ipv4_addr}")
            return resolved_dsn
        return dsn
    except Exception as e:
        logger.warning(f"IPv4 resolution failed ({e}), using original DSN")
        return dsn


def _ssl_ctx():
    """SSL context that works with Supabase (don't verify hostname
    since we may have replaced it with an IP address)."""
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    return ctx


async def _create_pool_with_retry(
    dsn: str,
    *,
    min_size: int = 1,
    max_size: int = 3,
    command_timeout: int = 30,
    server_settings: Optional[dict] = None,
) -> asyncpg.Pool:
    """Create a connection pool with retry logic and IPv4 fallback."""
    last_err = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            connect_dsn = dsn
            # On retry (or always when IPv6 might fail), force IPv4
            if attempt > 1:
                connect_dsn = _force_ipv4_dsn(dsn)
            else:
                # Try IPv4 on first attempt too — Render free tier
                # often lacks IPv6 connectivity
                connect_dsn = _force_ipv4_dsn(dsn)

            pool_kwargs = dict(
                dsn=connect_dsn,
                ssl=_ssl_ctx(),
                min_size=min_size,
                max_size=max_size,
                command_timeout=command_timeout,
                timeout=30,
                # Supabase pooler (port 6543) uses transaction mode
                # which does NOT support prepared statements
                statement_cache_size=0,
            )
            if server_settings:
                pool_kwargs["server_settings"] = server_settings

            pool = await asyncpg.create_pool(**pool_kwargs)
            logger.info(f"DB pool created on attempt {attempt}")
            return pool
        except OSError as e:
            last_err = e
            logger.warning(
                f"Pool creation attempt {attempt}/{MAX_RETRIES} failed: "
                f"{type(e).__name__}: {e}"
            )
            if attempt < MAX_RETRIES:
                await asyncio.sleep(RETRY_DELAY_S * attempt)
        except Exception as e:
            last_err = e
            logger.error(f"Pool creation failed (non-retryable): {type(e).__name__}: {e}")
            raise
    raise RuntimeError(f"Failed to create DB pool after {MAX_RETRIES} attempts: {last_err}")


async def get_pool() -> asyncpg.Pool:
    global _pool
    if _pool is None:
        dsn = os.getenv("DATABASE_URL")
        if not dsn:
            logger.error("DATABASE_URL environment variable is not set")
            raise RuntimeError("DATABASE_URL environment variable is not set")
        logger.info(f"Connecting to DB: {dsn[:40]}...")
        _pool = await _create_pool_with_retry(dsn)
    return _pool


async def get_pool_readonly() -> asyncpg.Pool:
    global _pool_readonly
    if _pool_readonly is None:
        dsn = os.getenv("DATABASE_URL")
        if not dsn:
            raise RuntimeError("DATABASE_URL environment variable is not set")
        _pool_readonly = await _create_pool_with_retry(
            dsn,
            server_settings={
                "default_transaction_read_only": "on",
                "statement_timeout": "30000",
            },
        )
    return _pool_readonly


async def close_pool():
    global _pool, _pool_readonly
    if _pool:
        await _pool.close()
        _pool = None
    if _pool_readonly:
        await _pool_readonly.close()
        _pool_readonly = None


async def _safe_acquire(pool: asyncpg.Pool):
    """Acquire a connection, resetting the pool if stale."""
    try:
        return await pool.acquire()
    except (ConnectionResetError, asyncpg.ConnectionDoesNotExistError, OSError) as e:
        logger.warning(f"Connection stale ({e}), resetting pool...")
        global _pool, _pool_readonly
        if pool is _pool:
            await close_pool()
            pool = await get_pool()
        elif pool is _pool_readonly:
            if _pool_readonly:
                await _pool_readonly.close()
                _pool_readonly = None
            pool = await get_pool_readonly()
        return await pool.acquire()


async def execute_query(sql: str, params: list) -> tuple[list, list[str]]:
    pool = await get_pool()
    conn = await _safe_acquire(pool)
    try:
        rows = await conn.fetch(sql, *params)
        if not rows:
            return [], []
        columns = list(rows[0].keys())
        data = [dict(r) for r in rows]
        return data, columns
    finally:
        await pool.release(conn)


async def execute_query_raw(sql: str, params: list) -> tuple[list, list[str]]:
    pool = await get_pool_readonly()
    conn = await _safe_acquire(pool)
    try:
        rows = await conn.fetch(sql, *params)
        if not rows:
            return [], []
        columns = list(rows[0].keys())
        data = [dict(r) for r in rows]
        return data, columns
    finally:
        await pool.release(conn)


async def explain_analyze(sql: str, params: list) -> str:
    pool = await get_pool_readonly()
    async with pool.acquire() as conn:
        result = await conn.fetchval(f"EXPLAIN (ANALYZE, FORMAT JSON) {sql}", *params)
        return json.dumps(result, indent=2) if result else "No plan"


async def stream_query(sql: str, params: list, chunk_size: int = 500):
    pool = await get_pool_readonly()
    async with pool.acquire() as conn:
        async with conn.transaction():
            cursor = await conn.cursor(sql, *params)
            columns = None
            while True:
                rows = await cursor.fetch(chunk_size)
                if not rows:
                    break
                if columns is None:
                    columns = list(rows[0].keys())
                    yield json.dumps({"columns": columns}) + "\n"
                for row in rows:
                    yield json.dumps(dict(row)) + "\n"
