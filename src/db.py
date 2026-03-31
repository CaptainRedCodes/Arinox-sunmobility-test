import os
import ssl
import json
import logging
import asyncpg
from typing import Optional

logger = logging.getLogger(__name__)

_pool: Optional[asyncpg.Pool] = None
_pool_readonly: Optional[asyncpg.Pool] = None


def _ssl_ctx():
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    return ctx


async def get_pool() -> asyncpg.Pool:
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(
            dsn=os.getenv("DATABASE_URL"),
            ssl=_ssl_ctx(),
            min_size=1,
            max_size=3,
            command_timeout=30,
        )
    return _pool


async def get_pool_readonly() -> asyncpg.Pool:
    global _pool_readonly
    if _pool_readonly is None:
        _pool_readonly = await asyncpg.create_pool(
            dsn=os.getenv("DATABASE_URL"),
            ssl=_ssl_ctx(),
            min_size=1,
            max_size=3,
            command_timeout=30,
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


async def execute_query(sql: str, params: list) -> tuple[list, list[str]]:
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(sql, *params)
        if not rows:
            return [], []
        columns = list(rows[0].keys())
        data = [dict(r) for r in rows]
        return data, columns


async def execute_query_raw(sql: str, params: list) -> tuple[list, list[str]]:
    pool = await get_pool_readonly()
    async with pool.acquire() as conn:
        rows = await conn.fetch(sql, *params)
        if not rows:
            return [], []
        columns = list(rows[0].keys())
        data = [dict(r) for r in rows]
        return data, columns


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
