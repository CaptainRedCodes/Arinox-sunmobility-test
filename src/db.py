import os
import ssl
import asyncpg
from typing import Optional

_pool: Optional[asyncpg.Pool] = None


async def get_pool() -> asyncpg.Pool:
    global _pool
    if _pool is None:
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        _pool = await asyncpg.create_pool(
            dsn=os.getenv("DATABASE_URL"),
            ssl=ctx,
            min_size=2,
            max_size=10,
            command_timeout=60,
        )
    return _pool


async def close_pool():
    global _pool
    if _pool:
        await _pool.close()
        _pool = None


async def execute_query(sql: str, params: list) -> tuple[list, list[str]]:
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(sql, *params)
        if not rows:
            return [], []
        columns = list(rows[0].keys())
        data = [dict(r) for r in rows]
        return data, columns
