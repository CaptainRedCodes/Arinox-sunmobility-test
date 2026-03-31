import os
import time
import json
import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI, Depends, HTTPException
from fastapi.responses import StreamingResponse
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request

from src.models import QueryRequest, RawSqlRequest, QueryResponse
from src.dsl import decode_instruction
from src.sql_builder import build_sql
from src.sql_guard import is_read_only
from src.db import (
    execute_query,
    execute_query_raw,
    explain_analyze,
    stream_query,
    close_pool,
)
from src.security import verify_api_key

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def _safe_int(val: str | None, default: int) -> int:
    try:
        return int(val)
    except (ValueError, TypeError):
        return default


def _safe_float(val: str | None, default: float) -> float:
    try:
        return float(val)
    except (ValueError, TypeError):
        return default


SLOW_QUERY_THRESHOLD_MS = _safe_float(os.getenv("SLOW_QUERY_THRESHOLD_MS"), 2000)
REQUEST_TIMEOUT_MS = _safe_int(os.getenv("REQUEST_TIMEOUT_MS"), 30000)


class TimeoutMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint):
        try:
            import asyncio
            return await asyncio.wait_for(
                call_next(request),
                timeout=REQUEST_TIMEOUT_MS / 1000,
            )
        except TimeoutError:
            return StreamingResponse(
                iter([json.dumps({"error": "Request timed out"}).encode()]),
                status_code=504,
                media_type="application/json",
            )


@asynccontextmanager
async def lifespan(app: FastAPI):
    yield
    await close_pool()


app = FastAPI(lifespan=lifespan)
app.add_middleware(TimeoutMiddleware)


@app.get("/healthz")
async def health():
    return {"status": "ok"}


async def _run_query(instruction: str) -> QueryResponse:
    """Shared query logic for both GET and POST endpoints."""
    start = time.time()
    try:
        spec = decode_instruction(instruction)
        instruction_id = spec.instruction_id or "unknown"
        sql, params = build_sql(spec)

        if spec.return_raw_sql:
            return QueryResponse(
                instruction_id=instruction_id,
                status="ok",
                row_count=0,
                data=[{"sql": sql, "params": [str(p) for p in params]}],
                columns=["sql", "params"],
            )

        data, columns = await execute_query(sql, params)
        elapsed_ms = (time.time() - start) * 1000

        if elapsed_ms > SLOW_QUERY_THRESHOLD_MS:
            try:
                plan = await explain_analyze(sql, params)
                logger.warning(
                    f"SLOW_QUERY instruction_id={instruction_id} time={elapsed_ms:.0f}ms\n{plan}"
                )
            except Exception:
                pass

        logger.info(
            f"instruction_id={instruction_id} rows={len(data)} time={elapsed_ms:.0f}ms"
        )
        return QueryResponse(
            instruction_id=instruction_id,
            status="ok",
            row_count=len(data),
            data=data,
            columns=columns,
        )
    except ValueError as e:
        logger.error(f"validation_error: {e}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"query_error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/v1/query", response_model=QueryResponse)
async def handle_query(request: Request, _=Depends(verify_api_key)):
    # Try body first, then fall back to X-Instruction header
    instruction = None
    try:
        body = await request.json()
        instruction = body.get("instruction")
    except Exception:
        pass
    if not instruction:
        instruction = request.headers.get("X-Instruction")
    if not instruction:
        raise HTTPException(
            status_code=400,
            detail="Provide instruction in JSON body or X-Instruction header",
        )
    return await _run_query(instruction)


@app.get("/api/v1/query", response_model=QueryResponse)
async def handle_query_get(
    request: Request,
    instruction: str | None = None,
    _=Depends(verify_api_key),
):
    # Priority: query param > header
    instr = instruction or request.headers.get("X-Instruction")
    if not instr:
        raise HTTPException(
            status_code=400,
            detail="Provide instruction as ?instruction= query param or X-Instruction header",
        )
    return await _run_query(instr)



@app.post("/api/v1/query/stream")
async def handle_query_stream(req: QueryRequest, _=Depends(verify_api_key)):
    try:
        spec = decode_instruction(req.instruction)
        sql, params = build_sql(spec)
        return StreamingResponse(
            stream_query(sql, params),
            media_type="application/x-ndjson",
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/v1/raw", response_model=QueryResponse)
async def handle_raw_sql(req: RawSqlRequest, _=Depends(verify_api_key)):
    start = time.time()
    try:
        safe, reason = is_read_only(req.sql)
        if not safe:
            raise ValueError(f"SQL rejected: {reason}")

        if req.explain:
            plan = await explain_analyze(req.sql, req.params)
            return QueryResponse(
                status="ok",
                explain_plan=plan,
            )

        data, columns = await execute_query_raw(req.sql, req.params)
        elapsed_ms = (time.time() - start) * 1000

        if elapsed_ms > SLOW_QUERY_THRESHOLD_MS:
            try:
                plan = await explain_analyze(req.sql, req.params)
                logger.warning(f"SLOW_QUERY time={elapsed_ms:.0f}ms sql={req.sql[:200]}\n{plan}")
            except Exception:
                pass

        logger.info(f"raw_query rows={len(data)} time={elapsed_ms:.0f}ms")
        return QueryResponse(
            status="ok",
            row_count=len(data),
            data=data,
            columns=columns,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
