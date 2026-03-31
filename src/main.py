import os
import time
import logging
from fastapi import FastAPI, Depends, HTTPException
from src.models import QueryRequest, QueryResponse, ErrorResponse
from src.dsl import decode_instruction
from src.sql_builder import build_sql
from src.db import execute_query
from src.security import verify_api_key

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()


@app.get("/healthz")
async def health():
    return {"status": "ok"}


@app.post("/api/v1/query", response_model=QueryResponse)
async def handle_query(req: QueryRequest, _=Depends(verify_api_key)):
    start = time.time()
    try:
        spec = decode_instruction(req.instruction)
        instruction_id = spec.instruction_id or "unknown"
        sql, params = build_sql(spec)
        if spec.return_raw_sql:
            return QueryResponse(
                instruction_id=instruction_id,
                status="ok",
                row_count=0,
                data=[],
                columns=["sql", "params"],
            )
        data, columns = await execute_query(sql, params)
        elapsed = time.time() - start
        logger.info(
            f"instruction_id={instruction_id} rows={len(data)} time={elapsed:.3f}s"
        )
        return QueryResponse(
            instruction_id=instruction_id,
            status="ok",
            row_count=len(data),
            data=data,
            columns=columns,
        )
    except Exception as e:
        logger.error(f"query_error: {e}")
        raise HTTPException(status_code=400, detail=str(e))
