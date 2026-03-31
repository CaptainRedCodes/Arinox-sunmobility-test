from pydantic import BaseModel, Field
from typing import Optional


class QueryRequest(BaseModel):
    instruction: str = Field(..., description="JSON-encoded inner DSL describing the read query")


class RawSqlRequest(BaseModel):
    sql: str = Field(..., description="Raw SQL query (read-only only)")
    params: list = Field(default_factory=list, description="Parameter values for $1, $2, etc.")
    explain: bool = Field(default=False, description="If true, return EXPLAIN ANALYZE instead of data")


class QueryResponse(BaseModel):
    instruction_id: Optional[str] = None
    status: str
    row_count: int = 0
    data: list = []
    columns: list = []
    explain_plan: Optional[str] = None


class ErrorResponse(BaseModel):
    instruction_id: Optional[str] = None
    status: str = "error"
    error: dict
