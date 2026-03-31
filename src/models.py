from pydantic import BaseModel, Field
from typing import Optional


class QueryRequest(BaseModel):
    instruction: str = Field(..., description="JSON-encoded inner DSL describing the read query")


class QueryResponse(BaseModel):
    instruction_id: Optional[str] = None
    status: str
    row_count: int = 0
    data: list = []
    columns: list = []


class ErrorResponse(BaseModel):
    instruction_id: Optional[str] = None
    status: str = "error"
    error: dict
