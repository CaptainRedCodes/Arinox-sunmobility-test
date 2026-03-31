import json
from typing import Optional
from pydantic import BaseModel, Field


class TableRef(BaseModel):
    name: str
    alias: str


class FilterValue(BaseModel):
    eq: Optional[str] = None
    in_: Optional[list] = Field(None, alias="in")
    gt: Optional[str] = None
    gte: Optional[str] = None
    lt: Optional[str] = None
    lte: Optional[str] = None
    like: Optional[str] = None


class OrderByItem(BaseModel):
    column: str
    dir: str = "asc"


class QuerySpec(BaseModel):
    action: str = "read"
    dataset: Optional[str] = None
    instruction_id: Optional[str] = None
    tables: list[TableRef]
    select: list[str]
    joins: Optional[list[dict]] = None
    filters: Optional[dict[str, FilterValue]] = None
    groupBy: Optional[list[str]] = None
    orderBy: Optional[list[OrderByItem]] = None
    limit: int = 1000
    offset: int = 0
    return_raw_sql: bool = False
    response_format: str = "json"


ALLOWED_TABLES = {"orders", "customers"}


def decode_instruction(instruction_str: str) -> QuerySpec:
    raw = json.loads(instruction_str)
    spec = QuerySpec(**raw)
    for t in spec.tables:
        if t.name not in ALLOWED_TABLES:
            raise ValueError(f"Table '{t.name}' is not allowed")
    return spec
