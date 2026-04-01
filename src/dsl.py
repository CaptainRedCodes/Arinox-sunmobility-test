import json
from typing import Optional, Any
from pydantic import BaseModel, Field


class TableRef(BaseModel):
    name: str
    alias: str


class FilterValue(BaseModel):
    eq: Optional[Any] = None
    in_: Optional[list] = Field(None, alias="in")
    gt: Optional[Any] = None
    gte: Optional[Any] = None
    lt: Optional[Any] = None
    lte: Optional[Any] = None
    like: Optional[str] = None


class OrderByItem(BaseModel):
    column: str
    dir: str = "asc"


class Aggregation(BaseModel):
    func: str
    column: str
    alias: str


class HavingCondition(BaseModel):
    column: str
    op: str
    value: Any


class QuerySpec(BaseModel):
    action: str = "read"
    dataset: Optional[str] = None
    instruction_id: Optional[str] = None
    tables: list[TableRef]
    select: list[str]
    distinct: bool = False
    joins: Optional[list[dict]] = None
    filters: Optional[dict[str, FilterValue]] = None
    aggregations: Optional[list[Aggregation]] = None
    groupBy: Optional[list[str]] = None
    having: Optional[list[HavingCondition]] = None
    orderBy: Optional[list[OrderByItem]] = None
    limit: int = 1000
    offset: int = 0
    return_raw_sql: bool = False
    response_format: str = "json"


ALLOWED_TABLES = {"station_data","vehicle_data"}

ALLOWED_COLUMNS = {
    "station_data": {
        "Transaction Id", "Station Id", "Vehicle Id", "Swap Start Time",
        "Swap End Time", "Duration (In Sec)", "Received BP Id",
        "Received BP Id Type", "Received Dock Id", "Received mDock Id",
        "Received SOC", "RegenAh", "Max Temp", "Issued BP Id",
        "Issued BP Id Type", "Issued Dock Id", "Issued SOC",
        "SOC Utilization", "Swap Type", "Total kWh Consumed",
        "Target DockId", "Cold TPH", "VType", "No. of BPs",
        "Status", "Status Reason", "Failure Reason",
        "Received PostSwap BpId", "Received PostSwap CycleNumber",
        "Received PostSwap PackVoltage", "Received PostSwap kwHr",
        "Received PostSwap Soc", "Received PostSwap CycleTime",
        "Received PostSwap AmpHr", "Received PostSwap Distance(GPS)",
        "Received PostSwap Distance(CAN)", "sd_id","vd_id"
    },
}


def decode_instruction(instruction) -> QuerySpec:
    if isinstance(instruction, str):
        raw = json.loads(instruction)
    elif isinstance(instruction, dict):
        raw = instruction
    else:
        raise ValueError(f"instruction must be str or dict, got {type(instruction).__name__}")
    spec = QuerySpec(**raw)
    if len(spec.tables) == 0:
        raise ValueError("At least one table is required")
    if len(spec.tables) > 2:
        raise ValueError("Maximum 2 tables allowed")
    for t in spec.tables:
        if t.name not in ALLOWED_TABLES:
            raise ValueError(f"Table '{t.name}' is not allowed")
    if spec.limit < 1:
        raise ValueError("limit must be >= 1")
    if spec.limit > 10000:
        raise ValueError("limit must be <= 10000")
    if spec.offset < 0:
        raise ValueError("offset must be >= 0")
    if spec.aggregations:
        allowed_funcs = {"COUNT", "SUM", "AVG", "MIN", "MAX"}
        for agg in spec.aggregations:
            if agg.func.upper() not in allowed_funcs:
                raise ValueError(f"Aggregation func '{agg.func}' not allowed. Use: {allowed_funcs}")
    if spec.having:
        allowed_ops = {"eq", "gt", "gte", "lt", "lte", "ne"}
        for h in spec.having:
            if h.op.lower() not in allowed_ops:
                raise ValueError(f"HAVING op '{h.op}' not allowed. Use: {allowed_ops}")
    return spec
