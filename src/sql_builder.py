import re
from src.dsl import ALLOWED_COLUMNS


def _validate_column(col: str, table_name: str) -> bool:
    allowed = ALLOWED_COLUMNS.get(table_name)
    if allowed is None:
        return False
    return col.strip() in allowed


def _validate_alias(alias: str) -> bool:
    return bool(re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', alias.strip()))


def _validate_join_on(on_clause: str) -> bool:
    return bool(re.match(r'^[a-zA-Z_"().\s=,]*$', on_clause.strip()))


def _build_having_op(op: str) -> str:
    mapping = {
        "eq": "=", "gt": ">", "gte": ">=",
        "lt": "<", "lte": "<=", "ne": "!=",
    }
    return mapping.get(op.lower(), "=")


def _quote_col(col: str) -> str:
    return f'"{col.strip()}"'


def build_sql(spec) -> tuple[str, list]:
    sql_parts = []
    params = []

    select_kw = "SELECT DISTINCT" if spec.distinct else "SELECT"
    select_exprs = spec.select if spec.select else ["*"]
    for expr in select_exprs:
        if expr.strip() == "*":
            continue
        alias_match = re.split(r'\s+AS\s+', expr, flags=re.IGNORECASE)
        col_part = alias_match[0].strip()
        for t in spec.tables:
            if _validate_column(col_part, t.name):
                break
        else:
            raise ValueError(f"Column '{col_part}' not found in any allowed table")
    sql_parts.append(f"{select_kw} {', '.join(select_exprs)}")

    tables = spec.tables
    from_table = tables[0]
    if not _validate_alias(from_table.alias):
        raise ValueError(f"Invalid alias: {from_table.alias}")
    sql_parts.append(f'FROM "{from_table.name}" {from_table.alias}')

    if spec.joins:
        for j in spec.joins:
            jtype = j.get("type", "INNER").upper()
            if jtype not in ("INNER", "LEFT", "LEFT OUTER", "RIGHT", "RIGHT OUTER", "FULL", "CROSS"):
                raise ValueError(f"Unsupported join type: {jtype}")
            on = j.get("on", "")
            if not _validate_join_on(on):
                raise ValueError(f"Invalid join condition: {on}")
            if len(tables) > 1:
                right = tables[1]
                if not _validate_alias(right.alias):
                    raise ValueError(f"Invalid alias: {right.alias}")
                sql_parts.append(f'{jtype} JOIN "{right.name}" {right.alias} ON {on}')

    if spec.filters:
        conditions = []
        for col, fv in spec.filters.items():
            col_table = None
            for t in spec.tables:
                if _validate_column(col, t.name):
                    col_table = t.name
                    break
            if col_table is None:
                raise ValueError(f"Filter column '{col}' not found in any allowed table")
            for op in ("eq", "gt", "gte", "lt", "lte", "like"):
                val = getattr(fv, op, None)
                if val is not None:
                    param_idx = len(params) + 1
                    quoted = _quote_col(col)
                    if op == "eq":
                        conditions.append(f"{quoted} = ${param_idx}")
                    elif op == "gt":
                        conditions.append(f"{quoted} > ${param_idx}")
                    elif op == "gte":
                        conditions.append(f"{quoted} >= ${param_idx}")
                    elif op == "lt":
                        conditions.append(f"{quoted} < ${param_idx}")
                    elif op == "lte":
                        conditions.append(f"{quoted} <= ${param_idx}")
                    elif op == "like":
                        conditions.append(f"{quoted} ILIKE ${param_idx}")
                    params.append(val)
            in_vals = getattr(fv, "in_", None)
            if in_vals is not None:
                if len(in_vals) > 1000:
                    raise ValueError("IN list cannot exceed 1000 items")
                placeholders = []
                for v in in_vals:
                    param_idx = len(params) + 1
                    placeholders.append(f"${param_idx}")
                    params.append(v)
                conditions.append(f"{_quote_col(col)} IN ({', '.join(placeholders)})")
        if conditions:
            sql_parts.append(f"WHERE {' AND '.join(conditions)}")

    if spec.groupBy:
        for g in spec.groupBy:
            found = any(_validate_column(g, t.name) for t in spec.tables)
            if not found:
                raise ValueError(f"groupBy column '{g}' not found in any allowed table")
        sql_parts.append(f"GROUP BY {', '.join(_quote_col(g) for g in spec.groupBy)}")

    if spec.having:
        having_parts = []
        for h in spec.having:
            op = _build_having_op(h.op)
            param_idx = len(params) + 1
            having_parts.append(f"{h.column} {op} ${param_idx}")
            params.append(h.value)
        sql_parts.append(f"HAVING {' AND '.join(having_parts)}")

    if spec.orderBy:
        order_items = []
        for o in spec.orderBy:
            found = any(_validate_column(o.column, t.name) for t in spec.tables)
            if not found:
                raise ValueError(f"orderBy column '{o.column}' not found in any allowed table")
            direction = "ASC" if o.dir.upper() in ("ASC", "DESC") else "ASC"
            order_items.append(f"{_quote_col(o.column)} {direction}")
        sql_parts.append(f"ORDER BY {', '.join(order_items)}")

    sql_parts.append(f"LIMIT ${len(params) + 1}")
    params.append(spec.limit)
    sql_parts.append(f"OFFSET ${len(params) + 1}")
    params.append(spec.offset)

    sql = " ".join(sql_parts)
    return sql, params
