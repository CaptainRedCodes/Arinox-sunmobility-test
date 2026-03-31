import re


def _validate_identifier(ident: str) -> bool:
    return bool(re.match(r'^[a-zA-Z_][a-zA-Z0-9_.]*$', ident.strip()))


def _validate_join_on(on_clause: str) -> bool:
    return bool(re.match(r'^[a-zA-Z_][a-zA-Z0-9_.\s=,()]*$', on_clause.strip()))


def _build_having_op(op: str) -> str:
    mapping = {
        "eq": "=", "gt": ">", "gte": ">=",
        "lt": "<", "lte": "<=", "ne": "!=",
    }
    return mapping.get(op.lower(), "=")


def build_sql(spec) -> tuple[str, list]:
    sql_parts = []
    params = []

    select_kw = "SELECT DISTINCT" if spec.distinct else "SELECT"
    select_exprs = spec.select if spec.select else ["*"]
    for expr in select_exprs:
        for part in re.split(r'\s+AS\s+', expr, flags=re.IGNORECASE):
            for token in part.split(','):
                token = token.strip()
                if token and not _validate_identifier(token):
                    raise ValueError(f"Invalid select expression: {expr}")
    sql_parts.append(f"{select_kw} {', '.join(select_exprs)}")

    tables = spec.tables
    from_table = tables[0]
    if not _validate_identifier(from_table.name) or not _validate_identifier(from_table.alias):
        raise ValueError(f"Invalid table name or alias: {from_table.name} / {from_table.alias}")
    sql_parts.append(f"FROM {from_table.name} {from_table.alias}")

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
                if not _validate_identifier(right.name) or not _validate_identifier(right.alias):
                    raise ValueError(f"Invalid join table name or alias: {right.name} / {right.alias}")
                sql_parts.append(f"{jtype} JOIN {right.name} {right.alias} ON {on}")

    if spec.filters:
        conditions = []
        for col, fv in spec.filters.items():
            if not _validate_identifier(col):
                raise ValueError(f"Invalid filter column: {col}")
            for op in ("eq", "gt", "gte", "lt", "lte", "like"):
                val = getattr(fv, op, None)
                if val is not None:
                    param_idx = len(params) + 1
                    if op == "eq":
                        conditions.append(f"{col} = ${param_idx}")
                    elif op == "gt":
                        conditions.append(f"{col} > ${param_idx}")
                    elif op == "gte":
                        conditions.append(f"{col} >= ${param_idx}")
                    elif op == "lt":
                        conditions.append(f"{col} < ${param_idx}")
                    elif op == "lte":
                        conditions.append(f"{col} <= ${param_idx}")
                    elif op == "like":
                        conditions.append(f"{col} ILIKE ${param_idx}")
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
                conditions.append(f"{col} IN ({', '.join(placeholders)})")
        if conditions:
            sql_parts.append(f"WHERE {' AND '.join(conditions)}")

    if spec.groupBy:
        for g in spec.groupBy:
            if not _validate_identifier(g):
                raise ValueError(f"Invalid groupBy column: {g}")
        sql_parts.append(f"GROUP BY {', '.join(spec.groupBy)}")

    if spec.having:
        having_parts = []
        for h in spec.having:
            if not _validate_identifier(h.column):
                raise ValueError(f"Invalid HAVING column: {h.column}")
            op = _build_having_op(h.op)
            param_idx = len(params) + 1
            having_parts.append(f"{h.column} {op} ${param_idx}")
            params.append(h.value)
        sql_parts.append(f"HAVING {' AND '.join(having_parts)}")

    if spec.orderBy:
        order_items = []
        for o in spec.orderBy:
            if not _validate_identifier(o.column):
                raise ValueError(f"Invalid orderBy column: {o.column}")
            direction = "ASC" if o.dir.upper() in ("ASC", "DESC") else "ASC"
            order_items.append(f"{o.column} {direction}")
        sql_parts.append(f"ORDER BY {', '.join(order_items)}")

    sql_parts.append(f"LIMIT ${len(params) + 1}")
    params.append(spec.limit)
    sql_parts.append(f"OFFSET ${len(params) + 1}")
    params.append(spec.offset)

    sql = " ".join(sql_parts)
    return sql, params
