import re


def _validate_column(col: str) -> bool:
    return bool(re.match(r'^[a-zA-Z_][a-zA-Z0-9_.\s,()]*$', col))


def build_sql(spec) -> tuple[str, list]:
    sql_parts = []
    params = []

    select_exprs = spec.select if spec.select else ["*"]
    for expr in select_exprs:
        if not _validate_column(expr):
            raise ValueError(f"Invalid select expression: {expr}")
    sql_parts.append(f"SELECT {', '.join(select_exprs)}")

    tables = spec.tables
    from_table = tables[0]
    sql_parts.append(f"FROM {from_table.name} {from_table.alias}")

    if spec.joins:
        for j in spec.joins:
            jtype = j.get("type", "INNER").upper()
            if jtype not in ("INNER", "LEFT", "LEFT OUTER", "RIGHT", "RIGHT OUTER", "FULL", "CROSS"):
                raise ValueError(f"Unsupported join type: {jtype}")
            on = j.get("on", "")
            if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_.\s=,()]*$', on):
                raise ValueError(f"Invalid join condition: {on}")
            if len(tables) > 1:
                right = tables[1]
                sql_parts.append(f"{jtype} JOIN {right.name} {right.alias} ON {on}")

    if spec.filters:
        conditions = []
        for col, fv in spec.filters.items():
            if not _validate_column(col):
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
            if not _validate_column(g):
                raise ValueError(f"Invalid groupBy column: {g}")
        sql_parts.append(f"GROUP BY {', '.join(spec.groupBy)}")

    if spec.orderBy:
        order_items = []
        for o in spec.orderBy:
            if not _validate_column(o.column):
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
