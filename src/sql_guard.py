import re


DANGEROUS_KEYWORDS = [
    "INSERT", "UPDATE", "DELETE", "DROP", "CREATE", "ALTER", "TRUNCATE",
    "GRANT", "REVOKE", "EXEC", "EXECUTE", "CALL", "COMMIT", "ROLLBACK",
    "SAVEPOINT", "SET ROLE", "SET SESSION", "COPY FROM", "\\i", "\\copy",
]


def is_read_only(sql: str) -> tuple[bool, str]:
    upper = sql.strip().upper()
    if not upper.startswith("SELECT") and not upper.startswith("WITH") and not upper.startswith("EXPLAIN"):
        return False, f"Query must start with SELECT, WITH, or EXPLAIN. Got: {upper.split()[0] if upper.split() else 'empty'}"
    for kw in DANGEROUS_KEYWORDS:
        if re.search(r'\b' + kw + r'\b', upper):
            return False, f"Dangerous keyword found: {kw}"
    return True, ""
