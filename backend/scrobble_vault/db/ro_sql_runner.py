import logging

import asyncpg
from sqlglot import parse, errors, exp

from scrobble_vault.db import core

logger = logging.getLogger(__name__)

ALLOWED_TABLES = {"artists", "albums", "tracks", "scrobbles", "last_sync"}

DANGEROUS_FUNCTIONS = {
    "pg_sleep", "pg_read_file", "pg_read_binary_file", "pg_ls_dir",
    "pg_stat_file", "pg_terminate_backend", "pg_cancel_backend",
    "pg_reload_conf", "pg_rotate_logfile",
    "dblink", "dblink_exec", "dblink_connect",
    "lo_import", "lo_export",
    "current_setting", "set_config",
}

DEFAULT_LIMIT = 100
MAX_LIMIT = 1000


def _sql_validator(query: str) -> str:
    """
    Validate and try to fix vibed SQL using sqlglot AST parsing.
    Returns:
        - str: the validated (and possibly fixed) SQL
    Raises:
        - ValueError: if SQL is unsafe and cannot be fixed
    """
    # Clean out the basic stuff
    if not query or not query.strip():
        raise ValueError("Query must not be empty.")

    # Strip markdown code fence from LLM
    query = query.strip()
    if query.startswith("```"):
        lines = query.splitlines()
        lines = [l for l in lines if not l.strip().startswith("```")]
        query = "\n".join(lines).strip()

    # Remove trailing semicolons
    query = query.rstrip(";").strip()

    # Parse into AST, rejects actually broken SQL
    try:
        statements = parse(query, dialect="postgres")
    except errors.ParseError as e:
        raise ValueError(f"SQL could not be parsed: {e}")

    # Filter to only SELECT statements if LLM generated multiple
    select_statements = [s for s in statements if isinstance(s, exp.Select)]
    if not select_statements:
        raise ValueError("Only SELECT queries are allowed.")

    # Use the first SELECT statement, discard the rest
    statement = select_statements[0]

    # Block non read operations hiding inside CTEs or subqueries
    for node in statement.walk():
        node_type = type(node)
        if node_type in (
            exp.Insert, exp.Update, exp.Delete, exp.Drop, exp.Create,
            exp.Alter, exp.AlterColumn, exp.Command, exp.Transaction,
            exp.Commit, exp.Rollback, exp.Set, exp.Grant,
        ):
            raise ValueError(f"Forbidden operation detected: {node_type.__name__}")

    # Block dangerous Postgres functions
    for func_call in statement.find_all(exp.Anonymous, exp.Func):
        func_name = (func_call.name or "").lower()
        if func_name in DANGEROUS_FUNCTIONS:
            raise ValueError(f"Dangerous function detected: {func_name}")

    # Only allow queries against known tables
    for table in statement.find_all(exp.Table):
        table_name = table.name.lower()
        if table_name not in ALLOWED_TABLES:
            raise ValueError(f"Table not allowed: {table_name}")

    # Add LIMIT if missing, or cap it if too high
    existing_limit = statement.find(exp.Limit)
    if not existing_limit:
        statement = statement.limit(DEFAULT_LIMIT)
    else:
        # Cap crazy limits
        limit_val = existing_limit.find(exp.Literal)
        if limit_val and limit_val.is_int:
            current = int(limit_val.this)
            if current > MAX_LIMIT:
                limit_val.set("this", str(MAX_LIMIT))

    return statement.sql(dialect="postgres")


async def ro_sql_runner(query: str, validation: bool = True) -> list[dict]:
    """
    Execute a read-only SQL query and return results as a list of dicts.
    When validation=True (default), runs strict LLM SQL validation first.
    Uses the read only connection pool.
    Values like numpy arrays (pgvector embeddings) are filtered out, as they
    are not serializable nor intended for direct LLM consumption.
    """
    if validation:
        query = _sql_validator(query)

    try:
        async with core.ro_pool.acquire() as conn:
            rows = await conn.fetch(query)
            
            cleaned_rows = []
            for row in rows:
                cleaned_row = {
                    k: v for k, v in dict(row).items() 
                    if not type(v).__name__ == "ndarray"
                }
                cleaned_rows.append(cleaned_row)
                
            return cleaned_rows
        
    except asyncpg.PostgresError:
        logger.exception(f"Read-only query failed:\n{query}")
        raise