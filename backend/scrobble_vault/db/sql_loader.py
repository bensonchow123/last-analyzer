from functools import lru_cache
from pathlib import Path
import asyncio

SQL_ROOT = Path(__file__).resolve().parent.parent / "sql"

@lru_cache(maxsize=None) # this prevent it reading the files every single request
def _load_sql_sync(module_name: str, query_name: str) -> str:
    sql_path = SQL_ROOT / module_name / f"{query_name}.sql"
    return sql_path.read_text(encoding="utf-8").strip()

async def load_sql(module_name: str, query_name: str) -> str:
    """A
    sync wrapper, runs sync I/O in thread pool.
    Currently the SQL are loaded on startup, so it is not nessary for async,
    but this just for future proofing.
    """
    return await asyncio.to_thread(_load_sql_sync, module_name, query_name)