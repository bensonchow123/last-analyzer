from functools import lru_cache
from pathlib import Path


SQL_ROOT = Path(__file__).resolve().parent.parent / "sql"


@lru_cache(maxsize=None) # this prevent it reading the files every single request
def load_sql(module_name: str, query_name: str) -> str:
    """Load a SQL statement from the sql directory."""
    sql_path = SQL_ROOT / module_name / f"{query_name}.sql"
    return sql_path.read_text(encoding="utf-8").strip()