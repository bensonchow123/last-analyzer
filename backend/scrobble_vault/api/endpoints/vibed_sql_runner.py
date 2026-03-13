from pydantic import BaseModel

from fastapi import HTTPException
from asyncpg import PostgresError

from scrobble_vault.db.ro_sql_runner import ro_sql_runner

class QueryRequest(BaseModel):
    sql: str
    
async def vibed_sql_runner(request: QueryRequest):
    """Pass SQL to the DB to be executed, mapping DB errors to HTTP error codes."""
    try:
        result = await ro_sql_runner(request.sql, validation=True)
        if not result:
            return {"columns": [], "rows": [], "row_count": 0}
        
        # Format the results into a JSON object that is easy to use
        columns = list(result[0].keys())
        rows = [list(row.values()) for row in result]
        return {
            "columns": columns,
            "rows": rows,
            "row_count": len(rows)
        }
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))
    except PostgresError:
        raise HTTPException(status_code=500, detail="The database errored out.")
