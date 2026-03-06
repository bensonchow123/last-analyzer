from pydantic import BaseModel

from fast_api import HTTPException
from asyncpg import PostgresError

from scrobble_vault.db import ro_sql_runner

class QueryRequest(BaseModel):
    sql: str

async def vibed_sql_runner(request: QueryRequest):
    try:
        await ro_sql_runner(request.sql)
    except ValueError:
        raise(HTTPException(status_code=422, detail="The SQL is not valid"))
    except PostgresError:
        raise(PostgresError(status_code=500, detail="The database errored out."))
