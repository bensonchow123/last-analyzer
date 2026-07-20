from typing import Literal

from pydantic import BaseModel, Field

from fastapi import HTTPException
from asyncpg import PostgresError

from scrobble_vault.db import semantic_search as db

class SemanticSearchRequest(BaseModel):
    text: str
    kind: Literal["artists", "albums", "tracks"] = "artists"
    limit: int = Field(default=10, ge=1, le=50)

async def semantic_search(request: SemanticSearchRequest):
    """Rank artists, albums or tracks by embedding similarity to a free text query."""
    try:
        results = await db.semantic_search(request.text, request.kind, request.limit)
        return {"results": results, "count": len(results)}
    except PostgresError:
        raise HTTPException(status_code=500, detail="The database errored out.")
