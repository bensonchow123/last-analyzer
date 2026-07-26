import hmac
import logging

from fastapi import Header, HTTPException

from last_llm_service.env import env

logger = logging.getLogger(__name__)

def require_admin(authorization: str = Header(default="")):
    """Bearer check for /settings, the only mutating surface on this service.

    With no ADMIN_API_TOKEN the endpoint is open. On a default install every port
    binds to 127.0.0.1 and /settings is no more exposed than the chat routes, and
    locking it would leave a new user with no way to configure anything. Set a
    token before pointing LAST_LLM_API_BIND_IP at a VPN address.

    The browser never calls this route, the frontend proxies server side and
    holds the token, so the wildcard CORS gives no way in either.
    """
    token = env.ADMIN_API_TOKEN
    if not token:
        logger.warning("ADMIN_API_TOKEN is not set, /settings is unauthenticated")
        return

    scheme, _, presented = authorization.partition(" ")
    if scheme.lower() != "bearer" or not hmac.compare_digest(presented.encode(), token.encode()):
        raise HTTPException(status_code=401, detail="Invalid or missing admin token.")
