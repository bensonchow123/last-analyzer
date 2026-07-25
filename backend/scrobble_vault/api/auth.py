import hmac

from fastapi import Header, HTTPException

from scrobble_vault.env import env

def require_admin(authorization: str = Header(default="")):
    """Bearer check for /settings, the only mutating surface on this service.

    Fails closed: with no ADMIN_API_TOKEN set the endpoints stay off rather than
    handing credentials to anything that can reach the port. The browser never
    calls these routes, the frontend proxies server side and holds the token, so
    the wildcard CORS above gives no way in.
    """
    token = env.ADMIN_API_TOKEN
    if not token:
        raise HTTPException(
            status_code=503,
            detail="Settings API is disabled, set ADMIN_API_TOKEN to enable it.",
        )

    scheme, _, presented = authorization.partition(" ")
    if scheme.lower() != "bearer" or not hmac.compare_digest(presented.encode(), token.encode()):
        raise HTTPException(status_code=401, detail="Invalid or missing admin token.")
