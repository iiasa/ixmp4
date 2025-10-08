from fastapi import Depends, Header
from toolkit.auth.context import AuthorizationContext
from toolkit.client.auth import SelfSignedAuth
from toolkit.manager.client import ManagerClient
from toolkit.token import PreencodedToken, verify

from ixmp4.rewrite.conf import settings
from ixmp4.rewrite.transport import DirectTransport

auth = SelfSignedAuth(settings.secret_hs256, "ixmp4")
client = ManagerClient(str(settings.manager_url), auth=auth)


async def validate_token(
    authorization: str | None = Header(None),
) -> PreencodedToken | None:
    """Validates a JSON Web Token with the secret from `settings.secret_hs256`."""

    if authorization is None:
        return None  # anonymous user

    encoded_jwt = authorization.split(" ")[1]
    return verify(encoded_jwt, settings.secret_hs256)


async def auth_ctx(
    token: PreencodedToken | None = Depends(validate_token),
) -> AuthorizationContext:
    return AuthorizationContext(getattr(token, "user", None), client)


async def transport(
    auth_ctx: AuthorizationContext = Depends(auth_ctx),
) -> AuthorizationContext:
    DirectTransport(auth_ctx)
    return AuthorizationContext(getattr(token, "user", None), client)
