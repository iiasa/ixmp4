import logging
from typing import Any

from httpx import ConnectError
from litestar.connection import ASGIConnection
from litestar.middleware.authentication import (
    AbstractAuthenticationMiddleware,
    AuthenticationResult,
)
from litestar.types import ASGIApp
from pydantic import SecretStr
from toolkit.auth import token
from toolkit.auth.context import AuthorizationContext
from toolkit.manager.client import ManagerClient

from ixmp4.conf.settings import ServerSettings
from ixmp4.core.exceptions import (
    BadRequest,
)

logger = logging.getLogger(__name__)


class AuthenticationMiddleware(AbstractAuthenticationMiddleware):
    auth_header: str = "Authorization"
    settings: ServerSettings
    secret_hs256: SecretStr | None

    def __init__(
        self, app: ASGIApp, secret_hs256: SecretStr | None, **kwargs: Any
    ) -> None:
        super().__init__(app, **kwargs)
        self.secret_hs256 = secret_hs256

    async def authenticate_request(
        self, connection: ASGIConnection[Any, Any, Any, Any]
    ) -> AuthenticationResult:
        auth_header = connection.headers.get(self.auth_header)

        if self.secret_hs256 is not None:
            token = self.validate_token(auth_header, self.secret_hs256)
        else:
            token = None

        auth_context: AuthorizationContext | None = None

        try:
            manager_client = connection.app.state.manager_client
            auth_context = self.get_auth_context(token, manager_client)
        except ConnectError:
            pass

        return AuthenticationResult(
            user=getattr(token, "user", None), auth=auth_context
        )

    def validate_token(
        self, auth_header: str | None, secret_hs256: SecretStr
    ) -> token.PreencodedToken | None:
        """Validates a JSON Web Token with the secret from `secret_hs256`."""
        if auth_header is None:
            return None  # anonymous user

        try:
            bearer, encoded_jwt = auth_header.split(" ")
        except ValueError:
            raise BadRequest(
                "Invalid 'Authorization' header. "
                "Make sure its value has the format of: 'Bearer <token>'"
            )

        return token.verify(encoded_jwt, secret_hs256.get_secret_value())

    def get_auth_context(
        self, token: token.PreencodedToken | None, manager_client: ManagerClient | None
    ) -> AuthorizationContext | None:
        if manager_client is None:
            return None

        return AuthorizationContext(getattr(token, "user", None), manager_client)
