from contextlib import asynccontextmanager, suppress
from pathlib import Path
from typing import TYPE_CHECKING, Any, AsyncIterator, Callable, Coroutine

from sqlalchemy import orm
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response
from toolkit.auth import token
from toolkit.auth.context import AuthorizationContext
from toolkit.manager.client import ManagerClient

from ixmp4.conf.platforms import (
    Ixmp4Instance,
    ManagerPlatforms,
    TomlPlatform,
    TomlPlatforms,
)
from ixmp4.core.exceptions import BadRequest, Forbidden, PlatformNotFound
from ixmp4.transport import (
    AuthorizedTransport,
    DirectTransport,
    Session,
    cached_create_engine,
)

if TYPE_CHECKING:
    from .base import Service


class ServiceMiddleware(BaseHTTPMiddleware):
    service_class: type["Service"]

    def __init__(self, app: Any, service_class: type["Service"]):
        super().__init__(app)
        self.service_class = service_class

    async def dispatch(
        self,
        request: Request,
        call_next: Callable[[Request], Coroutine[Any, Any, Response]],
    ) -> Response:
        request.state.service = self.service_class(request.state.transport)
        return await call_next(request)


class TransportMiddleware(BaseHTTPMiddleware):
    auth_header = "authorization"

    secret_hs256: str | None
    manager_platforms: ManagerPlatforms
    toml_platforms: TomlPlatforms
    toml_file: Path | None
    manager_client: ManagerClient | None
    override_transport: DirectTransport | None

    def __init__(
        self,
        app: Any,
        secret_hs256: str | None,
        toml_file: Path | None = None,
        manager_client: ManagerClient | None = None,
        override_transport: DirectTransport | None = None,
    ):
        self.secret_hs256 = secret_hs256
        self.toml_file = toml_file
        self.manager_client = manager_client

        if manager_client is not None:
            self.manager_platforms = ManagerPlatforms(manager_client)
        if toml_file is not None:
            self.toml_platforms = TomlPlatforms(toml_file)

        self.override_transport = override_transport

        super().__init__(app)

    def get_platform_name(self, request: Request) -> str:
        return str(request.path_params.pop("platform"))

    async def get_auth_context(
        self,
        token: token.PreencodedToken | None,
        manager_client: ManagerClient,
    ) -> AuthorizationContext:
        user = getattr(token, "user", None)
        return AuthorizationContext(user, manager_client)

    async def validate_token(
        self,
        request: Request,
    ) -> token.PreencodedToken | None:
        """Validates a JSON Web Token with the secret from `secret_hs256`."""
        if self.secret_hs256 is None:
            return None

        authorization = request.headers.get(self.auth_header, None)
        if authorization is None:
            return None  # anonymous user

        try:
            bearer, encoded_jwt = authorization.split(" ")
        except ValueError:
            raise BadRequest(
                "Invalid 'Authorization' header. "
                "Make sure its value has the format of: 'Bearer <token>'"
            )

        return token.verify(encoded_jwt, self.secret_hs256)

    async def get_manager_platform(
        self,
        platform: str,
        auth_ctx: AuthorizationContext,
    ) -> Ixmp4Instance:
        ci = self.manager_platforms.get_platform(platform)
        auth_ctx.has_access_permission(
            ci,
            raise_exc=Forbidden(
                f"Access to platform '{platform}' denied due to insufficient permissions."
            ),
        )
        return ci

    async def get_toml_platform(
        self, platform: str, token: token.PreencodedToken | None
    ) -> TomlPlatform:
        ci = self.toml_platforms.get_platform(platform)
        return ci

    @asynccontextmanager
    async def yield_session(
        self,
        dsn: str,
    ) -> AsyncIterator[orm.Session]:
        engine = cached_create_engine(dsn)
        try:
            session = Session(bind=engine)
            yield session
        finally:
            session.rollback()
            session.close()

    def get_transport(
        self,
        session: orm.Session,
        platform: Ixmp4Instance | TomlPlatform,
        auth_ctx: AuthorizationContext | None,
    ) -> DirectTransport:
        if auth_ctx is not None:
            return AuthorizedTransport(session, auth_ctx, platform)
        else:
            return DirectTransport(session)

    async def dispatch(
        self,
        request: Request,
        call_next: Callable[[Request], Coroutine[Any, Any, Response]],
    ) -> Response:
        if self.override_transport is not None:
            request.state.transport = self.override_transport
            response = await call_next(request)
            return response

        platform_name = self.get_platform_name(request)
        token = await self.validate_token(request)

        auth_ctx: AuthorizationContext | None = None
        platform: Ixmp4Instance | TomlPlatform | None = None

        if self.manager_client is not None:
            auth_ctx = await self.get_auth_context(token, self.manager_client)
            with suppress(PlatformNotFound):
                platform = await self.get_manager_platform(platform_name, auth_ctx)
        if platform is None and self.toml_file is not None:
            auth_ctx = None  # TODO: figure out local auth context
            with suppress(PlatformNotFound):
                platform = await self.get_toml_platform(platform_name, token)

        if platform is None or platform.dsn.startswith("http"):
            raise PlatformNotFound(f"Platform '{platform_name}' was not found.")

        async with self.yield_session(platform.dsn) as session:
            transport = self.get_transport(session, platform, auth_ctx)
            request.state.transport = transport
            response = await call_next(request)

        return response
