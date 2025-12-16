import logging
from contextlib import asynccontextmanager, suppress
from typing import TYPE_CHECKING, Any, AsyncIterator, Awaitable, Callable, Sequence

from litestar import Controller, Litestar, Request, Response, Router, get
from litestar.datastructures import State
from litestar.di import Provide
from litestar.middleware import DefineMiddleware
from sqlalchemy import orm
from toolkit.auth.context import AuthorizationContext
from toolkit.auth.user import User
from toolkit.client.auth import SelfSignedAuth
from toolkit.manager.client import ManagerClient

from ixmp4 import data
from ixmp4.conf.platforms import (
    ManagerPlatforms,
    PlatformConnectionInfo,
    TomlPlatforms,
)
from ixmp4.conf.settings import ServerSettings
from ixmp4.core.exceptions import (
    Forbidden,
    Ixmp4Error,
    PlatformNotFound,
    registry,
)
from ixmp4.transport import (
    AuthorizedTransport,
    DirectTransport,
    Session,
    cached_create_engine,
)

if TYPE_CHECKING:
    from ixmp4.services import Service

from .middleware import AuthenticationMiddleware

logger = logging.getLogger(__name__)


v1_services: list[type["Service"]] = [
    data.RunMetaEntryService,
    data.ModelService,
    data.RegionService,
    data.RunService,
    data.ScenarioService,
    data.UnitService,
    data.CheckpointService,
    data.iamc.VariableService,
    data.iamc.TimeSeriesService,
    data.iamc.DataPointService,
    data.optimization.EquationService,
    data.optimization.IndexSetService,
    data.optimization.ParameterService,
    data.optimization.ScalarService,
    data.optimization.TableService,
    data.optimization.VariableService,
]


class V1HttpApi:
    service_classes: Sequence[type["Service"]] | None = None
    settings: ServerSettings

    router: Router
    platform_router: Router
    provide_platform: Provide | None = None
    provide_transport: Provide | None = None

    def __init__(
        self,
        settings: ServerSettings,
        override_transport: "Callable[..., Awaitable[DirectTransport]] | None" = None,
        service_classes: Sequence[type["Service"]] | None = None,
    ):
        if service_classes is None:
            service_classes = v1_services

        self.settings = settings
        self.provide_transport = Provide(override_transport or self.get_transport)
        self.provide_platform = Provide(self.get_platform)

        self.platform_router = Router(
            path="/{platform_name:str}",
            route_handlers=[PlatformController],
            dependencies={
                "platform": self.provide_platform,
                "transport": self.provide_transport,
            },
        )

        for service in service_classes:
            logger.info(f"Mounting {service.__name__}:")
            logger.info(f"   Path: {service.router.path}")
            logger.info(f"   Routes: {[route.path for route in service.router.routes]}")
            self.platform_router.register(service.router)

        auth_mw = DefineMiddleware(
            AuthenticationMiddleware, secret_hs256=settings.secret_hs256
        )

        self.router = Router(
            "/v1",
            middleware=[auth_mw],
            route_handlers=[self.platform_router],
            exception_handlers={Ixmp4Error: self.service_exception_handler},
        )

    def on_startup(self, app: Litestar) -> None:
        if (
            self.settings.manager_url is not None
            and self.settings.secret_hs256 is not None
        ):
            self_signed_auth = SelfSignedAuth(
                self.settings.secret_hs256.get_secret_value(), issuer="ixmp4"
            )
            app.state.manager_client = ManagerClient(
                str(self.settings.manager_url),
                self_signed_auth,
            )
            app.state.manager_platforms = ManagerPlatforms(app.state.manager_client)
        else:
            app.state.manager_client = None
            app.state.manager_platforms = None

        if self.settings.toml_platforms is not None:
            app.state.toml_platforms = TomlPlatforms(self.settings.toml_platforms)
        else:
            app.state.toml_platforms = None

    @staticmethod
    def service_exception_handler(
        request: Request[Any, Any, Any], exc: Ixmp4Error, /
    ) -> Response[dict[str, Any]]:
        exc_dict = registry.exception_to_response_dict(exc)
        logger.info(
            f"Received `{exc.__class__.__name__}` exception, "
            "returning appropriate error response."
        )
        return Response(
            exc_dict,
            status_code=exc.http_status_code,
        )

    async def get_platform(
        self,
        state: State,
        platform_name: str,
        request: Request[User | None, AuthorizationContext | None, Any],
    ) -> PlatformConnectionInfo:
        platform: PlatformConnectionInfo | None = None

        if state.manager_platforms is not None and request.auth is not None:
            with suppress(PlatformNotFound):
                platform = state.manager_platforms.get_platform(platform_name)
                request.auth.has_access_permission(
                    platform,
                    raise_exc=Forbidden(
                        f"Access to platform '{platform}' denied due "
                        "to insufficient permissions."
                    ),
                )

        if platform is None and state.toml_platforms is not None:
            with suppress(PlatformNotFound):
                platform = state.toml_platforms.get_platform(platform_name)

        if platform is None or platform.dsn.startswith("http"):
            raise PlatformNotFound(f"Platform '{platform_name}' was not found.")

        return platform

    @asynccontextmanager
    async def yield_session(self, dsn: str) -> AsyncIterator[orm.Session]:
        engine = cached_create_engine(dsn)
        try:
            session = Session(bind=engine)
            yield session
        finally:
            session.rollback()
            session.close()

    async def get_transport(
        self,
        platform: PlatformConnectionInfo,
        request: Request[User | None, AuthorizationContext | None, Any],
    ) -> AsyncIterator[DirectTransport]:
        async with self.yield_session(platform.dsn) as session:
            if request.auth is not None:
                yield AuthorizedTransport(session, request.auth, platform)
            else:
                yield DirectTransport(session)


class PlatformController(Controller):
    @get("/")
    async def info(self, platform: PlatformConnectionInfo) -> Response[dict[str, Any]]:
        return Response(
            {
                "slug": platform.slug,
                "name": platform.name,
            }
        )

    @get("/status")
    async def status(
        self,
        platform: PlatformConnectionInfo,
        request: Request[User | None, AuthorizationContext | None, Any],
    ) -> Response[dict[str, Any]]:
        status = {}
        if request.auth is not None:
            status["auth"] = {
                "access": request.auth.has_access_permission(platform),
                "view": request.auth.has_view_permission(platform),
                "submit": request.auth.has_submit_permission(platform),
                "edit": request.auth.has_edit_permission(platform),
                "manage": request.auth.has_management_permission(platform),
            }
        return Response(status)
