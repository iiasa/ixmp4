from typing import TYPE_CHECKING, Any, Generic, TypeVar, cast

from litestar import Controller, Request
from litestar.di import Provide

from ixmp4.conf.settings import Settings
from ixmp4.data.pagination import Pagination

if TYPE_CHECKING:
    from .base import Service
    from .procedure.endpoint import ProcedureRouteHandler

ServiceT = TypeVar("ServiceT", bound="Service")


default_settings = Settings()


async def get_pagination(
    offset: int = 0, limit: int = default_settings.server.default_page_size
) -> Pagination:
    return Pagination(limit=limit, offset=offset)


class ServiceController(Controller, Generic[ServiceT]):
    dependencies = {"pagination": Provide(get_pagination)}

    def get_handler(
        self, service: ServiceT, name: str
    ) -> "ProcedureRouteHandler[ServiceT, Any, Any]":
        return cast(
            "ProcedureRouteHandler[ServiceT, Any, Any]",
            getattr(type(service), name).procedure.handlers[service.__class__],
        )

    async def call_procedure(
        self,
        service: ServiceT,
        name: str,
        request: Request[Any, Any, Any],
    ) -> Any:
        handler = self.get_handler(service, name)

        bound_func = handler.bind_endpoint_func(service, dict(request.query_params))
        args, kwargs = handler.build_call_args(
            request.path_params, dict(request.query_params), await request.body()
        )
        return bound_func(*args, **kwargs)
