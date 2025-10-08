import abc
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    ClassVar,
    TypeVar,
)

import fastapi as fa
from toolkit.auth.context import AuthorizationContext
from toolkit.manager.models import Ixmp4Instance

from ixmp4.rewrite.transport import DirectTransport, HttpxTransport, Transport

if TYPE_CHECKING:
    from .procedures import ServiceProcedure


TransportT = TypeVar("TransportT", bound=Transport)


class Service(abc.ABC):
    router_prefix: ClassVar[str]
    transport: Transport
    auth_ctx: AuthorizationContext
    platform: Ixmp4Instance

    def __init__(self, transport: Transport):
        self.transport = transport
        if isinstance(transport, DirectTransport):
            self.auth_ctx = transport.auth_ctx
            self.platform = transport.platform
            self.__init_direct__(transport)
        elif isinstance(transport, HttpxTransport):
            self.__init_httpx__(transport)

    def __init_direct__(self, transport: DirectTransport) -> None:
        pass

    def __init_httpx__(self, transport: HttpxTransport) -> None:
        pass

    @classmethod
    def build_router(
        cls,
        transport_dep: Callable[..., Any],
    ) -> fa.APIRouter:
        def svc_dep(
            transport: DirectTransport = fa.Depends(transport_dep),
        ) -> Service:
            return cls(transport)

        router = fa.APIRouter(prefix=cls.router_prefix)
        for proc in cls.collect_procedures():
            proc.register_endpoint(router, svc_dep)
        return router

    @classmethod
    def collect_procedures(cls) -> "list[ServiceProcedure[Any, Any, Any]]":
        from .procedures import ServiceProcedure

        procedures = []
        for _, val in vars(cls).items():
            if isinstance(val, ServiceProcedure):
                procedures.append(val)

        return procedures
