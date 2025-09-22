import abc
from typing import (
    Any,
    Callable,
    ClassVar,
    TypeVar,
)

import fastapi as fa
from sqlalchemy import orm
from toolkit.auth import AuthorizationContext

from .procedures import ServiceProcedure
from .transport import AbstractTransport, DirectTransport, HttpxTransport

TransportT = TypeVar("TransportT", bound=AbstractTransport)


class AbstractService(abc.ABC):
    router_prefix: ClassVar[str]
    transport: AbstractTransport

    def __init__(self, transport: AbstractTransport):
        self.transport = transport
        if isinstance(transport, DirectTransport):
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
        session_dep: Callable[..., Any],
        auth_dep: Callable[..., Any],
    ) -> fa.APIRouter:
        def svc_dep(
            session: orm.Session = fa.Depends(session_dep),
            auth_ctx: AuthorizationContext = fa.Depends(auth_dep),
        ) -> AbstractService:
            transport = DirectTransport(session, auth_ctx)
            return cls(transport)

        router = fa.APIRouter(prefix=cls.router_prefix)
        for proc in cls.collect_procedures():
            proc.register_endpoint(router, svc_dep)
        return router

    @classmethod
    def collect_procedures(cls) -> "list[ServiceProcedure[Any, Any, Any]]":
        procedures = []
        for _, val in vars(cls).items():
            if isinstance(val, ServiceProcedure):
                procedures.append(val)

        return procedures
