import abc
import functools
from typing import Any, Callable, Concatenate, Generic, ParamSpec, TypeVar

import fastapi as fa
import httpx
from sqlalchemy import orm
from toolkit.auth import AuthorizationContext
from toolkit.exceptions import ProgrammingError


class AbstractService(abc.ABC):
    session: orm.Session
    auth_ctx: AuthorizationContext

    def __init__(self, session: orm.Session, auth_ctx: AuthorizationContext):
        self.session = session
        self.auth_ctx = auth_ctx

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
            return cls(session, auth_ctx)

        router = fa.APIRouter()
        for proc in cls.collect_procedures():
            for func in proc.endpoint_functions:
                func(router, svc_dep)

        return router

    @classmethod
    def collect_procedures(cls) -> "list[ServiceProcedure[Any, Any, Any]]":
        procedures = []
        for _, val in vars(cls).items():
            if isinstance(val, ServiceProcedure):
                procedures.append(val)

        return procedures


ReturnT = TypeVar("ReturnT")
Params = ParamSpec("Params")

HttpReturnT = TypeVar("HttpReturnT")
HttpParams = ParamSpec("HttpParams")

EndpointFunc = Callable[[fa.APIRouter, Callable[..., Any]], None]
ClientFunc = Callable[[httpx.Client], ReturnT]

ServiceT = TypeVar("ServiceT", bound=AbstractService)


class ServiceProcedure(Generic[ServiceT, Params, ReturnT]):
    func: Callable[Concatenate[ServiceT, Params], ReturnT]
    service_class: type[AbstractService]
    endpoint_functions: list[EndpointFunc]
    client_function: ClientFunc[ReturnT]

    def __init__(self, func: Callable[Concatenate[ServiceT, Params], ReturnT]):
        self.func = func
        self.endpoint_functions = []

    def endpoint(
        self,
    ) -> Callable[[EndpointFunc], EndpointFunc]:
        def decorator(
            func: EndpointFunc,
        ) -> EndpointFunc:
            self.endpoint_functions.append(func)
            return func

        return decorator

    def client(
        self,
    ) -> Callable[[ClientFunc[ReturnT]], ClientFunc[ReturnT]]:
        def decorator(
            func: ClientFunc[ReturnT],
        ) -> ClientFunc[ReturnT]:
            self.client_function = func
            return func

        return decorator

    def __get__(
        self, obj: ServiceT, cls: type[ServiceT] | None = None
    ) -> Callable[Params, ReturnT]:
        bound_func = functools.partial(self.func, obj)
        return bound_func

    def __set_name__(self, owner: type[Any], name: str) -> None:
        if not issubclass(owner, AbstractService):
            raise ProgrammingError(
                f"`ServiceProcedure` cannot be a method of `{owner.__name__}`."
            )

        self.service_class = owner


def procedure(
    cls: type[ServiceProcedure[Any, Any, Any]] = ServiceProcedure,
    **kwargs: Any,
) -> Callable[
    [Callable[Concatenate[ServiceT, Params], ReturnT]],
    ServiceProcedure[ServiceT, Params, ReturnT],
]:
    def decorator(
        func: Callable[Concatenate[ServiceT, Params], ReturnT],
    ) -> ServiceProcedure[ServiceT, Params, ReturnT]:
        return cls(func)

    return decorator
