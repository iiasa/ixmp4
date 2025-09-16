import abc
import functools
import inspect
from typing import Any, Callable, Concatenate, Generic, ParamSpec, TypeVar

import fastapi as fa
import httpx
from pydantic import create_model
from sqlalchemy import orm
from toolkit.auth import AuthorizationContext
from toolkit.exceptions import ProgrammingError


class AbstractTransport(abc.ABC):
    pass


class DirectTransport(AbstractTransport):
    session: orm.Session
    auth_ctx: AuthorizationContext

    def __init__(self, session: orm.Session, auth_ctx: AuthorizationContext):
        self.session = session
        self.auth_ctx = auth_ctx


class HttpxTransport(AbstractTransport):
    client: httpx.Client

    def __init__(self, client: httpx.Client):
        self.client = client


TransportT = TypeVar("TransportT", bound=AbstractTransport)


class AbstractService(abc.ABC):
    transport: AbstractTransport

    def __init__(self, transport: AbstractTransport):
        self.transport = transport

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
ClientFunc = Callable[Params, ReturnT]
ServiceT = TypeVar("ServiceT", bound=AbstractService)
ClientFuncGetter = Callable[[ServiceT, httpx.Client], ClientFunc[Params, ReturnT]]


class ServiceProcedure(Generic[ServiceT, Params, ReturnT]):
    func: Callable[Concatenate[ServiceT, Params], ReturnT]
    service_class: type[AbstractService]
    endpoint_functions: list[EndpointFunc]
    client_function_getters: dict[
        type[AbstractTransport], ClientFuncGetter[ServiceT, Params, ReturnT]
    ]

    def __init__(self, func: Callable[Concatenate[ServiceT, Params], ReturnT]):
        self.func = func
        self.endpoint_functions = []
        self.client_function_getters = {}

    def register_endpoint(self, router: fa.APIRouter) -> None:
        func_name = self.func.__name__
        cc_func_name = func_name.title().replace("_", "")
        sig = inspect.signature(self.func)
        fields = {}
        for name, param in sig.parameters.items():
            if param.annotation == inspect.Parameter.empty:
                raise ProgrammingError(
                    f"Paramater `{name}` of `{func_name}` requires a type annotation."
                )

            if param.default == inspect.Parameter.empty:
                fields[name] = param.annotation
            else:
                fields[name] = (param.annotation, param.default)

        PayloadModel = create_model(cc_func_name, **fields)

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
        self, for_transport: type[AbstractTransport] = HttpxTransport
    ) -> Callable[
        [ClientFuncGetter[ServiceT, Params, ReturnT]],
        ClientFuncGetter[ServiceT, Params, ReturnT],
    ]:
        def decorator(
            func: ClientFuncGetter[ServiceT, Params, ReturnT],
        ) -> ClientFuncGetter[ServiceT, Params, ReturnT]:
            self.client_function_getters[for_transport] = func
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
