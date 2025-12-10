import functools
import inspect
from typing import (
    Any,
    Callable,
    Concatenate,
    Generic,
    ParamSpec,
    Protocol,
    TypeVar,
    cast,
    overload,
)

from toolkit.auth.context import AuthorizationContext, PlatformProtocol

from ixmp4.base_exceptions import ProgrammingError
from ixmp4.data.pagination import PaginatedResult, Pagination
from ixmp4.transport import AuthorizedTransport, DirectTransport, HttpxTransport

from .base import Service
from .http import HttpConfig, HttpProcedureEndpoint, ServiceProcedureClient

ReturnT = TypeVar("ReturnT")
CoReturnT = TypeVar("CoReturnT", covariant=True)
Params = ParamSpec("Params")
ServiceT = TypeVar("ServiceT", bound="Service")
ContraServiceT = TypeVar("ContraServiceT", bound="Service", contravariant=True)

ProcedureFunc = Callable[Concatenate[ServiceT, Params], ReturnT]

ProcedureAuthCheckFuncWithParams = Callable[
    Concatenate[ServiceT, AuthorizationContext, PlatformProtocol, Params], Any
]
ProcedureAuthCheckFuncNoParams = Callable[
    Concatenate[ServiceT, AuthorizationContext, PlatformProtocol, ...], Any
]
ProcedureAuthCheckFunc = (
    ProcedureAuthCheckFuncNoParams[ServiceT]
    | ProcedureAuthCheckFuncWithParams[ServiceT, Params]
)

AnyFuncArgs = tuple[tuple[Any, ...], dict[str, Any]]
FuncParams = ParamSpec("FuncParams")
FuncReturnT = TypeVar("FuncReturnT")


class ProcedurePaginatedFunc(Protocol[ContraServiceT, Params, CoReturnT]):
    __name__: str

    def __call__(
        self,
        svc: ContraServiceT,
        pagination: Pagination,
        /,
        *args: Params.args,
        **kwds: Params.kwargs,
    ) -> CoReturnT: ...


class ServiceProcedure(Generic[ServiceT, Params, ReturnT]):
    func: ProcedureFunc[ServiceT, Params, ReturnT]
    signature: inspect.Signature

    has_auth_check = False
    auth_check_func: ProcedureAuthCheckFunc[ServiceT, Params]
    has_paginated_func = False
    paginated_func: ProcedurePaginatedFunc[ServiceT, Params, PaginatedResult[ReturnT]]
    paginated_signature: inspect.Signature
    http_config: HttpConfig
    endpoint: HttpProcedureEndpoint[ServiceT, Params, ReturnT]

    def __init__(
        self,
        func: ProcedureFunc[ServiceT, Params, ReturnT],
        http_config: HttpConfig | None = None,
    ):
        self.func = func
        self.signature = self.parse_signature(self.func)
        self.http_config = http_config or HttpConfig()

    # provides type hints for service methods
    def __call__(self, *args: Params.args, **kwds: Params.kwargs) -> ReturnT:
        raise ProgrammingError("`ServiceProcedure` cannot be called directly.")

    def parse_signature(self, func: Callable[..., Any]) -> inspect.Signature:
        org_sig = inspect.signature(func)
        valid_params = []
        param_dict = org_sig.parameters.items()

        for name, param in param_dict:
            if name == "self":
                continue  # skip self parameter as it will not be bound yet
            if param.kind == inspect.Parameter.POSITIONAL_ONLY:
                raise ProgrammingError(
                    f"`{func.__name__}` has positional-only arguments, "
                    "which is not supported."
                )

            if param.annotation == inspect.Parameter.empty:
                raise ProgrammingError(
                    f"Parameter `{name}` of `{func.__name__}` requires "
                    "a type annotation."
                )

            valid_params.append(param)

        return inspect.Signature(
            valid_params, return_annotation=org_sig.return_annotation
        )

    def auth_check(
        self,
    ) -> Callable[
        [ProcedureAuthCheckFunc[ServiceT, Params]],
        ProcedureAuthCheckFunc[ServiceT, Params],
    ]:
        def decorator(
            auth_check: ProcedureAuthCheckFunc[ServiceT, Params],
        ) -> ProcedureAuthCheckFunc[ServiceT, Params]:
            self.auth_check_func = auth_check
            self.has_auth_check = True
            return auth_check

        return decorator

    def paginated(
        self,
    ) -> Callable[
        [ProcedurePaginatedFunc[ServiceT, Params, PaginatedResult[ReturnT]]],
        ProcedurePaginatedFunc[ServiceT, Params, PaginatedResult[ReturnT]],
    ]:
        def decorator(
            paginated_func: ProcedurePaginatedFunc[
                ServiceT, Params, PaginatedResult[ReturnT]
            ],
        ) -> ProcedurePaginatedFunc[ServiceT, Params, PaginatedResult[ReturnT]]:
            self.paginated_func = paginated_func
            self.paginated_signature = self.parse_signature(paginated_func)
            self.has_paginated_func = True
            return paginated_func

        return decorator

    @overload
    def __get__(
        self, obj: Any, cls: type[Any] | None = None
    ) -> "ServiceProcedure[ServiceT, Params, ReturnT]": ...

    @overload
    def __get__(
        self, obj: ServiceT, cls: type[ServiceT] | None = None
    ) -> Callable[Params, ReturnT]: ...

    def __get__(
        self, obj: Any, cls: type[Any] | None = None
    ) -> "Callable[Params, ReturnT] | ServiceProcedure[ServiceT, Params, ReturnT]":
        if isinstance(obj, Service):
            return self.connect_service(cast(ServiceT, obj))
        else:
            return self

    def connect_service(self, service: ServiceT) -> Callable[Params, ReturnT]:
        if isinstance(service.transport, DirectTransport):
            return self.connect_direct_service(service)
        elif isinstance(service.transport, HttpxTransport):
            return self.connect_httpx_service(service)
        else:
            raise ProgrammingError(
                f"Transport class `{service.transport.__class__.__name__}` "
                "is not supported."
            )

    def maybe_add_auth_check(
        self, service: ServiceT, func: Callable[Params, Any]
    ) -> Callable[Params, Any]:
        transport = service.transport
        if self.has_auth_check and isinstance(transport, AuthorizedTransport):

            @functools.wraps(func)
            def auth_wrapper(*args: Params.args, **kwargs: Params.kwargs) -> Any:
                self.auth_check_func(
                    service,
                    transport.auth_ctx,
                    transport.platform,
                    *args,
                    **kwargs,
                )
                return func(*args, **kwargs)

            return auth_wrapper
        else:
            return func

    def connect_direct_service(self, service: ServiceT) -> Callable[Params, ReturnT]:
        bound_func = functools.partial(self.func, service)
        return self.maybe_add_auth_check(service, bound_func)

    def connect_httpx_service(self, service: ServiceT) -> Callable[Params, ReturnT]:
        return ServiceProcedureClient(self.endpoint, service)

    def get_endpoint(
        self, service_class: type["Service"]
    ) -> HttpProcedureEndpoint[ServiceT, Params, ReturnT]:
        return HttpProcedureEndpoint(self, service_class, self.http_config)


def procedure(
    http: HttpConfig,
) -> Callable[
    [Callable[Concatenate[ServiceT, Params], ReturnT]],
    ServiceProcedure[ServiceT, Params, ReturnT],
]:
    def decorator(
        func: Callable[Concatenate[ServiceT, Params], ReturnT],
    ) -> ServiceProcedure[ServiceT, Params, ReturnT]:
        return ServiceProcedure(func, http)

    return decorator
