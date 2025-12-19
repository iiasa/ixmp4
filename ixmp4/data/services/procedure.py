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


class InternalProcedureAuthCheckFunc(Protocol[ContraServiceT]):
    __name__: str

    def __call__(
        self,
        svc: ContraServiceT,
        auth_ctx: AuthorizationContext,
        platform: PlatformProtocol,
        *args: Any,
        **kwds: Any,
    ) -> Any: ...


class ProcedureAuthCheckFuncWithParams(Protocol[ContraServiceT, Params]):
    __name__: str

    def __call__(
        self,
        svc: ContraServiceT,
        auth_ctx: AuthorizationContext,
        platform: PlatformProtocol,
        /,
        *args: Params.args,
        **kwds: Params.kwargs,
    ) -> Any: ...


class ProcedureAuthCheckFuncNoParams(Protocol[ContraServiceT]):
    __name__: str

    def __call__(
        self,
        svc: ContraServiceT,
        auth_ctx: AuthorizationContext,
        platform: PlatformProtocol,
        /,
    ) -> Any: ...


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
    endpoints: dict[type[Service], HttpProcedureEndpoint[ServiceT, Params, ReturnT]]

    def __init__(
        self,
        func: ProcedureFunc[ServiceT, Params, ReturnT],
        http_config: HttpConfig | None = None,
    ):
        self.func = func
        self.signature = self.parse_signature(self.func)
        self.http_config = http_config or HttpConfig()
        self.endpoints = {}

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
                    f"Procedure `{func.__name__}` has positional-only arguments, "
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

    def parse_auth_check_signature(self, func: Callable[..., Any]) -> inspect.Signature:
        org_sig = inspect.signature(func)
        valid_params = []
        param_dict = org_sig.parameters.items()

        for index, (name, param) in enumerate(param_dict):
            if name == "self":
                continue  # skip self parameter as it will not be bound yet

            if param.annotation == inspect.Parameter.empty:
                raise ProgrammingError(
                    f"Argument `{name}` of `{func.__name__}` requires "
                    "a type annotation."
                )

            if param.kind == inspect.Parameter.POSITIONAL_ONLY:
                if index == 1 and param.annotation is AuthorizationContext:
                    pass
                elif index == 2 and param.annotation is PlatformProtocol:
                    pass
                else:
                    raise ProgrammingError(
                        f"Unexpected positional-only argument '{name}' with annotation "
                        f"`{param.annotation}` in function definiton for "
                        f"`{func.__name__}`."
                    )

            original_params = list(self.signature.parameters.items())
            if index > 2:
                try:
                    coresp_name, coresp_param = original_params[index - 3]
                except IndexError:
                    raise ProgrammingError(
                        f"Argument '{name}' for auth check `{func.__name__}` "
                        f"is superfluous because `{self.func.__name__}` only "
                        f"expects {len(original_params)} argument(s)."
                    )

                if param.annotation != coresp_param.annotation:
                    raise ProgrammingError(
                        f"Annotation `{param.annotation}` of argument '{name}' "
                        f"for auth check `{func.__name__}` does not match "
                        f"corresponding annotation `{coresp_param.annotation}` for "
                        f"'{coresp_name}' argument of `{self.func.__name__}`."
                    )

                if param.default != coresp_param.default:
                    raise ProgrammingError(
                        f"Default `{param.default}` of keyword argument '{name}' "
                        f"for auth check `{func.__name__}` does not match "
                        f"corresponding default `{coresp_param.default}` for "
                        f"'{coresp_name}' argument of `{self.func.__name__}`."
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
            decorated_auth_check: ProcedureAuthCheckFunc[ServiceT, Params],
        ) -> ProcedureAuthCheckFunc[ServiceT, Params]:
            auth_check_sig = self.parse_auth_check_signature(decorated_auth_check)

            auth_check_func: InternalProcedureAuthCheckFunc[ServiceT]

            if len(auth_check_sig.parameters) <= 2:
                wrapped = cast(  # type: ignore[redundant-cast]
                    ProcedureAuthCheckFuncNoParams[ServiceT], decorated_auth_check
                )

                @functools.wraps(wrapped)
                def auth_check_wrapper(
                    service_self: ServiceT,
                    auth_ctx: AuthorizationContext,
                    platform: PlatformProtocol,
                    *arg: Any,
                    **kwargs: Any,
                ) -> Any:
                    return wrapped(service_self, auth_ctx, platform)

                auth_check_func = cast(
                    InternalProcedureAuthCheckFunc[ServiceT],
                    auth_check_wrapper,
                )

            else:
                decorated_auth_check = cast(
                    InternalProcedureAuthCheckFunc[ServiceT],
                    decorated_auth_check,
                )
                auth_check_func = decorated_auth_check

            self.auth_check_func = auth_check_func
            self.has_auth_check = True
            return decorated_auth_check

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
        return ServiceProcedureClient(self.endpoints[type(service)], service)

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
