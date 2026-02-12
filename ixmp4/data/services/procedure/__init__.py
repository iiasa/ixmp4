import functools
import inspect
from typing import (
    Any,
    Callable,
    Concatenate,
    Generic,
    ParamSpec,
    TypeVar,
)

from litestar.handlers import HTTPRouteHandler

from ixmp4.base_exceptions import ProgrammingError
from ixmp4.transport import AuthorizedTransport

from ..base import Service
from .auth import ProcedureAuthCheck
from .client import ProcedureClient
from .descriptor import ProcedureDescriptor
from .endpoint import ProcedureHttpConfig as ProcedureHttpConfig
from .endpoint import ProcedureRouteHandler
from .pagination import ProcedurePagination

ReturnT = TypeVar("ReturnT")
Params = ParamSpec("Params")
ServiceT = TypeVar("ServiceT", bound="Service")

ProcedureFunc = Callable[Concatenate[ServiceT, Params], ReturnT]


class Procedure(Generic[ServiceT, Params, ReturnT]):
    """Represents a service procedure and its HTTP/transport adapters.

    A :class:`Procedure` wraps a service method, validates its signature,
    and provides adapters for direct invocation, http client calls, and
    registration of HTTP route handlers. It also manages authorization
    checks and pagination metadata attached to the procedure.
    """

    func: ProcedureFunc[ServiceT, Params, ReturnT]
    signature: inspect.Signature
    auth_check: ProcedureAuthCheck[ServiceT, Params]
    pagination: ProcedurePagination[ServiceT, Params, ReturnT]
    handlers: dict[type[Service], ProcedureRouteHandler[ServiceT, Params, ReturnT]]
    http_config: ProcedureHttpConfig

    def __init__(
        self,
        func: ProcedureFunc[ServiceT, Params, ReturnT],
        http_config: ProcedureHttpConfig,
    ):
        self.func = func
        self.signature = self.validate_signature(func)
        self.auth_check = ProcedureAuthCheck(self)
        self.pagination = ProcedurePagination(self)
        self.http_config = http_config
        self.handlers = {}

    def validate_signature(self, func: Callable[..., Any]) -> inspect.Signature:
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

    def validate_corresponding_parameter(
        self,
        index: int,
        name: str,
        param: inspect.Parameter,
        func: Callable[..., Any],
    ) -> bool:
        original_params = list(self.signature.parameters.items())
        try:
            coresp_name, coresp_param = original_params[index]
        except IndexError:
            raise ProgrammingError(
                f"Argument '{name}' for function `{func.__name__}` "
                f"is superfluous because `{self.func.__name__}` only "
                f"expects {len(original_params)} argument(s)."
            )

        if param.annotation != coresp_param.annotation:
            raise ProgrammingError(
                f"Annotation `{param.annotation}` of argument '{name}' "
                f"for function `{func.__name__}` does not match "
                f"corresponding annotation `{coresp_param.annotation}` for "
                f"'{coresp_name}' argument of `{self.func.__name__}`."
            )

        if param.default != coresp_param.default:
            raise ProgrammingError(
                f"Default `{param.default}` of keyword argument '{name}' "
                f"for function `{func.__name__}` does not match "
                f"corresponding default `{coresp_param.default}` for "
                f"'{coresp_name}' argument of `{self.func.__name__}`."
            )
        return True

    def set_route_handler(self, handler: HTTPRouteHandler) -> None:
        pass

    def get_authorized_callable(
        self, service: ServiceT, func: Callable[Params, Any]
    ) -> Callable[Params, Any]:
        if isinstance(service.transport, AuthorizedTransport):
            return self.auth_check.prepend_auth_check(
                service,
                service.transport.auth_ctx,
                service.transport.platform,
                func,
            )
        else:
            return func

    def get_direct_callable(self, service: ServiceT) -> Callable[Params, ReturnT]:
        bound_func = functools.partial(self.func, service)
        return self.get_authorized_callable(service, bound_func)

    def get_httpx_callable(self, service: ServiceT) -> Callable[Params, ReturnT]:
        handler = self.handlers[type(service)]
        return ProcedureClient(service, handler)

    def get_descriptor(self) -> ProcedureDescriptor[ServiceT, Params, ReturnT]:
        return ProcedureDescriptor(self)

    def register_service(
        self, svc_cls: type[ServiceT]
    ) -> ProcedureRouteHandler[ServiceT, Params, ReturnT]:
        handler = ProcedureRouteHandler(self, svc_cls, self.http_config)
        self.handlers[svc_cls] = handler
        return handler


def procedure(
    http_config: ProcedureHttpConfig,
) -> Callable[
    [Callable[Concatenate[ServiceT, Params], ReturnT]],
    ProcedureDescriptor[ServiceT, Params, ReturnT],
]:
    """Makes a service method callable directly or via http.
    Constructs an internal :class:`~ixmp4.data.services.procedure.Procedure`
    instance and

    Returns
    =======
    :class:`~ixmp4.data.services.procedure.descriptor.ProcedureDescriptor`
        A special descriptor class that provides procedure
        functionality on a service class.
    """

    def decorator(
        func: Callable[Concatenate[ServiceT, Params], ReturnT],
    ) -> ProcedureDescriptor[ServiceT, Params, ReturnT]:
        return Procedure(func, http_config).get_descriptor()

    return decorator
