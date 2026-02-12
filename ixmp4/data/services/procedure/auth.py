import functools
import inspect
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Generic,
    ParamSpec,
    Protocol,
    TypeVar,
    cast,
)

from toolkit.auth.context import AuthorizationContext, PlatformProtocol

from ixmp4.base_exceptions import ProgrammingError

if TYPE_CHECKING:
    from ..base import Service
    from . import Procedure

ReturnT = TypeVar("ReturnT")
Params = ParamSpec("Params")
ServiceT = TypeVar("ServiceT", bound="Service")


ContraServiceT = TypeVar("ContraServiceT", bound="Service", contravariant=True)


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


class ProcedureAuthCheck(Generic[ServiceT, Params]):
    input_func: ProcedureAuthCheckFunc[ServiceT, Params]
    check_func: InternalProcedureAuthCheckFunc[ServiceT]
    has_check: bool
    signature: inspect.Signature
    procedure: "Procedure[ServiceT, Params, Any]"

    def __init__(self, procedure: "Procedure[ServiceT, Params, Any]"):
        self.procedure = procedure
        self.has_check = False

    """Holder for an optional authorization check for a Procedure.

    Use as a decorator to register a function that receives the
    :class:`toolkit.auth.context.AuthorizationContext` and
    :class:`toolkit.auth.context.PlatformProtocol` before a procedure
    is executed. The registered function is validated against the
    procedure signature and can be prepended to the procedure callable
    to enforce access control.
    """

    def __call__(
        self,
    ) -> Callable[
        [ProcedureAuthCheckFunc[ServiceT, Params]],
        ProcedureAuthCheckFunc[ServiceT, Params],
    ]:
        return self.decorator

    def decorator(
        self, func: ProcedureAuthCheckFunc[ServiceT, Params]
    ) -> ProcedureAuthCheckFunc[ServiceT, Params]:
        self.signature = self.validate_signature(func)

        if len(self.signature.parameters) <= 2:
            wrapped = cast(  # type: ignore[redundant-cast]
                ProcedureAuthCheckFuncNoParams[ServiceT], func
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

            self.check_func = cast(
                InternalProcedureAuthCheckFunc[ServiceT],
                auth_check_wrapper,
            )
        else:
            self.check_func = cast(InternalProcedureAuthCheckFunc[ServiceT], func)

        self.has_check = True

        return func

    def validate_signature(self, func: Callable[..., Any]) -> inspect.Signature:
        org_sig = inspect.signature(func)
        valid_params = []
        param_dict = org_sig.parameters.items()

        for index, (name, param) in enumerate(param_dict):
            if self.validate_parameter(index, name, param, func):
                valid_params.append(param)

        return inspect.Signature(
            valid_params, return_annotation=org_sig.return_annotation
        )

    def validate_parameter(
        self, index: int, name: str, param: inspect.Parameter, func: Callable[..., Any]
    ) -> bool:
        if name == "self":
            return False  # skip self parameter as it will not be bound yet

        if index == 1:
            if param.annotation is not AuthorizationContext:
                raise ProgrammingError(
                    f"Unexpected positional-only argument '{name}' with annotation "
                    f"`{param.annotation}` in function definiton for `{func.__name__}`,"
                    f" expected argument of type `AuthorizationContext`."
                )

        if index == 2:
            if param.annotation is not PlatformProtocol:
                raise ProgrammingError(
                    f"Unexpected positional-only argument '{name}' with annotation "
                    f"`{param.annotation}` in function definiton for `{func.__name__}`,"
                    f" expected argument of type `PlatformProtocol`."
                )

        if index > 2:
            self.procedure.validate_corresponding_parameter(
                index - 3, name, param, func
            )

        return True

    def prepend_auth_check(
        self,
        service: ServiceT,
        auth_ctx: AuthorizationContext,
        platform: PlatformProtocol,
        procedure_func: Callable[Params, Any],
    ) -> Callable[Params, Any]:
        if self.has_check:

            @functools.wraps(procedure_func)
            def auth_wrapper(*args: Params.args, **kwargs: Params.kwargs) -> Any:
                self.check_func(service, auth_ctx, platform, *args, **kwargs)
                return procedure_func(*args, **kwargs)

            return auth_wrapper
        else:
            return procedure_func
