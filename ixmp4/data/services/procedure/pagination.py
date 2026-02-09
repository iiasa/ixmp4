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

from ixmp4.base_exceptions import ProgrammingError
from ixmp4.data.pagination import PaginatedResult, Pagination

if TYPE_CHECKING:
    from ..base import Service
    from . import Procedure

ReturnT = TypeVar("ReturnT")
CoReturnT = TypeVar("CoReturnT", covariant=True)
Params = ParamSpec("Params")
ServiceT = TypeVar("ServiceT", bound="Service")
ContraServiceT = TypeVar("ContraServiceT", bound="Service", contravariant=True)


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


class BoundProcedurePaginatedFunc(Protocol[Params, CoReturnT]):
    __name__: str

    def __call__(
        self,
        pagination: Pagination,
        /,
        *args: Params.args,
        **kwds: Params.kwargs,
    ) -> CoReturnT: ...


class ProcedurePagination(Generic[ServiceT, Params, ReturnT]):
    paginated_func: ProcedurePaginatedFunc[ServiceT, Params, PaginatedResult[ReturnT]]
    procedure: "Procedure[ServiceT, Params, Any]"
    has_pagination: bool

    def __init__(self, procedure: "Procedure[ServiceT, Params, Any]"):
        self.procedure = procedure
        self.has_pagination = False

    def __call__(
        self,
    ) -> Callable[
        [ProcedurePaginatedFunc[ServiceT, Params, PaginatedResult[ReturnT]]],
        BoundProcedurePaginatedFunc[Params, PaginatedResult[ReturnT]],
    ]:
        return self.decorator

    def decorator(
        self,
        func: ProcedurePaginatedFunc[ServiceT, Params, PaginatedResult[ReturnT]],
    ) -> BoundProcedurePaginatedFunc[Params, PaginatedResult[ReturnT]]:
        self.signature = self.validate_signature(func)
        self.paginated_func = func
        self.has_pagination = True

        return cast(BoundProcedurePaginatedFunc[Params, PaginatedResult[ReturnT]], func)

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
            if param.annotation is not Pagination:
                raise ProgrammingError(
                    f"Unexpected positional-only argument '{name}' with annotation "
                    f"`{param.annotation}` in function definiton for `{func.__name__}`,"
                    f" expected argument of type `AuthorizationContext`."
                )

        if index > 1:
            self.procedure.validate_corresponding_parameter(
                index - 2, name, param, func
            )

        return True
