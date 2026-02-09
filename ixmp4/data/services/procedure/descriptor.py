from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Generic,
    ParamSpec,
    TypeVar,
)

from ixmp4.base_exceptions import ProgrammingError
from ixmp4.transport import DirectTransport, HttpxTransport

from ..base import Service
from .auth import ProcedureAuthCheck
from .pagination import ProcedurePagination

if TYPE_CHECKING:
    from . import Procedure


ReturnT = TypeVar("ReturnT")
Params = ParamSpec("Params")
ServiceT = TypeVar("ServiceT", bound="Service")


class ProcedureDescriptor(Generic[ServiceT, Params, ReturnT]):
    procedure: "Procedure[ServiceT, Params, ReturnT]"

    @property
    def auth_check(self) -> ProcedureAuthCheck[ServiceT, Params]:
        return self.procedure.auth_check

    @property
    def paginated(self) -> ProcedurePagination[ServiceT, Params, ReturnT]:
        return self.procedure.pagination

    def __init__(self, procedure: "Procedure[ServiceT, Params, ReturnT]"):
        self.procedure = procedure

    # provides type hints for service methods
    def __call__(self, *args: Params.args, **kwds: Params.kwargs) -> ReturnT:
        raise ProgrammingError("`ServiceProcedure` cannot be called directly.")

    def __get__(
        self, obj: ServiceT | None, cls: type[Any] | None = None
    ) -> "Callable[Params, ReturnT]":
        if obj is None:
            return self

        if not isinstance(obj, Service):
            raise ProgrammingError(
                f"`{self.__class__.__name__}` must be used "
                "as a descriptor for `Service` classes."
            )

        if isinstance(obj.transport, DirectTransport):
            return self.procedure.get_direct_callable(obj)
        elif isinstance(obj.transport, HttpxTransport):
            return self.procedure.get_httpx_callable(obj)
        else:
            raise ProgrammingError(
                f"Transport class `{obj.transport.__class__.__name__}` "
                "is not supported."
            )

    def __set_name__(self, owner: type[ServiceT], name: str) -> None:
        self.procedure.register_service(owner)
