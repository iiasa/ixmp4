import abc
import functools
from datetime import datetime, timezone
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    ClassVar,
    Concatenate,
    ParamSpec,
    TypeVar,
)

import fastapi as fa
import pandas as pd
import pandera.pandas as pa
import sqlalchemy as sa
from pandera.errors import SchemaError
from toolkit.auth.context import AuthorizationContext
from toolkit.manager.models import Ixmp4Instance

from ixmp4.rewrite.exceptions import InvalidDataFrame, ProgrammingError
from ixmp4.rewrite.transport import (
    AuthorizedTransport,
    DirectTransport,
    HttpxTransport,
    Transport,
)

if TYPE_CHECKING:
    from .procedures import ServiceProcedure


TransportT = TypeVar("TransportT", bound=Transport)
ReturnT = TypeVar("ReturnT")
Params = ParamSpec("Params")


class Service(abc.ABC):
    router_tags: ClassVar[list[str]] = []
    router_prefix: ClassVar[str]
    transport: Transport

    def __init__(self, transport: Transport):
        self.transport = transport
        if isinstance(transport, DirectTransport):
            self.__init_direct__(transport)
        elif isinstance(transport, HttpxTransport):
            self.__init_httpx__(transport)

    def __init_direct__(self, transport: DirectTransport) -> None:
        pass

    def __init_httpx__(self, transport: HttpxTransport) -> None:
        pass

    def get_dialect(self) -> sa.Dialect:
        if isinstance(self.transport, DirectTransport):
            return self.transport.session.bind.engine.dialect
        else:
            raise ProgrammingError(
                f"{self.transport} is not a `DirectTransport` "
                "instance and thus does not hold database dialect information."
            )

    def get_datetime(self) -> datetime:
        return datetime.now(tz=timezone.utc)

    def get_username(self):
        if isinstance(self.transport, AuthorizedTransport):
            user = self.transport.auth_ctx.user
            if user is None:
                username = "@anonymous"
            else:
                username = user.username

        else:
            username = "@unknown"
        return username

    def get_creation_info(self) -> dict[str, str | datetime]:
        return {
            "created_by": self.get_username(),
            "created_at": self.get_datetime(),
        }

    def get_update_info(self) -> dict[str, str | datetime]:
        return {
            "updated_by": self.get_username(),
            "updated_at": self.get_datetime(),
        }

    def bind_service_func(
        self,
        func: Callable[Concatenate["Service", Params], ReturnT],
        auth_check_func: Callable[
            Concatenate["Service", AuthorizationContext, Ixmp4Instance, Params], Any
        ]
        | None,
    ) -> Callable[Params, ReturnT]:
        bound_func = functools.partial(func, self)
        transport = self.transport

        if isinstance(transport, AuthorizedTransport) and auth_check_func is not None:

            @functools.wraps(bound_func)
            def auth_wrapper(*args: Params.args, **kwargs: Params.kwargs) -> ReturnT:
                auth_check_func(
                    self,
                    transport.auth_ctx,
                    transport.platform,
                    *args,
                    **kwargs,
                )
                return bound_func(*args, **kwargs)

            return auth_wrapper
        else:
            return bound_func

    @classmethod
    def build_router(
        cls,
        transport_dep: Callable[..., Any],
    ) -> fa.APIRouter:
        def svc_dep(
            transport: DirectTransport = fa.Depends(transport_dep),
        ) -> Service:
            return cls(transport)

        router = fa.APIRouter(prefix=cls.router_prefix, tags=[cls.router_tags])
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

    def validate_df_or_raise(
        self, df: pd.DataFrame, model: type[pa.DataFrameModel]
    ) -> pd.DataFrame:
        try:
            return model.validate(df)
        except SchemaError as e:
            raise InvalidDataFrame(str(e))
