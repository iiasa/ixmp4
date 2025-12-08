import abc
from datetime import datetime, timezone
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    ParamSpec,
    Sequence,
    TypeVar,
)

import pandas as pd
import pandera.pandas as pa
import sqlalchemy as sa
from pandera.errors import SchemaError
from starlette.applications import Starlette
from starlette.middleware import Middleware

from ixmp4.base_exceptions import InvalidDataFrame, ProgrammingError
from ixmp4.data.base.dto import BaseModel
from ixmp4.transport import (
    AuthorizedTransport,
    DirectTransport,
    HttpxTransport,
    Transport,
)

from .middleware import ServiceMiddleware

if TYPE_CHECKING:
    from .procedure import ServiceProcedure


TransportT = TypeVar("TransportT", bound=Transport)
ReturnT = TypeVar("ReturnT")
Params = ParamSpec("Params")


class Service(abc.ABC):
    router_tags: ClassVar[Sequence[str]] = []
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
            assert self.transport.session.bind is not None
            return self.transport.session.bind.engine.dialect
        else:
            raise ProgrammingError(
                f"{self.transport} is not a `DirectTransport` "
                "instance and thus does not hold database dialect information."
            )

    def get_datetime(self) -> datetime:
        return datetime.now(tz=timezone.utc)

    def get_username(self) -> str:
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

    @classmethod
    def get_v1_app(cls) -> Starlette:
        routes = [proc.get_endpoint().get_route() for proc in cls.collect_procedures()]

        return Starlette(
            middleware=[Middleware(ServiceMiddleware, cls)],
            routes=routes,
        )

    @classmethod
    def collect_procedures(cls) -> "list[ServiceProcedure[Any, Any, Any]]":
        from .procedure import ServiceProcedure

        procedures = []
        for attrname in dir(cls):
            val = getattr(cls, attrname)
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


class GetByIdService(Service):
    @abc.abstractmethod
    def get_by_id(self, id: int) -> BaseModel:
        raise NotImplementedError
