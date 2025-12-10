import abc
import inspect
from datetime import datetime, timezone
from typing import Any, ClassVar, ParamSpec, Sequence, TypeVar

import pandas as pd
import pandera.pandas as pa
import sqlalchemy as sa
from litestar import Router, route
from litestar.di import Provide
from pandera.errors import SchemaError

from ixmp4.base_exceptions import InvalidDataFrame, ProgrammingError
from ixmp4.data.base.dto import BaseModel
from ixmp4.services.http import HttpProcedureEndpoint
from ixmp4.transport import (
    AuthorizedTransport,
    DirectTransport,
    HttpxTransport,
    Transport,
)

TransportT = TypeVar("TransportT", bound=Transport)
ReturnT = TypeVar("ReturnT")
Params = ParamSpec("Params")


class Service(abc.ABC):
    router_tags: ClassVar[Sequence[str]] = []
    router_prefix: ClassVar[str]
    router: ClassVar[Router]
    transport: Transport

    def __init__(self, transport: Transport):
        self.transport = transport
        if isinstance(transport, DirectTransport):
            self.__init_direct__(transport)
        elif isinstance(transport, HttpxTransport):
            self.__init_httpx__(transport)

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        if getattr(cls, "__abstract__", False):
            setattr(cls, "__abstract__", False)
            return
        if not inspect.isabstract(cls):
            cls.router = cls.get_router()

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

    def validate_df_or_raise(
        self, df: pd.DataFrame, model: type[pa.DataFrameModel]
    ) -> pd.DataFrame:
        try:
            return model.validate(df)
        except SchemaError as e:
            raise InvalidDataFrame(str(e))

    @classmethod
    def get_router(cls) -> Router:
        from ixmp4.services.procedure import ServiceProcedure

        async def service_dep(transport: DirectTransport) -> Service:
            return cls(transport)

        router = Router(
            cls.router_prefix,
            route_handlers=[],
            dependencies={"service": Provide(service_dep)},
            tags=cls.router_tags,
        )

        for attrname in dir(cls):
            val = getattr(cls, attrname, None)
            if isinstance(val, ServiceProcedure):
                val.endpoint = val.get_endpoint(cls)
                proc_route = cls.get_procedure_route(val.endpoint)
                routes = router.register(proc_route)
                val.endpoint.routes = routes

        return router

    @classmethod
    def get_procedure_route(
        cls,
        endpoint: HttpProcedureEndpoint[Any, Any, Any],
    ) -> route:
        handler = route(
            endpoint.path,
            http_method=endpoint.methods,
            status_code=200,
            name=endpoint.name,
            operation_id=endpoint.name,
            description=endpoint.procedure.func.__doc__,
            summary=endpoint.name,
        )
        return handler(endpoint.handle_request)


class GetByIdService(Service):
    @abc.abstractmethod
    def get_by_id(self, id: int) -> BaseModel:
        raise NotImplementedError
