import abc
import copy
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, ClassVar, Mapping, ParamSpec, Sequence, TypeVar

import pandas as pd
import pandera.pandas as pa
import sqlalchemy as sa
from litestar import Controller, Router
from litestar.di import Provide
from pandera.errors import SchemaError
from toolkit.auth.context import AuthorizationContext, PlatformProtocol
from typing_extensions import TypedDict

from ixmp4.base_exceptions import InvalidDataFrame, ProgrammingError
from ixmp4.conf.settings import ServerSettings
from ixmp4.data.base.dto import BaseModel
from ixmp4.transport import (
    AuthorizedTransport,
    DirectTransport,
    HttpxTransport,
    Transport,
)

if TYPE_CHECKING:
    pass

TransportT = TypeVar("TransportT", bound=Transport)
ReturnT = TypeVar("ReturnT")
Params = ParamSpec("Params")


class AuthKwargs(TypedDict):
    auth_ctx: AuthorizationContext | None
    platform: PlatformProtocol | None


class Service(abc.ABC):
    """Main data layer interface for a data type.

    .. code:: python

        from ixmp4.data.services import (
            GetByIdService,
            Http,
            procedure,
        )
        from .exceptions import ExampleError

        class ExampleService(Service):
            @procedure(Http(path="/", methods=("POST",)))
            def do_something(self):
                raise ExampleError("Can't do something, sorry.")

    To mark service methods as interface procedures that can
    be called directly or via the http api, use the
    :func:`~ixmp4.data.services.procedure.procedure` decorator.

    Services can then be instantiated with a :class:`ixmp4.transport.Transport`
    object:

    .. code:: python

        from ixmp4.transport import DirectTransport, HttpxTransport
        from .example import ExampleService

        direct = DirectTranport.from_dsn("sqlite://...")
        direct_svc = ExampleService(direct)

        http = HttpxTransport.from_url/from_asgi(...)
        http_svc = ExampleService(http)

        direct_svc.do_something()
        #> ExampleError

        http_svc.do_something()
        #> ExampleError
    """

    router_tags: ClassVar[Sequence[str]] = []
    router_prefix: ClassVar[str]
    # router: ClassVar[Router]
    transport: Transport
    http_controller: ClassVar[type[Controller] | None] = None

    default_filter: Mapping[str, Any] = {}

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

    def __init_direct__(self, transport: DirectTransport) -> None:
        pass

    def __init_httpx__(self, transport: HttpxTransport) -> None:
        pass

    def get_auth_kwargs(self, transport: DirectTransport) -> AuthKwargs:
        if isinstance(transport, AuthorizedTransport):
            return AuthKwargs(auth_ctx=transport.auth_ctx, platform=transport.platform)
        else:
            return AuthKwargs(auth_ctx=None, platform=None)

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

    def apply_filter_defaults(self, values: Mapping[str, Any]) -> dict[str, Any]:
        defaults = copy.deepcopy(dict(self.default_filter))
        return self.deep_update_dict(defaults, values)

    def deep_update_dict(
        self, d: dict[str, Any], o: Mapping[str, Any]
    ) -> dict[str, Any]:
        for key, value in o.items():
            if isinstance(value, dict):
                node = d.setdefault(key, {})
                self.deep_update_dict(node, value)
            else:
                d[key] = value
        return d

    @classmethod
    def get_router(cls, settings: ServerSettings) -> Router:
        from .procedure.descriptor import ProcedureDescriptor

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
            if isinstance(val, ProcedureDescriptor):
                try:
                    handler = val.procedure.handlers[cls]
                except KeyError:
                    handler = val.procedure.register_service(cls)
                router.register(handler)

        if cls.http_controller is not None:
            router.register(cls.http_controller)

        return router


class GetByIdService(Service):
    """Service interface for types that can be retrieved by numeric id.

    Implementations must provide :meth:`get_by_id` which returns a
    :class:`ixmp4.data.base.dto.BaseModel` instance for the given id.
    """

    @abc.abstractmethod
    def get_by_id(self, id: int) -> BaseModel:
        raise NotImplementedError
