import abc
import logging
from concurrent.futures import ThreadPoolExecutor
from functools import lru_cache
from typing import Any

import httpx
import sqlalchemy as sa
from litestar import Litestar
from litestar.testing import TestClient
from sqlalchemy import orm
from toolkit.auth.context import AuthorizationContext, PlatformProtocol
from toolkit.client.auth import Auth, ManagerAuth, SelfSignedAuth
from toolkit.client.base import ServiceClient

from ixmp4.base_exceptions import ImproperlyConfigured
from ixmp4.conf.settings import ClientSettings, Settings
from ixmp4.core.exceptions import OperationNotSupported, ProgrammingError
from ixmp4.core.exceptions import registry as exception_registry

from ._version import __version__


class Transport(abc.ABC):
    def check_versioning_compatiblity(self) -> None:
        raise NotImplementedError

    def __str__(self) -> str:
        return f"<{self.__class__.__name__}>"


logger = logging.getLogger(__name__)


@lru_cache()
def cached_create_engine(dsn: str, **kwargs: Any) -> sa.Engine:
    # max_identifier_length=63 to avoid exceeding postgres' default maximum
    return sa.create_engine(
        dsn, poolclass=sa.StaticPool, max_identifier_length=63, **kwargs
    )


Session = orm.sessionmaker(autocommit=False, autoflush=False)


class DirectTransport(Transport):
    session: orm.Session

    def __init__(self, session: orm.Session):
        self.session = session

    @classmethod
    def check_dsn(cls, dsn: str) -> str:
        if dsn.startswith("postgresql://"):
            logger.debug(
                "Replacing the platform dsn prefix to use the new `psycopg` driver."
            )
            dsn = dsn.replace("postgresql://", "postgresql+psycopg://")
        return dsn

    @classmethod
    def from_dsn(cls, dsn: str, *args: Any, **kwargs: Any) -> "DirectTransport":
        dsn = cls.check_dsn(dsn)
        if dsn.startswith("sqlite"):
            engine = cls.create_sqlite_engine(dsn)
        elif dsn.startswith("postgresql"):
            engine = cls.create_postgresql_engine(dsn)
        else:
            raise ProgrammingError("Unsupported database dialect for DSN: " + dsn)
        session = Session(bind=engine)
        return cls(session, *args, **kwargs)

    @classmethod
    @lru_cache()
    def create_postgresql_engine(cls, dsn: str) -> sa.Engine:
        return sa.create_engine(dsn, poolclass=sa.StaticPool, max_identifier_length=63)

    @classmethod
    @lru_cache()
    def create_sqlite_engine(cls, dsn: str) -> sa.Engine:
        return sa.create_engine(
            dsn,
            poolclass=sa.StaticPool,
            max_identifier_length=63,
            connect_args={"check_same_thread": False},
        )

    def get_engine_info(self) -> str:
        if self.session.bind is None:
            return ""
        else:
            dialect = self.session.bind.engine.dialect.name
            host = self.session.bind.engine.url.host
            database = self.session.bind.engine.url.database
            return f"dialect={dialect} database={database} host={host}"

    def close(self) -> None:
        self.session.rollback()
        self.session.close()
        assert self.session.bind is not None
        self.session.bind.engine.dispose()

    def check_versioning_compatiblity(self) -> None:
        assert self.session.bind is not None
        if self.session.bind.engine.dialect.name != "postgresql":
            raise OperationNotSupported(
                "Versioning is only enabled on 'postgresql' platforms..."
            )

    def __str__(self) -> str:
        return f"<{self.__class__.__name__} {self.get_engine_info()}>"


class AuthorizedTransport(DirectTransport):
    platform: PlatformProtocol
    auth_ctx: AuthorizationContext
    unauthorized_transport: DirectTransport

    def __init__(
        self,
        session: orm.Session,
        auth_ctx: AuthorizationContext,
        platform: PlatformProtocol,
    ):
        super().__init__(
            session,
        )
        self.auth_ctx = auth_ctx
        self.platform = platform
        self.unauthorized_transport = DirectTransport(session)

    def __str__(self) -> str:
        return (
            f"<{self.__class__.__name__} {self.get_engine_info()} "
            f"user={self.auth_ctx.user} platform={self.platform.id}>"
        )


class HttpxTransport(Transport, ServiceClient):
    http_client: httpx.Client | TestClient[Litestar]
    settings: ClientSettings
    executor: ThreadPoolExecutor
    exception_registry = exception_registry
    direct: DirectTransport | None = None

    def __init__(
        self,
        client: httpx.Client | TestClient[Litestar],
        settings: ClientSettings,
        check_root: bool = True,
    ):
        self.url = str(client.base_url)
        logger.debug(f"Connecting to IXMP4 http server at '{self.url}'.")

        self.settings = settings
        self.executor = ThreadPoolExecutor(max_workers=settings.max_concurrent_requests)
        self.http_client = client

        if check_root:
            self.check_root()

    def check_root(self) -> None:
        """Requests root api endpoint and logs messages."""
        from ixmp4.server.v1.platform import PlatformInfo

        res = self.http_client.get("/")
        self.raise_service_exception(res)
        root = PlatformInfo(**res.json())

        if __version__ != root.version:
            logger.warning(
                "IXMP4 Client and Server versions do not match. "
                f"(Client: {__version__}, Server: {root.version})"
            )

        logger.debug("Server UTC Time: " + root.utcnow.strftime("%c"))

        if (
            isinstance(self.http_client.auth, ManagerAuth)
            and root.manager_url is not None
        ):
            client_manager_url = str(self.http_client.auth.client.base_url)
            if client_manager_url.rstrip("/") != root.manager_url.rstrip("/"):
                logger.error(f"Server Manager URL: {root.manager_url}")
                logger.error(f"Client Manager URL: {root.manager_url}")
                raise ImproperlyConfigured(
                    "Trying to connect to a managed http Platform "
                    "with a mismatching Manager URL."
                )

    @classmethod
    def from_url(
        cls, url: str, settings: ClientSettings | None = None, auth: Auth | None = None
    ) -> "HttpxTransport":
        if settings is None:
            settings = Settings().client

        timeout = httpx.Timeout(settings.timeout, connect=10.0)

        if settings.secret_hs256 is not None and auth is None:
            logger.info(
                "Found `secret_hs256` in client settings, using self-signed auth."
            )
            auth = SelfSignedAuth(
                settings.secret_hs256.get_secret_value(), issuer="ixmp4"
            )

        client = httpx.Client(
            base_url=url,
            timeout=timeout,
            http2=True,
            auth=auth,
        )
        return cls(client, settings)

    @classmethod
    def from_asgi(
        cls,
        asgi: Litestar,
        settings: ClientSettings,
        direct: DirectTransport | None = None,
        raise_server_exceptions: bool = True,
    ) -> "HttpxTransport":
        client = TestClient(
            app=asgi,
            base_url="http://testserver.local/v1/direct/",
            raise_server_exceptions=raise_server_exceptions,
        )
        transport = cls(client, settings, check_root=False)
        transport.direct = direct
        return transport

    def __str__(self) -> str:
        if (
            isinstance(self.http_client.auth, ManagerAuth)
            and self.http_client.auth.access_token.user is not None
        ):
            user = self.http_client.auth.access_token.user
        else:
            user = None
        return f"<HttpxTransport base_url={self.http_client.base_url} user={user}>"
