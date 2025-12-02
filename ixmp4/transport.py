import abc
import logging
from concurrent.futures import ThreadPoolExecutor
from functools import lru_cache
from typing import Any

import httpx
import sqlalchemy as sa
from fastapi.testclient import TestClient
from sqlalchemy import orm
from toolkit.auth.context import AuthorizationContext
from toolkit.client.auth import Auth, ManagerAuth
from toolkit.client.base import ServiceClient
from toolkit.manager.models import Ixmp4Instance
from toolkit.utils import ttl_cache

from ixmp4.base_exceptions import ProgrammingError
from ixmp4.base_exceptions import registry as exception_registry
from ixmp4.conf import settings


class Transport(abc.ABC):
    pass


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

    def __init__(
        self,
        session: orm.Session,
    ):
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
    def create_postgresql_engine(cls, dsn: str) -> sa.Engine:
        return sa.create_engine(dsn, poolclass=sa.StaticPool, max_identifier_length=63)

    @classmethod
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

    def close(self):
        self.session.rollback()
        self.session.close()
        self.session.bind.engine.dispose()

    def __str__(self) -> str:
        return f"<{self.__class__.__name__} {self.get_engine_info()}>"


class AuthorizedTransport(DirectTransport):
    platform: Ixmp4Instance
    auth_ctx: AuthorizationContext

    def __init__(
        self,
        session: orm.Session,
        auth_ctx: AuthorizationContext,
        platform: Ixmp4Instance,
    ):
        super().__init__(
            session,
        )
        self.auth_ctx = auth_ctx
        self.platform = platform

    def __str__(self) -> str:
        return (
            f"<{self.__class__.__name__} {self.get_engine_info()} "
            f"user={self.auth_ctx.user} platform={self.platform.slug}>"
        )


class HttpxTransport(Transport, ServiceClient):
    http_client: httpx.Client | TestClient
    executor: ThreadPoolExecutor
    exception_registry = exception_registry
    direct: DirectTransport | None = None

    def __init__(
        self,
        client: httpx.Client | TestClient,
        max_concurrent_requests: int = settings.client_max_concurrent_requests,
        cache_ttl: int = 60 * 15,
        direct: DirectTransport | None = None,
    ):
        self.url = client.base_url
        self.executor = ThreadPoolExecutor(max_workers=max_concurrent_requests)
        self.http_client = client
        self.ttl_cache = ttl_cache(cache_ttl)
        self.direct = direct

    @classmethod
    def from_url(cls, url: str, auth: Auth | None) -> "HttpxTransport":
        timeout = httpx.Timeout(settings.client_timeout, connect=60.0)
        client = httpx.Client(
            base_url=url,
            timeout=timeout,
            http2=True,
            auth=auth,
        )
        return cls(client)

    @classmethod
    def from_direct(
        cls, direct: DirectTransport, raise_server_exceptions: bool = False
    ):
        # avoid circular import
        # this usually only used in testing contexts
        from ixmp4.server import get_app

        app = get_app(lambda: direct)
        client = TestClient(
            app=app,
            base_url="http://testserver/v1/direct/",
            raise_server_exceptions=raise_server_exceptions,
        )
        return cls(client, direct=direct)

    def __str__(self) -> str:
        if (
            isinstance(self.http_client.auth, ManagerAuth)
            and self.http_client.auth.access_token.user is not None
        ):
            user = self.http_client.auth.access_token.user
        else:
            user = None
        return f"<HttpxTransport base_url={self.http_client.base_url} user={user}>"
