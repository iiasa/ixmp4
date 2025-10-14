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
from toolkit.manager.models import Ixmp4Instance

from ixmp4.rewrite.conf import settings


class Transport(abc.ABC):
    pass


logger = logging.getLogger(__name__)


@lru_cache()
def cached_create_engine(dsn: str) -> sa.Engine:
    # max_identifier_length=63 to avoid exceeding postgres' default maximum
    return sa.create_engine(dsn, poolclass=sa.NullPool, max_identifier_length=63)


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
        engine = cached_create_engine(dsn)
        session = Session(bind=engine)
        return cls(session, *args, **kwargs)

    def get_engine_info(self) -> str:
        if self.session.bind is None:
            return ""
        else:
            dialect = self.session.bind.engine.dialect.name
            host = self.session.bind.engine.url.host
            database = self.session.bind.engine.url.database
            return f"dialect={dialect} database={database} host={host}"

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


class HttpxTransport(Transport):
    client: httpx.Client | TestClient
    executor: ThreadPoolExecutor

    def __init__(
        self,
        client: httpx.Client | TestClient,
        max_concurrent_requests: int = settings.client_max_concurrent_requests,
    ):
        self.executor = ThreadPoolExecutor(max_workers=max_concurrent_requests)
        self.client = client

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

    def __str__(self) -> str:
        if (
            isinstance(self.client.auth, ManagerAuth)
            and self.client.auth.access_token.user is not None
        ):
            user = self.client.auth.access_token.user
        else:
            user = None
        return f"<HttpxTransport base_url={self.client.base_url} user={user}>"
