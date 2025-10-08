import abc
import logging
from concurrent.futures import ThreadPoolExecutor
from functools import lru_cache

import httpx
import sqlalchemy as sa
from fastapi.testclient import TestClient
from sqlalchemy import orm
from toolkit.auth.context import AuthorizationContext
from toolkit.client.auth import Auth, ManagerAuth

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
    auth_ctx: AuthorizationContext

    def __init__(
        self,
        auth_ctx: AuthorizationContext,
        session: orm.Session,
    ):
        self.auth_ctx = auth_ctx
        self.session = session

    @classmethod
    def from_dsn(
        cls, dsn: str, auth_ctx: AuthorizationContext | None = None
    ) -> "DirectTransport":
        # if auth_ctx is None:
        #     manager_client = settings.get_manager_client()
        #     default_auth = manager_client.auth
        #     if default_auth is None or not isinstance(default_auth, ManagerAuth):
        #         user = None
        #     elif getattr(default_auth, "access_token", None) is None:
        #         user = None
        #     else:
        #         user = default_auth.access_token.user

        #     auth_ctx = AuthorizationContext(user, settings.load_manager_client())

        if dsn.startswith("postgresql://"):
            logger.debug(
                "Replacing the platform dsn prefix to use the new `psycopg` driver."
            )
            dsn = dsn.replace("postgresql://", "postgresql+psycopg://")

        engine = cached_create_engine(dsn)
        session = Session(bind=engine)
        return cls(auth_ctx, session)

    def __str__(self) -> str:
        if self.session.bind is None:
            engine_info = ""
        else:
            dialect = self.session.bind.engine.dialect.name
            host = self.session.bind.engine.url.host
            database = self.session.bind.engine.url.database
            engine_info = f"dialect={dialect} database={database} host={host}"

        return f"<DirectTransport {engine_info} user={self.auth_ctx.user}>"


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
    def from_url(cls, url: str, auth: Auth | None = None) -> "HttpxTransport":
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
