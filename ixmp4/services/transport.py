import abc
from concurrent.futures import ThreadPoolExecutor

import httpx
from fastapi.testclient import TestClient
from sqlalchemy import orm
from toolkit.auth import AuthorizationContext


class AbstractTransport(abc.ABC):
    pass


class DirectTransport(AbstractTransport):
    session: orm.Session
    auth_ctx: AuthorizationContext

    def __init__(self, session: orm.Session, auth_ctx: AuthorizationContext):
        self.session = session
        self.auth_ctx = auth_ctx


class HttpxTransport(AbstractTransport):
    client: httpx.Client | TestClient
    executor: ThreadPoolExecutor

    def __init__(
        self,
        client: httpx.Client | TestClient,
        max_concurrent_requests: int = 2,
    ):
        self.executor = ThreadPoolExecutor(max_workers=max_concurrent_requests)
        self.client = client
