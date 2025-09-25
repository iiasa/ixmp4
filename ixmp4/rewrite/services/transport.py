import abc
from concurrent.futures import ThreadPoolExecutor

import httpx
from fastapi.testclient import TestClient
from sqlalchemy import orm
from toolkit.auth import AuthorizationContext
from toolkit.manager.models import Ixmp4Instance


class Transport(abc.ABC):
    pass


class DirectTransport(Transport):
    session: orm.Session
    auth_ctx: AuthorizationContext
    platform: Ixmp4Instance

    def __init__(
        self,
        session: orm.Session,
        auth_ctx: AuthorizationContext,
        platform: Ixmp4Instance,
    ):
        self.session = session
        self.auth_ctx = auth_ctx
        self.platform = platform


class HttpxTransport(Transport):
    client: httpx.Client | TestClient
    executor: ThreadPoolExecutor

    def __init__(
        self,
        client: httpx.Client | TestClient,
        max_concurrent_requests: int = 2,
    ):
        self.executor = ThreadPoolExecutor(max_workers=max_concurrent_requests)
        self.client = client
