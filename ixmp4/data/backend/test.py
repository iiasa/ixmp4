from typing import TYPE_CHECKING

import pandas as pd
from fastapi.testclient import TestClient
from sqlalchemy.engine import create_engine
from sqlalchemy.pool import NullPool, StaticPool

# TODO Import this from typing when dropping support for Python 3.11
from typing_extensions import Unpack

from ixmp4.conf import settings
from ixmp4.conf.auth import BaseAuth, SelfSignedAuth
from ixmp4.conf.base import PlatformInfo
from ixmp4.conf.manager import ManagerPlatformInfo, MockManagerConfig
from ixmp4.conf.user import User
from ixmp4.server import app, v1
from ixmp4.server.rest import deps

from .api import RestBackend
from .db import SqlAlchemyBackend

if TYPE_CHECKING:
    from .db import SqlAlchemyBackend


class SqliteTestBackend(SqlAlchemyBackend):
    def __init__(self, *args: Unpack[tuple[PlatformInfo]]) -> None:
        super().__init__(*args)

    def make_engine(self, dsn: str) -> None:
        self.engine = create_engine(
            dsn,
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
            max_identifier_length=63,
        )
        self.session = self.Session(bind=self.engine)


class PostgresTestBackend(SqlAlchemyBackend):
    def __init__(self, *args: Unpack[tuple[PlatformInfo]]) -> None:
        super().__init__(*args)

    def make_engine(self, dsn: str) -> None:
        self.engine = create_engine(
            dsn,
            poolclass=NullPool,
            max_identifier_length=63,
        )
        self.session = self.Session(bind=self.engine)


test_platform = ManagerPlatformInfo(
    id=1,
    management_group=1,
    access_group=1,
    accessibility=ManagerPlatformInfo.Accessibilty.PRIVATE,
    slug="test",
    notice="Welcome to the test platform.",
    dsn="http://testserver/v1/:test:/",
    url="http://testserver/v1/:test:/",
)

test_user = User(id=-1, username="@unknown", is_superuser=True, is_verified=True)

test_permissions = pd.DataFrame(
    [], columns=["id", "instance", "group", "access_type", "model"]
)

mock_manager = MockManagerConfig([test_platform], test_permissions)


class RestTestBackend(RestBackend):
    db_backend: "SqlAlchemyBackend"

    def __init__(self, db_backend: "SqlAlchemyBackend") -> None:
        self.db_backend = db_backend
        self.auth_params = (test_user, mock_manager, test_platform)
        super().__init__(test_platform, SelfSignedAuth(settings.secret_hs256))

    def make_client(self, rest_url: str, auth: BaseAuth) -> None:
        self.client = TestClient(
            app=app,
            base_url=rest_url,
            raise_server_exceptions=False,
        )

        app.dependency_overrides[deps.validate_token] = deps.do_not_validate_token
        v1.dependency_overrides[deps.validate_token] = deps.do_not_validate_token

        app.dependency_overrides[deps.get_backend] = deps.get_test_backend_dependency(
            self.db_backend, self.auth_params
        )
        v1.dependency_overrides[deps.get_backend] = deps.get_test_backend_dependency(
            self.db_backend, self.auth_params
        )

    def close(self) -> None:
        self.client.close()
        self.executor.shutdown(cancel_futures=True)

    def setup(self) -> None:
        pass

    def teardown(self) -> None:
        pass
