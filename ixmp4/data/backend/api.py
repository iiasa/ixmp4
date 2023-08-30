import logging

import httpx
import pandas as pd
from fastapi.testclient import TestClient

from ixmp4.conf import settings
from ixmp4.conf.auth import BaseAuth, SelfSignedAuth, User
from ixmp4.conf.manager import ManagerPlatformInfo, MockManagerConfig, PlatformInfo
from ixmp4.core.exceptions import ImproperlyConfigured, UnknownApiError
from ixmp4.data.api import (
    DataPointRepository,
    IndexSetRepository,
    ModelRepository,
    RunMetaEntryRepository,
    RunRepository,
    ScenarioRepository,
    TimeSeriesRepository,
    VariableRepository,
)
from ixmp4.data.api.region import RegionRepository
from ixmp4.data.api.unit import UnitRepository
from ixmp4.server import app, v1
from ixmp4.server.rest import APIInfo, deps

from .base import Backend

logger = logging.getLogger(__name__)


class RestBackend(Backend):
    client: httpx.Client

    def __init__(self, info: PlatformInfo, auth: BaseAuth | None = None) -> None:
        super().__init__(info)
        logger.info(f"Connecting to IXMP4 REST API at {info.dsn}")
        if isinstance(info, ManagerPlatformInfo):
            if info.notice is not None:
                logger.info("Platform notice: " + info.notice)
        self.make_client(info.dsn, auth=auth)
        self.create_repositories()

    def make_client(self, rest_url: str, auth: BaseAuth | None):
        auth = self.get_auth(rest_url, auth)

        self.client = httpx.Client(
            base_url=rest_url,
            timeout=30.0,
            http2=True,
            auth=auth,
            follow_redirects=True,
        )

    def get_auth(self, rest_url: str, override_auth: BaseAuth | None) -> BaseAuth:
        root = httpx.get(rest_url, follow_redirects=True)
        if root.status_code != 200:
            logger.error("Root API response not OK: " + root.text)
            raise UnknownApiError(f"Server response not OK. ({root.status_code})")

        api_info = APIInfo(**root.json())
        logger.info(f"Connected to Platform '{api_info.name}'")
        logger.info("Server IXMP4 Version: " + api_info.version)

        logger.debug("Server UTC Time: " + api_info.utcnow.strftime("%c"))
        logger.debug("Server Is Managed: " + str(api_info.is_managed))
        if api_info.manager_url is not None:
            if (
                api_info.manager_url.rstrip("/") != settings.manager_url.rstrip("/")
                and api_info.is_managed
            ):
                logger.error("Server Manager URL: " + api_info.manager_url)
                logger.error("Local Manager URL: " + settings.manager_url)
                raise ImproperlyConfigured(
                    "Trying to connect to a managed REST Platform "
                    "with a mismatching Manager URL."
                )

        if override_auth is None:
            if api_info.is_managed:
                return settings.default_auth
            else:
                logger.info(
                    "Connecting to unmanaged server instance, "
                    "falling back to self-signed auth."
                )
                return SelfSignedAuth(settings.secret_hs256)
        else:
            return override_auth

    def create_repositories(self):
        self.iamc.datapoints = DataPointRepository(self.client)
        self.iamc.timeseries = TimeSeriesRepository(self.client)
        self.iamc.variables = VariableRepository(self.client)
        self.meta = RunMetaEntryRepository(self.client)
        self.models = ModelRepository(self.client)
        self.optimization.indexsets = IndexSetRepository(self.client)
        self.regions = RegionRepository(self.client)
        self.runs = RunRepository(self.client)
        self.scenarios = ScenarioRepository(self.client)
        self.units = UnitRepository(self.client)


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
    def __init__(self, db_backend, *args, **kwargs) -> None:
        self.db_backend = db_backend
        self.auth_params = (test_user, mock_manager, test_platform)
        super().__init__(
            test_platform, SelfSignedAuth(settings.secret_hs256), *args, **kwargs
        )

    def make_client(self, rest_url: str, auth: BaseAuth):
        self.client = TestClient(app=app, base_url=rest_url)

        app.dependency_overrides[deps.validate_token] = deps.do_not_validate_token
        v1.dependency_overrides[deps.validate_token] = deps.do_not_validate_token

        app.dependency_overrides[deps.get_backend] = deps.get_test_backend_dependency(
            self.db_backend, self.auth_params
        )
        v1.dependency_overrides[deps.get_backend] = deps.get_test_backend_dependency(
            self.db_backend, self.auth_params
        )

    def close(self):
        self.client.close()
