import httpx
from fastapi.testclient import TestClient

from ixmp4.conf.auth import BaseAuth, SelfSignedAuth
from ixmp4.conf import settings, PlatformInfo
from ixmp4.data.api import (
    DataPointRepository,
    RunRepository,
    RunMetaEntryRepository,
    TimeSeriesRepository,
    ScenarioRepository,
    ModelRepository,
    VariableRepository,
)
from ixmp4.data.api.unit import UnitRepository
from ixmp4.data.api.region import RegionRepository
from ixmp4.server import app, v1
from ixmp4.server.rest import deps

from .base import Backend


class RestBackend(Backend):
    client: httpx.Client

    def __init__(self, info: PlatformInfo, auth: BaseAuth | None) -> None:
        super().__init__(info)
        self.make_client(info.dsn, auth=auth)
        self.create_repositories()

    def make_client(self, rest_url: str, auth: BaseAuth | None):
        self.client = httpx.Client(
            base_url=rest_url, timeout=30.0, http2=True, auth=auth
        )

    def create_repositories(self):
        self.runs = RunRepository(self.client)
        self.meta = RunMetaEntryRepository(self.client)
        self.iamc.datapoints = DataPointRepository(self.client)
        self.iamc.timeseries = TimeSeriesRepository(self.client)
        self.iamc.variables = VariableRepository(self.client)
        self.regions = RegionRepository(self.client)
        self.scenarios = ScenarioRepository(self.client)
        self.models = ModelRepository(self.client)
        self.units = UnitRepository(self.client)


class RestTestBackend(RestBackend):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(
            PlatformInfo(name="test", dsn="http://testserver/v1/:memory:/"),
            SelfSignedAuth(settings.secret_hs256),
            *args,
            **kwargs
        )

    def make_client(self, rest_url: str, auth: BaseAuth):
        self.client = TestClient(app=app, base_url=rest_url)

        app.dependency_overrides[deps.validate_token] = deps.do_not_validate_token
        v1.dependency_overrides[deps.validate_token] = deps.do_not_validate_token

        app.dependency_overrides[deps.get_backend] = deps.get_test_backend_dependency()
        v1.dependency_overrides[deps.get_backend] = deps.get_test_backend_dependency()

    def close(self):
        self.client.close()
