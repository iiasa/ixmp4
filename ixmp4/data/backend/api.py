import logging
from concurrent.futures import ThreadPoolExecutor

import httpx

from ixmp4.conf import settings
from ixmp4.conf.auth import BaseAuth, SelfSignedAuth
from ixmp4.conf.manager import ManagerPlatformInfo, PlatformInfo
from ixmp4.core.exceptions import ImproperlyConfigured, UnknownApiError
from ixmp4.data.api import (
    CheckpointRepository,
    DataPointRepository,
    EquationRepository,
    IndexSetRepository,
    ModelRepository,
    OptimizationVariableRepository,
    ParameterRepository,
    RegionRepository,
    RunMetaEntryRepository,
    RunRepository,
    ScalarRepository,
    ScenarioRepository,
    TableRepository,
    TimeSeriesRepository,
    UnitRepository,
    VariableRepository,
)
from ixmp4.server.rest import APIInfo

from .base import Backend

logger = logging.getLogger(__name__)


class RestBackend(Backend):
    client: httpx.Client
    executor: ThreadPoolExecutor
    timeout: httpx.Timeout

    def __init__(
        self,
        info: PlatformInfo,
        auth: BaseAuth | None = None,
        max_concurrent_requests: int = settings.client_max_concurrent_requests,
    ) -> None:
        super().__init__(info)
        logger.debug(f"Connecting to IXMP4 REST API at {info.dsn}.")
        self.executor = ThreadPoolExecutor(max_workers=max_concurrent_requests)
        self.timeout = httpx.Timeout(settings.client_timeout, connect=60.0)
        self.make_client(info.dsn, auth=auth)
        if isinstance(info, ManagerPlatformInfo):
            if info.notice is not None:
                logger.info("Platform notice: >\n" + info.notice)
        self.create_repositories()

    def make_client(self, rest_url: str, auth: BaseAuth | None) -> None:
        auth = self.get_auth(rest_url, auth)

        self.client = httpx.Client(
            base_url=rest_url,
            timeout=self.timeout,
            http2=True,
            auth=auth,
        )

    def get_auth(
        self, rest_url: str, override_auth: BaseAuth | None
    ) -> BaseAuth | None:
        root = httpx.get(rest_url, follow_redirects=True)
        if root.status_code != 200:
            logger.error("Root API response not OK: " + root.text)
            raise UnknownApiError(f"Server response not OK. ({root.status_code})")

        api_info = APIInfo(**root.json())
        logger.info(f"Connected to IXMP4 Platform '{api_info.name}'")

        import ixmp4

        if ixmp4.__version__ != api_info.version:
            logger.warning(
                "IXMP4 Client and Server versions do not match. "
                f"(Client: {ixmp4.__version__}, Server: {api_info.version})"
            )

        logger.debug("Server UTC Time: " + api_info.utcnow.strftime("%c"))
        logger.debug("Server Is Managed: " + str(api_info.is_managed))
        if api_info.manager_url is not None:
            if (
                api_info.manager_url.rstrip("/")
                != str(settings.manager_url).rstrip("/")
                and api_info.is_managed
            ):
                logger.error(f"Server Manager URL: {api_info.manager_url}")
                logger.error(f"Local Manager URL: {settings.manager_url}")
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

    def create_repositories(self) -> None:
        self.iamc.datapoints = DataPointRepository(self)
        self.iamc.timeseries = TimeSeriesRepository(self)
        self.iamc.variables = VariableRepository(self)
        self.meta = RunMetaEntryRepository(self)
        self.models = ModelRepository(self)
        self.optimization.equations = EquationRepository(self)
        self.optimization.indexsets = IndexSetRepository(self)
        self.optimization.parameters = ParameterRepository(self)
        self.optimization.scalars = ScalarRepository(self)
        self.optimization.tables = TableRepository(self)
        self.optimization.variables = OptimizationVariableRepository(self)
        self.regions = RegionRepository(self)
        self.runs = RunRepository(self)
        self.scenarios = ScenarioRepository(self)
        self.units = UnitRepository(self)
        self.checkpoints = CheckpointRepository(self)
