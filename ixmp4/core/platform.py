"""
IXMP4 provides a CLI for platform creation, deletion, etc.:

.. code:: bash

   # list all existing databases
   ixmp4 platforms list

   # run all migrations on all existing databases
   ixmp4 platforms upgrade

   # create a new database or register it
   ixmp4 platforms add <database-name> [--dsn sqlite://my/database/file.db]

   # delete a database
   ixmp4 platforms delete <database-name>

In development mode additional commands are available:

.. code:: bash

   # set the revision hash of all databases
   # without running migrations
   ixmp4 platforms stamp <revision-hash>

"""

from ixmp4.backend import Backend
from ixmp4.conf.platforms import PlatformConnectionInfo
from ixmp4.conf.settingsmodel import Settings
from ixmp4.core.exceptions import PlatformNotFound
from ixmp4.transport import DirectTransport, HttpxTransport

from .iamc import PlatformIamcData
from .meta import RunMetaServiceFacade
from .model import ModelServiceFacade
from .region import RegionServiceFacade
from .run import RunServiceFacade
from .scenario import ScenarioServiceFacade
from .unit import UnitServiceFacade


class Platform(object):
    """A modeling platform instance as a connection to a data backend.
    Enables the manipulation of data via the `Run` class and `Repository` instances."""

    NotFound = PlatformNotFound
    NotUnique = PlatformNotFound

    runs: RunServiceFacade
    iamc: PlatformIamcData
    models: ModelServiceFacade
    regions: RegionServiceFacade
    scenarios: ScenarioServiceFacade
    units: UnitServiceFacade
    meta: RunMetaServiceFacade

    backend: Backend
    settings: Settings
    connection_info: PlatformConnectionInfo

    """Provides a unified data interface for the platform.
    Using it directly is not recommended."""

    def __init__(
        self,
        name: str | None = None,
        _backend: Backend | None = None,
        _settings: Settings | None = None,
    ) -> None:
        if _settings is not None:
            self.settings = _settings
        else:
            self.settings = Settings()

        if _backend is not None:
            self.backend = _backend
        else:
            self.backend = self.init_backend(name)

        self.runs = RunServiceFacade(self.backend)
        self.iamc = PlatformIamcData(self.backend)
        self.models = ModelServiceFacade(self.backend)
        self.regions = RegionServiceFacade(self.backend)
        self.scenarios = ScenarioServiceFacade(self.backend)
        self.units = UnitServiceFacade(self.backend)
        self.meta = RunMetaServiceFacade(self.backend)

    def init_backend(self, name: str | None) -> Backend:
        if name is None:
            raise TypeError("__init__() is missing required argument 'name'")

        ci = self.get_toml_platform_ci(name)

        if ci is None:
            ci = self.get_manager_platform_ci(name)

        if ci is None:
            raise PlatformNotFound(f"Platform '{name}' was not found.")

        self.connection_info = ci

        transport = self.get_transport(ci)
        return Backend(transport)

    def get_transport(
        self, ci: PlatformConnectionInfo, http_credentials: str = "default"
    ) -> HttpxTransport | DirectTransport:
        if ci.dsn.startswith("http"):
            cred_dict = self.settings.get_credentials().get(http_credentials)
            return HttpxTransport.from_url(
                ci.dsn, self.settings.client, self.settings.get_client_auth(cred_dict)
            )
        else:
            return DirectTransport.from_dsn(ci.dsn)

    def get_toml_platform_ci(self, name: str) -> PlatformConnectionInfo | None:
        toml = self.settings.get_toml_platforms()

        try:
            return toml.get_platform(name)
        except PlatformNotFound:
            return None

    def get_manager_platform_ci(self, name: str) -> PlatformConnectionInfo | None:
        manager = self.settings.get_manager_platforms()

        try:
            return manager.get_platform(name)
        except PlatformNotFound:
            return None
