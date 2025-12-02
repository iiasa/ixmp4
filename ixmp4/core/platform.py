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

from toolkit.client.auth import Auth

from ixmp4.backend import Backend
from ixmp4.base_exceptions import PlatformNotFound
from ixmp4.conf import settings
from ixmp4.conf.platforms import PlatformConnectionInfo
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

    runs: RunServiceFacade
    iamc: PlatformIamcData
    models: ModelServiceFacade
    regions: RegionServiceFacade
    scenarios: ScenarioServiceFacade
    units: UnitServiceFacade
    meta: RunMetaServiceFacade

    backend: Backend
    connection_info: PlatformConnectionInfo

    """Provides a unified data interface for the platform.
    Using it directly is not recommended."""

    def __init__(
        self,
        name: str | None = None,
        _backend: Backend | None = None,
        _auth: Auth | None = None,
    ) -> None:
        if _backend is None:
            if name is None:
                raise TypeError("__init__() is missing required argument 'name'")

            ci = self.get_toml_platform_ci(name)

            if ci is None:
                ci = self.get_manager_platform_ci(name)

            if ci is None:
                raise PlatformNotFound(f"Platform '{name}' was not found.")

            self.connection_info = ci

            if ci.dsn.startswith("http"):
                self.backend = Backend(
                    HttpxTransport.from_url(ci.dsn, settings.get_client_auth())
                )
            else:
                self.backend = Backend(DirectTransport.from_dsn(ci.dsn))
        else:
            self.backend = _backend

        self.runs = RunServiceFacade(self.backend)
        self.iamc = PlatformIamcData(self.backend)
        self.models = ModelServiceFacade(self.backend)
        self.regions = RegionServiceFacade(self.backend)
        self.scenarios = ScenarioServiceFacade(self.backend)
        self.units = UnitServiceFacade(self.backend)
        self.meta = RunMetaServiceFacade(self.backend)

    def get_toml_platform_ci(self, name: str) -> PlatformConnectionInfo | None:
        toml = settings.get_toml_platforms()

        try:
            return toml.get_platform(name)
        except PlatformNotFound:
            return None

    def get_manager_platform_ci(self, name: str) -> PlatformConnectionInfo | None:
        manager = settings.get_manager_platforms()

        try:
            return manager.get_platform(name)
        except PlatformNotFound:
            return None
