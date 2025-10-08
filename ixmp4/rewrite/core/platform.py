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

from ixmp4.rewrite.backend import Backend
from ixmp4.rewrite.conf import settings
from ixmp4.rewrite.conf.platforms import PlatformConnectionInfo
from ixmp4.rewrite.exceptions import PlatformNotFound
from ixmp4.rewrite.transport import DirectTransport, HttpxTransport, Transport

from .iamc import PlatformIamcData
from .meta import MetaRepository
from .model import ModelRepository
from .region import RegionRepository
from .run import RunRepository
from .scenario import ScenarioRepository
from .unit import UnitRepository


class Platform(object):
    """A modeling platform instance as a connection to a data backend.
    Enables the manipulation of data via the `Run` class and `Repository` instances."""

    runs: RunRepository
    iamc: PlatformIamcData
    models: ModelRepository
    regions: RegionRepository
    scenarios: ScenarioRepository
    units: UnitRepository
    meta: MetaRepository

    backend: Backend
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

            ci = self.get_toml_platform(name)

            if ci is None:
                ci = self.get_manager_platform(name)

            if ci is None:
                raise PlatformNotFound(f"Platform '{name}' was not found.")

            self.backend = Backend()
        else:
            self.backend = _backend

        self.runs = RunRepository(backend=self.backend)
        self.iamc = PlatformIamcData(backend=self.backend)
        self.models = ModelRepository(backend=self.backend)
        self.regions = RegionRepository(backend=self.backend)
        self.scenarios = ScenarioRepository(backend=self.backend)
        self.units = UnitRepository(backend=self.backend)
        self.meta = MetaRepository(backend=self.backend)

    def get_transport(self, ci: PlatformConnectionInfo) -> Transport:
        if ci.dsn.startswith("http"):
            auth = settings.get_client_auth()
            return HttpxTransport.from_url(ci.dsn, auth=auth)
        else:
            return DirectTransport.from_dsn(ci.dsn)

    def get_toml_platform(self, name: str) -> PlatformConnectionInfo | None:
        toml = settings.get_toml_platforms()

        try:
            return toml.get_platform(name)
        except PlatformNotFound:
            return None

    def get_manager_platform(self, name: str) -> PlatformConnectionInfo | None:
        manager = settings.get_manager_platforms()

        try:
            return manager.get_platform(name)
        except PlatformNotFound:
            return None
