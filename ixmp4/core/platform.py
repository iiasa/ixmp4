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

from functools import partial

from ixmp4.conf import settings
from ixmp4.core.exceptions import PlatformNotFound
from ixmp4.data.backend import SqlAlchemyBackend, RestBackend, Backend

from .run import Run as RunModel, RunRepository
from .model import ModelRepository
from .scenario import ScenarioRepository
from .unit import UnitRepository
from .region import RegionRepository
from .meta import MetaRepository
from .iamc import IamcRepository


class Platform(object):
    """A modeling platform instance as a connection to a data backend.
    Enables the manipulation of data via the `Run` class and `Repository` instances."""

    Run: partial[RunModel]

    runs: RunRepository
    iamc: IamcRepository
    models: ModelRepository
    regions: RegionRepository
    scenarios: ScenarioRepository
    units: UnitRepository
    meta: MetaRepository

    backend: Backend
    """Provides a unified data interface for the platform.
    Using it directly is not recommended."""

    def __init__(
        self, name: str | None = None, _backend: Backend | None = None
    ) -> None:
        if name is not None:
            try:
                config = settings.toml.get_platform(name)
                auth = settings.default_auth
            except PlatformNotFound:
                if settings.manager is not None:
                    config = settings.manager.get_platform(name)
                    auth = settings.default_auth
                else:
                    raise PlatformNotFound(f"Platform '{name}' was not found.")

            if config.dsn.startswith("http"):
                self.backend = RestBackend(config, auth=auth)
            else:
                self.backend = SqlAlchemyBackend(config)  # type: ignore
        elif _backend is not None:
            self.backend = _backend
        else:
            raise TypeError("__init__() is missing required argument 'name'")
        self.Run = partial(RunModel, _backend=self.backend)

        self.runs = RunRepository(_backend=self.backend)
        self.iamc = IamcRepository(_backend=self.backend)
        self.models = ModelRepository(_backend=self.backend)
        self.regions = RegionRepository(_backend=self.backend)
        self.scenarios = ScenarioRepository(_backend=self.backend)
        self.units = UnitRepository(_backend=self.backend)
        self.meta = MetaRepository(_backend=self.backend)
