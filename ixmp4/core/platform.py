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

from ixmp4.conf import settings
from ixmp4.conf.auth import BaseAuth
from ixmp4.core.exceptions import PlatformNotFound
from ixmp4.data.backend import Backend, RestBackend, SqlAlchemyBackend

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
        _auth: BaseAuth | None = None,
    ) -> None:
        if name is not None:
            if name in settings.toml.platforms:
                config = settings.toml.get_platform(name)
            else:
                settings.check_credentials()
                if settings.manager is not None:
                    config = settings.manager.get_platform(name)
                else:
                    raise PlatformNotFound(f"Platform '{name}' was not found.")

            if config.dsn.startswith("http"):
                self.backend = RestBackend(config, auth=_auth)
            else:
                self.backend = SqlAlchemyBackend(config)  # type: ignore
        elif _backend is not None:
            self.backend = _backend
        else:
            raise TypeError("__init__() is missing required argument 'name'")

        self.runs = RunRepository(_backend=self.backend)
        self.iamc = PlatformIamcData(_backend=self.backend)
        self.models = ModelRepository(_backend=self.backend)
        self.regions = RegionRepository(_backend=self.backend)
        self.scenarios = ScenarioRepository(_backend=self.backend)
        self.units = UnitRepository(_backend=self.backend)
        self.meta = MetaRepository(_backend=self.backend)
