from ixmp4.base_exceptions import PlatformNotFound, PlatformNotUnique
from ixmp4.conf.platforms import PlatformConnectionInfo
from ixmp4.conf.settings import Settings
from ixmp4.data.backend import Backend
from ixmp4.transport import DirectTransport, HttpxTransport, Transport

from .iamc import PlatformIamcData
from .meta import PlatformRunMetaFacade
from .model import ModelServiceFacade
from .region import RegionServiceFacade
from .run import RunServiceFacade
from .scenario import ScenarioServiceFacade
from .unit import UnitServiceFacade


class Platform(object):
    """A modeling platform instance as a connection to a data backend.
    Enables the manipulation of data via the ``Facade`` instances.

    To instantiate a new platform, provide a name which will be
    used to first search the local 'platforms.toml' file and
    then the ECE Manager API.

    .. code:: python

        import ixmp4

        platform = ixmp4.Platform("<name>")


    You may override the settings the platform uses ...

    .. code:: python

        from ixmp4.conf.settings import Settings

        platform = ixmp4.Platform("<name>", settings=Settings(manager_url="https://.../"))

    ...and provide a :class:`~ixmp4.data.backend.Backend` or
    :class:`~ixmp4.transport.Transport` class directly.

    .. code:: python

        from ixmp4.transport import DirectTransport
        from ixmp4.data.backend import Backend

        platform = ixmp4.Platform(Backend(...))
        # or
        platform = ixmp4.Platform(DirectTransport.from_dsn(...))


    Once created, the platform's ``Facade`` attributes
    can be used to manipulate data:

    .. list-table::
        :header-rows: 1

        * - Attribute
          - Service Facade Class
          - Object Facade Class

        * - :py:attr:`~.runs`
          - :class:`~ixmp4.core.run.RunServiceFacade`
          - :class:`~ixmp4.core.run.Run`

        * - :py:attr:`~.meta`
          - :class:`~ixmp4.core.meta.PlatformRunMetaFacade`
          - :class:`~ixmp4.core.meta.RunMetaDescriptor`/
            :class:`~ixmp4.core.meta.RunMetaDictFacade`

        * - :py:attr:`~.iamc`
          - :class:`~ixmp4.core.iamc.data.PlatformIamcData`
          - :class:`~ixmp4.core.iamc.data.RunIamcData`

        * - :py:attr:`~.models`
          - :class:`~ixmp4.core.run.ModelServiceFacade`
          - :class:`~ixmp4.core.run.Model`

        * - :py:attr:`~.scenarios`
          - :class:`~ixmp4.core.scenario.ScenarioServiceFacade`
          - :class:`~ixmp4.core.scenario.Scenario`

        * - :py:attr:`~.regions`
          - :class:`~ixmp4.core.region.RegionServiceFacade`
          - :class:`~ixmp4.core.region.Region`

        * - :py:attr:`~.units`
          - :class:`~ixmp4.core.unit.UnitServiceFacade`
          - :class:`~ixmp4.core.unit.Unit`

    """

    NotFound = PlatformNotFound
    NotUnique = PlatformNotUnique

    runs: RunServiceFacade
    """Facade instance to manage :class:`~ixmp4.core.run.Run` instances
    for a platform."""

    meta: PlatformRunMetaFacade
    """Facade instance to query run meta indicators
    globally for a platform."""

    iamc: PlatformIamcData
    """Facade instance to query IAMC data globally for a platform."""

    models: ModelServiceFacade
    """Facade instance to manage :class:`~ixmp4.core.model.Model` instances
    for a platform."""

    scenarios: ScenarioServiceFacade
    """Facade instance to manage :class:`~ixmp4.core.scenario.Scenario` instances
    for a platform."""

    regions: RegionServiceFacade
    """Facade instance to manage :class:`~ixmp4.core.region.Region` instances
    for a platform."""

    units: UnitServiceFacade
    """Facade instance to manage :class:`~ixmp4.core.unit.Unit` instances
    for a platform."""

    backend: Backend
    """Central data layer object that is composed of services."""

    settings: Settings
    """The settings object the platform is using."""

    def __init__(
        self,
        name_or_connection: str | Transport | Backend,
        settings: Settings | None = None,
    ) -> None:
        if settings is not None:
            self.settings = settings
        else:
            self.settings = Settings()

        if isinstance(name_or_connection, str):
            self.backend = self.init_backend(name_or_connection)
        elif isinstance(name_or_connection, Transport):
            self.backend = Backend(name_or_connection)
        elif isinstance(name_or_connection, Backend):
            self.backend = name_or_connection
        else:
            raise TypeError("__init__() is missing required argument 'name'")

        self.runs = RunServiceFacade(self.backend)
        self.iamc = PlatformIamcData(self.backend)
        self.models = ModelServiceFacade(self.backend)
        self.regions = RegionServiceFacade(self.backend)
        self.scenarios = ScenarioServiceFacade(self.backend)
        self.units = UnitServiceFacade(self.backend)
        self.meta = PlatformRunMetaFacade(self.backend)

    def init_backend(self, name: str) -> Backend:
        ci = self.get_toml_platform_ci(name)

        if ci is None:
            ci = self.get_manager_platform_ci(name)

        if ci is None:
            raise PlatformNotFound(f"Platform '{name}' was not found.")

        transport = self.get_transport(ci)
        return Backend(transport)

    def get_transport(
        self, ci: PlatformConnectionInfo, http_credentials: str = "default"
    ) -> HttpxTransport | DirectTransport:
        if ci.dsn.startswith("http"):
            cred_dict = self.settings.get_credentials().get(http_credentials)
            return HttpxTransport.from_url(
                ci.dsn,
                settings=self.settings.client,
                auth=self.settings.get_client_auth(cred_dict),
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
