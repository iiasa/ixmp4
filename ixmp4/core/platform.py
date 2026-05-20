import logging

import sqlalchemy as sa

from ixmp4.base_exceptions import (
    Ixmp4Error,
    PlatformNotFound,
    PlatformNotUnique,
    ServiceException,
)
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

logger = logging.getLogger(__name__)


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
        """Initialize a Platform instance.

        Parameters
        ----------
        name_or_connection : str or Transport or Backend
            Either the name of a platform (looked up in the local
            ``platforms.toml`` first, then the ECE Manager API), a
            :class:`~ixmp4.transport.Transport` instance, or a
            :class:`~ixmp4.data.backend.Backend` instance.
        settings : Settings, optional
            Custom :class:`~ixmp4.conf.settings.Settings` to use. Defaults to
            a newly initialized ``Settings()`` instance.

        Raises
        ------
        TypeError
            If ``name_or_connection`` is not a ``str``, ``Transport``, or
            ``Backend``.
        PlatformNotFound
            If a name is given but the platform cannot be found in either the
            local TOML configuration or the manager service.
        """
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
            raise TypeError(
                f"__init__() argument 'name_or_connection' must be a string "
                f"(platform name), Transport, or Backend, not "
                f"{type(name_or_connection).__name__}"
            )
        logger.debug(f"Initializing facade objects for {self.backend}.")

        self.runs = RunServiceFacade(self.backend)
        self.iamc = PlatformIamcData(self.backend)
        self.models = ModelServiceFacade(self.backend)
        self.regions = RegionServiceFacade(self.backend)
        self.scenarios = ScenarioServiceFacade(self.backend)
        self.units = UnitServiceFacade(self.backend)
        self.meta = PlatformRunMetaFacade(self.backend)

    def init_backend(self, name: str) -> Backend:
        """Resolve a platform name to a :class:`~ixmp4.data.backend.Backend`.

        Looks up the platform connection info first in the local
        ``platforms.toml`` file, then in the ECE Manager API, and initialises
        the appropriate :class:`~ixmp4.transport.Transport`.

        Parameters
        ----------
        name : str
            The name of the platform to look up.

        Returns
        -------
        Backend
            A fully initialised backend for the resolved platform.

        Raises
        ------
        PlatformNotFound
            If ``name`` cannot be found in either source.
        """
        ci = self.get_toml_platform_ci(name)

        if ci is None:
            ci = self.get_manager_platform_ci(name)

        if ci is None:
            raise PlatformNotFound(f"Platform '{name}' was not found.")

        transport = self.get_transport(ci)
        logger.debug(f"Initializing backend for {transport}.")
        return Backend(transport)

    def get_transport(
        self, ci: PlatformConnectionInfo, http_credentials: str = "default"
    ) -> HttpxTransport | DirectTransport:
        """Instantiate the correct transport for the given connection info.

        For HTTP-based DSNs an :class:`~ixmp4.transport.HttpxTransport` is
        returned.  For other DSNs (e.g. SQLite) a
        :class:`~ixmp4.transport.DirectTransport` is attempted first; if that
        fails and ``ci.url`` is available the method transparently falls back to
        an :class:`~ixmp4.transport.HttpxTransport`.

        Parameters
        ----------
        ci : PlatformConnectionInfo
            Connection details for the platform.
        http_credentials : str, optional
            Key used to look up HTTP credentials from the settings.  Defaults
            to ``"default"``.

        Returns
        -------
        HttpxTransport or DirectTransport
            The transport instance to use for this platform.

        Raises
        ------
        ServiceException, Ixmp4Error, SQLAlchemyError, ImportError
            Re-raised if the direct connection fails and no HTTP fallback URL
            is configured.
        """
        if ci.dsn.startswith("http"):
            cred_dict = self.settings.get_credentials().get(http_credentials)
            return HttpxTransport.from_url(
                ci.dsn,
                settings=self.settings.client,
                auth=self.settings.get_client_auth(cred_dict),
            )
        else:
            # Try direct connection first
            try:
                return DirectTransport.from_dsn(
                    ci.dsn,
                    check_alembic_version=self.settings.check_alembic_version,
                )
            except (
                ServiceException,
                Ixmp4Error,
                sa.exc.SQLAlchemyError,
                ImportError,
            ) as e:
                # If direct connection fails and HTTP URL is available,
                # fall back to HTTP transport.
                if ci.url is not None:
                    logger.debug(
                        f"Error while trying to establish direct connection: \n{e}"
                    )
                    logger.warning(
                        f"Direct connection failed with `{e.__class__.__name__}`. "
                        "Falling back to HTTP connection for platform "
                        f"'{ci.name}' at {ci.url}"
                    )
                    cred_dict = self.settings.get_credentials().get(http_credentials)
                    return HttpxTransport.from_url(
                        str(ci.url),
                        settings=self.settings.client,
                        auth=self.settings.get_client_auth(cred_dict),
                    )
                else:
                    # No HTTP URL available, re-raise the original error
                    raise

    def get_toml_platform_ci(self, name: str) -> PlatformConnectionInfo | None:
        """Look up platform connection info in the local TOML configuration."""
        toml = self.settings.get_toml_platforms()

        try:
            toml_platform = toml.get_platform(name)
            logger.debug(
                f"Connecting to platform '{toml_platform.name}' "
                "via toml configuration..."
            )
            return toml_platform
        except PlatformNotFound:
            return None

    def get_manager_platform_ci(self, name: str) -> PlatformConnectionInfo | None:
        """Look up platform connection info via the ECE Manager API."""
        manager = self.settings.get_manager_platforms()

        try:
            manager_platform = manager.get_platform(name)
            logger.debug(
                f"Connecting to platform '{manager_platform.name}' "
                "via manager service..."
            )

            if manager_platform.notice is not None:
                logger.info(manager_platform.name + ": " + manager_platform.notice)

            return manager_platform
        except PlatformNotFound:
            return None
