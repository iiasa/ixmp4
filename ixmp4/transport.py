"""
The ``ixmp4.transport`` module defines how ixmp4 routes data-layer operations
to their execution target.  All transports share a common abstract base class
(:class:`~ixmp4.transport.Transport`) and can either call service methods
directly using a local database session (:class:`~ixmp4.transport.DirectTransport`,
:class:`~ixmp4.transport.AuthorizedTransport`) or use a remote ixmp4 http server
():class:`~ixmp4.transport.HttpxTransport`).

Transport selection is handled automatically by
:meth:`~ixmp4.core.platform.Platform.get_transport` based on the platform's
configured DSN:

* A DSN starting with ``http`` or ``https`` -> :class:`~ixmp4.transport.HttpxTransport`

* Any other DSN (``sqlite://...``, ``postgresql://...``) -> \
:class:`~ixmp4.transport.DirectTransport`

If a direct connection to a database fails *and* the platform also has an HTTP
URL configured, :class:`~ixmp4.core.platform.Platform` automatically falls back
to :class:`~ixmp4.transport.HttpxTransport`.

"""

import abc
import datetime as dt
import logging
import time
from concurrent.futures import ThreadPoolExecutor
from email.utils import parsedate_to_datetime
from functools import lru_cache
from typing import Any

import httpx
import sqlalchemy as sa
from litestar import Litestar
from litestar.testing import TestClient
from sqlalchemy import orm
from toolkit.auth.context import AuthorizationContext, PlatformProtocol
from toolkit.client.auth import Auth, ManagerAuth, SelfSignedAuth
from toolkit.client.base import ServiceClient

from ixmp4.base_exceptions import ImproperlyConfigured
from ixmp4.conf.platforms import resolve_dsn_env_tokens
from ixmp4.conf.settings import ClientSettings, Settings
from ixmp4.core.exceptions import ProgrammingError, VersioningNotSupported
from ixmp4.core.exceptions import registry as exception_registry
from ixmp4.db import get_alembic_controller

from ._version import __version__


class Transport(abc.ABC):
    """Abstract base class for all ixmp4 transport backends.

    A transport is holds context for routing data-layer operations to their
    execution target, which can be either a local SQLAlchemy session
    (:class:`DirectTransport`) or a remote ixmp4 HTTP server
    (:class:`HttpxTransport`).
    """

    def check_versioning_compatiblity(self) -> None:
        raise NotImplementedError

    def __str__(self) -> str:
        return f"<{self.__class__.__name__}>"


logger = logging.getLogger(__name__)


@lru_cache()
def cached_create_engine(dsn: str, **kwargs: Any) -> sa.Engine:
    """Create and cache a SQLAlchemy engine for *dsn*.

    The result is memoized so that repeated calls with the same DSN reuse the
    existing engine.

    Parameters
    ----------
    dsn:
        Database connection string (SQLAlchemy DSN format).
    **kwargs:
        Additional keyword arguments forwarded to :func:`sqlalchemy.create_engine`.
    """
    # max_identifier_length=63 to avoid exceeding postgres' default maximum
    return sa.create_engine(
        dsn, poolclass=sa.NullPool, max_identifier_length=63, **kwargs
    )


Session = orm.sessionmaker(autocommit=False, autoflush=False)


class DirectTransport(Transport):
    """Transport that operates directly with a local SQLAlchemy database session.

    Attributes
    ----------
    session:
        The active SQLAlchemy ORM session used for all database operations.
    """

    session: orm.Session

    def __init__(
        self,
        session: orm.Session,
        ping_database: bool = True,
        check_alembic_version: bool = True,
    ):
        """Initialise the transport with an existing SQLAlchemy session.

        Parameters
        ----------
        session:
            An already-configured SQLAlchemy ORM session.
        ping_database:
            When ``True`` (the default), a ``SELECT 1`` statement is executed
            immediately to verify that the database connection is live.
        check_alembic_version:
            When ``True`` (the default), the database migration version is
            compared to the newest revision known to this ixmp4 version.
        """
        self.session = session
        if ping_database:
            self.session.execute(sa.text("SELECT 1"))

        if check_alembic_version:
            self.check_alembic_version()

        if (url := self.get_database_url()) is not None:
            logger.debug(f"Connected to IXMP4 database at '{url.render_as_string()}'.")

    @classmethod
    def check_dsn(cls, dsn: str) -> str:
        """Normalise *dsn* to use the ``psycopg`` driver prefix where required."""
        if dsn.startswith("postgresql://"):
            logger.debug(
                "Replacing the platform dsn prefix to use the new `psycopg` driver."
            )
            dsn = dsn.replace("postgresql://", "postgresql+psycopg://")
        return dsn

    @classmethod
    def from_dsn(cls, dsn: str, *args: Any, **kwargs: Any) -> "DirectTransport":
        """Create a :class:`DirectTransport` from a connection-string DSN.

        The DSN is first passed through
        :func:`~ixmp4.conf.platforms.resolve_dsn_env_tokens` to expand any
        ``{env:VAR}`` placeholders, then normalised via :meth:`check_dsn`
        before a database engine is created.

        Parameters
        ----------
        dsn:
            Database connection string.
        *args:
            Positional arguments forwarded to the constructor.
        **kwargs:
            Keyword arguments forwarded to the constructor.

        Returns
        -------
        DirectTransport
            A transport instance connected to the specified database.

        Raises
        ------
        :exc:`~ixmp4.core.exceptions.ProgrammingError`
            If the dialect prefix in *dsn* is not ``sqlite`` or ``postgresql``.
        """
        # Resolve environment variable placeholders in DSN
        dsn = resolve_dsn_env_tokens(dsn)
        dsn = cls.check_dsn(dsn)
        if dsn.startswith("sqlite"):
            engine = cls.create_sqlite_engine(dsn)
        elif dsn.startswith("postgresql"):
            engine = cls.create_postgresql_engine(dsn)
        else:
            raise ProgrammingError("Unsupported database dialect for DSN: " + dsn)
        session = Session(bind=engine)
        return cls(session, *args, **kwargs)

    @classmethod
    @lru_cache()
    def create_postgresql_engine(cls, dsn: str) -> sa.Engine:
        return sa.create_engine(dsn, poolclass=sa.StaticPool, max_identifier_length=63)

    @classmethod
    @lru_cache()
    def create_sqlite_engine(cls, dsn: str) -> sa.Engine:
        return sa.create_engine(
            dsn,
            poolclass=sa.StaticPool,
            max_identifier_length=63,
            connect_args={"check_same_thread": False},
        )

    def get_database_url(self) -> sa.URL | None:
        """Return the SQLAlchemy URL of the bound engine, or ``None``."""
        if self.session.bind is None:
            return None
        else:
            return self.session.bind.engine.url

    def get_engine_info(self) -> str:
        if self.session.bind is None:
            return ""
        else:
            dialect = self.session.bind.engine.dialect.name
            host = self.session.bind.engine.url.host
            database = self.session.bind.engine.url.database
            return f"dialect={dialect} database={database} host={host}"

    def close(self) -> None:
        """Roll back any open transaction, close the session, and dispose the engine."""
        self.session.rollback()
        self.session.close()
        assert self.session.bind is not None
        self.session.bind.engine.dispose()

    def check_alembic_version(self) -> None:
        assert self.session.bind is not None
        engine = self.session.bind.engine
        inspector = sa.inspect(engine)
        # hide_password=False so we dont try to connect to a masked dsn
        controller = get_alembic_controller(
            engine.url.render_as_string(hide_password=False)
        )
        if not inspector.has_table("alembic_version"):
            raise ImproperlyConfigured(
                "Database schema version check failed because the table "
                "'alembic_version' does not exist. Run migrations or disable the "
                "check via check_alembic_version=False/"
                "IXMP4_CHECK_ALEMBIC_VERSION=false."
            )

        current_revision = controller.get_database_revision()
        head_revision = controller.get_head_revision()
        if head_revision is None:
            raise ImproperlyConfigured(
                "Could not determine the expected alembic "
                "revision from migration scripts."
            )

        if isinstance(head_revision, tuple):
            if len(head_revision) == 1:
                head_revision = head_revision[0]
            raise ImproperlyConfigured(
                "Could not determine a unique expected alembic revision because "
                f"multiple heads were found: {head_revision}."
            )

        if current_revision is None:
            raise ImproperlyConfigured(
                "Database schema version check failed because no alembic revision "
                "entry was found in 'alembic_version'. Run migrations or disable "
                "the check via check_alembic_version=False/"
                "IXMP4_CHECK_ALEMBIC_VERSION=false."
            )

        if current_revision != head_revision:
            revision_order = [script.revision for script in controller.list_revisions()]
            is_previous_migration = (
                current_revision in revision_order
                and head_revision in revision_order
                and revision_order.index(current_revision)
                > revision_order.index(head_revision)
            )

            if is_previous_migration:
                raise ImproperlyConfigured(
                    "Database schema version mismatch. "
                    f"Expected revision '{head_revision}' but found older "
                    f"revision '{current_revision}'. Upgrade the database to the "
                    "current ixmp4 migration head, or downgrade ixmp4 to a version "
                    "compatible with this database revision."
                )

            raise ImproperlyConfigured(
                "Database schema version mismatch. "
                f"Expected revision '{head_revision}' but found "
                f"'{current_revision}'. Upgrade your ixmp4 installation or disable the "
                "check via check_alembic_version=False/"
                "IXMP4_CHECK_ALEMBIC_VERSION=false."
            )

    def check_versioning_compatiblity(self) -> None:
        """Raise :exc:`~ixmp4.core.exceptions.OperationNotSupported` unless the
        underlying database is PostgreSQL.

        Versioning (row-level history tracking) relies on PostgreSQL-specific
        features and is not available on SQLite.
        """
        assert self.session.bind is not None
        if self.session.bind.engine.dialect.name != "postgresql":
            raise VersioningNotSupported()

    def __str__(self) -> str:
        return f"<{self.__class__.__name__} {self.get_engine_info()}>"


class AuthorizedTransport(DirectTransport):
    """A :class:`DirectTransport` decorated with authorisation context.

    This transport holds the current :class:`~toolkit.auth.context.AuthorizationContext`
    and exposes a separate ``unauthorized_transport`` that can be used for
    operations that must bypass permission checks.

    Attributes
    ----------
    platform:
        The platform being accessed, used for permission look-ups.
    auth_ctx:
        The current user's authorisation context.
    unauthorized_transport:
        A second :class:`DirectTransport` bound to the same session that skips
        authorisation checks.
    """

    platform: PlatformProtocol
    auth_ctx: AuthorizationContext
    unauthorized_transport: DirectTransport

    def __init__(
        self,
        session: orm.Session,
        auth_ctx: AuthorizationContext,
        platform: PlatformProtocol,
        ping_database: bool = True,
        check_alembic_version: bool = True,
    ):
        """Initialise the transport.

        Parameters
        ----------
        session:
            An already-configured SQLAlchemy ORM session.
        auth_ctx:
            The authorisation context for the current request.
        platform:
            The platform being accessed.
        ping_database:
            Forwarded to :meth:`DirectTransport.__init__`.
        check_alembic_version:
            Forwarded to :meth:`DirectTransport.__init__`.
        """
        super().__init__(
            session,
            ping_database=ping_database,
            check_alembic_version=check_alembic_version,
        )
        self.auth_ctx = auth_ctx
        self.platform = platform
        # turn off extra checks so they are not run twice
        self.unauthorized_transport = DirectTransport(
            session, ping_database=False, check_alembic_version=False
        )

    def __str__(self) -> str:
        return (
            f"<{self.__class__.__name__} {self.get_engine_info()} "
            f"user={self.auth_ctx.user} platform='{self.platform.slug}'>"
        )


class HttpxTransport(Transport, ServiceClient):
    """Transport that communicates with a remote ixmp4 server over HTTP.

    Attributes
    ----------
    http_client:
        The underlying HTTP client used to issue requests.
    settings:
        Client-side configuration (timeouts, concurrency, retries, …).
    executor:
        Thread-pool used for concurrent data-layer calls.
    direct:
        Optional :class:`DirectTransport` used for in-process test clients
        that need a live database session alongside the ASGI app.
    backoff_maximum:
        Upper bound (seconds) on the exponential back-off delay.  Default: 16.
    backoff_factor:
        Multiplier for the exponential back-off calculation.  Default: 0.5.
    backoff_exp_base:
        Base of the exponent used in back-off.  Default: 2.
    """

    http_client: httpx.Client | TestClient[Litestar]
    settings: ClientSettings
    executor: ThreadPoolExecutor
    exception_registry = exception_registry
    direct: DirectTransport | None = None

    backoff_maximum = 16.0
    backoff_factor = 0.5
    backoff_exp_base = 2.0

    def __init__(
        self,
        client: httpx.Client | TestClient[Litestar],
        settings: ClientSettings,
        check_root: bool = True,
    ):
        """Initialise the transport with an existing HTTP client.

        Parameters
        ----------
        client:
            A configured :class:`httpx.Client` or Litestar
            :class:`~litestar.testing.TestClient`.
        settings:
            Client configuration object.
        check_root:
            When ``True`` (the default), :meth:`check_root` is called
            immediately to verify connectivity and log server metadata.
        """
        self.url = str(client.base_url)
        logger.debug(f"Connected to IXMP4 http server at '{self.url}'.")

        self.settings = settings
        self.executor = ThreadPoolExecutor(max_workers=settings.concurrency)
        self.http_client = client

        if check_root:
            self.check_root()

    def check_root(self) -> None:
        """Verify connectivity and compatibility with the remote server.

        Sends a ``GET /`` request to the server's root endpoint, checks that
        the client and server ixmp4 versions match, and validates that the
        manager URL configured on the server matches the one used by the client
        (when :class:`~toolkit.client.auth.ManagerAuth` is in use).

        Raises
        ------
        :exc:`~ixmp4.base_exceptions.ImproperlyConfigured`
            If the manager URLs on the client and server disagree.
        """
        from ixmp4.server.v1.platform import PlatformInfo

        res = self.request("GET", "/")
        self.raise_service_exception(res)
        root = PlatformInfo(**res.json())

        if __version__ != root.version:
            logger.warning(
                "IXMP4 Client and Server versions do not match. "
                f"(Client: {__version__}, Server: {root.version})"
            )

        logger.debug("Server UTC Time: " + root.utcnow.strftime("%c"))

        if (
            isinstance(self.http_client.auth, ManagerAuth)
            and root.manager_url is not None
        ):
            client_manager_url = str(self.http_client.auth.client.base_url)
            if client_manager_url.rstrip("/") != str(root.manager_url).rstrip("/"):
                logger.error(f"Server Manager URL: {root.manager_url}")
                logger.error(f"Client Manager URL: {client_manager_url}")
                raise ImproperlyConfigured(
                    "Trying to connect to a managed http Platform "
                    "with a mismatching Manager URL."
                )

    @classmethod
    def from_url(
        cls, url: str, settings: ClientSettings | None = None, auth: Auth | None = None
    ) -> "HttpxTransport":
        """Create an :class:`HttpxTransport` from a plain URL string.

        Configures an :class:`httpx.Client` with HTTP/2, the timeout from
        *settings*, and optionally a :class:`~toolkit.client.auth.SelfSignedAuth`
        handler when ``settings.secret_hs256`` is set.

        Parameters
        ----------
        url:
            Base URL of the remote ixmp4 server (e.g. ``https://host/v1/myplatform/``).
        settings:
            Client configuration.  Falls back to the default
            :class:`~ixmp4.conf.settings.Settings` when ``None``.
        auth:
            Authentication handler to attach to the client.  When ``None`` and
            ``settings.secret_hs256`` is set, a
            :class:`~toolkit.client.auth.SelfSignedAuth` is created automatically.

        Returns
        -------
        HttpxTransport
            A transport instance connected to *url*.
        """
        if settings is None:
            settings = Settings().client

        timeout = httpx.Timeout(settings.timeout, connect=10.0)

        if settings.secret_hs256 is not None and auth is None:
            logger.info(
                "Found `secret_hs256` in client settings, using self-signed auth."
            )
            auth = SelfSignedAuth(
                settings.secret_hs256.get_secret_value(), issuer="ixmp4"
            )

        client = httpx.Client(
            base_url=url,
            timeout=timeout,
            http2=True,
            auth=auth,
            transport=httpx.HTTPTransport(retries=settings.retries, http2=True),
        )
        return cls(client, settings)

    @classmethod
    def from_asgi(
        cls,
        asgi: Litestar,
        settings: ClientSettings,
        direct: DirectTransport | None = None,
        raise_server_exceptions: bool = True,
    ) -> "HttpxTransport":
        """Create an :class:`HttpxTransport` backed by an in-process ASGI app.

        Used in the test suite to exercise the full HTTP stack without a real
        network connection. The root-check is intentionally skipped because
        the ASGI test client is not yet live at construction time.

        Parameters
        ----------
        asgi:
            A fully constructed :class:`~litestar.Litestar` application.
        settings:
            Client configuration object.
        direct:
            An optional :class:`DirectTransport` to attach as
            ``transport.direct`` — useful when tests need both HTTP and
            direct-database access.
        raise_server_exceptions:
            Forwarded to :class:`~litestar.testing.TestClient`.  When
            ``True``, server-side exceptions propagate to the test.

        Returns
        -------
        HttpxTransport
            A transport instance connected to the ASGI app.
        """
        client = TestClient(
            app=asgi,
            base_url="http://testserver.local/v1/direct/",
            raise_server_exceptions=raise_server_exceptions,
        )
        transport = cls(client, settings, check_root=False)
        transport.direct = direct
        return transport

    def __str__(self) -> str:
        if (
            isinstance(self.http_client.auth, ManagerAuth)
            and self.http_client.auth.access_token.user is not None
        ):
            user = self.http_client.auth.access_token.user
        else:
            user = None
        return f"<HttpxTransport base_url={self.http_client.base_url} user={user}>"

    def request(self, method: str, path: str, **kwargs: Any) -> httpx.Response:
        """Issue an HTTP request and retry automatically on ``HTTP 429``.

        Retries up to ``settings.retries`` times. The delay between attempts
        is determined by :meth:`get_retry_delay_seconds`.

        Parameters
        ----------
        method:
            HTTP method string (e.g. ``"GET"``, ``"POST"``).
        path:
            Path relative to the client's base URL.
        **kwargs:
            Additional keyword arguments forwarded to
            :meth:`httpx.Client.request`.

        Returns
        -------
        httpx.Response
            The first non-429 response, or the last response if the retry
            budget is exhausted.
        """
        max_retries = self.settings.retries
        for attempt in range(max_retries + 1):
            response = self.http_client.request(method, path, **kwargs)
            if response.status_code != 429 or attempt >= max_retries:
                return response

            delay = self.get_retry_delay_seconds(response, attempt)
            logger.warning(
                f"Rate limited (429) for {method} {path}. "
                f"Retrying in {delay:.2f}s ({attempt + 1}/{max_retries})."
            )
            time.sleep(delay)

        raise AssertionError("Unreachable retry loop termination")

    def get_retry_delay_seconds(self, response: httpx.Response, attempt: int) -> float:
        """Calculate the retry delay for a rate-limited response.

        Honours the ``Retry-After`` response header when present (both numeric
        seconds and HTTP-date formats are supported). Falls back to
        exponential back-off:
        ``min(backoff_maximum, backoff_factor * backoff_exp_base ** attempt)``.

        Parameters
        ----------
        response:
            The ``HTTP 429`` response whose headers may contain ``Retry-After``.
        attempt:
            The current retry attempt, used to compute the exponential back-off.

        Returns
        -------
        float
            Seconds to wait before the next attempt (always ≥ 0).
        """
        retry_after = response.headers.get("retry-after")
        if retry_after:
            retry_after = retry_after.strip()

            try:
                return max(0.0, float(retry_after))
            except ValueError:
                pass

            try:
                # parses a http data which is in-spec for Retry-After
                retry_dt = parsedate_to_datetime(retry_after)
                if retry_dt is not None:
                    if retry_dt.tzinfo is None:
                        retry_dt = retry_dt.replace(tzinfo=dt.timezone.utc)

                    now = dt.datetime.now(tz=dt.timezone.utc)
                    return max(0.0, (retry_dt - now).total_seconds())
            except (TypeError, ValueError, OverflowError):
                pass

        return float(
            min(
                self.backoff_maximum,
                self.backoff_factor * (self.backoff_exp_base**attempt),
            )
        )
