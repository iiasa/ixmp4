import json
import logging
import logging.config
import sys
from pathlib import Path
from typing import Any, Literal

from pydantic import Field, HttpUrl, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from toolkit.client.auth import ManagerAuth, SelfSignedAuth
from toolkit.manager.client import ManagerClient

from .credentials import Credentials, CredentialsDict
from .platforms import ManagerPlatforms, TomlPlatforms

logger = logging.getLogger(__name__)

here = Path(__file__).parent

try:
    __IPYTHON__  # type: ignore
    _in_ipython_session = True
except NameError:
    _in_ipython_session = False

_sys_has_ps1 = hasattr(sys, "ps1")


class ClientSettings(BaseSettings):
    """Client-side runtime settings.


    Attributes
    ----------

    default_upload_chunk_size: int
        Number of records uploaded per client chunk (currently unused).
        Environment variable: ``IXMP4_CLIENT__DEFAULT_UPLOAD_CHUNK_SIZE``.
    concurrency: int
        Maximum number of concurrent client workers.
        Environment variable: ``IXMP4_CLIENT__CONCURRENCY``.
    retries: int
        Number of retry attempts for retryable client requests.
        Environment variable: ``IXMP4_CLIENT__RETRIES``.
    timeout: int
        Request timeout in seconds for client-side HTTP operations.
        Environment variable: ``IXMP4_CLIENT__TIMEOUT``.
    secret_hs256: int
        Shared secret used for self-signed client authentication.
        Environment variable: ``IXMP4_CLIENT__SECRET_HS256``.
    """

    default_upload_chunk_size: int = Field(
        10_000,
        description=(
            "Number of records uploaded per client chunk (currently unused). "
            "Environment variable: IXMP4_CLIENT__DEFAULT_UPLOAD_CHUNK_SIZE."
        ),
    )
    concurrency: int = Field(
        2,
        le=4,
        description=(
            "Maximum number of concurrent client workers. "
            "Environment variable: IXMP4_CLIENT__CONCURRENCY."
        ),
    )
    retries: int = Field(
        3,
        description=(
            "Number of retry attempts for retryable client requests. "
            "Environment variable: IXMP4_CLIENT__RETRIES."
        ),
    )
    timeout: int = Field(
        30,
        description=(
            "Request timeout in seconds for client-side HTTP operations. "
            "Environment variable: IXMP4_CLIENT__TIMEOUT."
        ),
    )
    secret_hs256: SecretStr | None = Field(
        None,
        description=(
            "Shared secret used for self-signed client authentication. "
            "Environment variable: IXMP4_CLIENT__SECRET_HS256."
        ),
    )


class ServerSettings(BaseSettings):
    """Server-side runtime settings.

    Attributes
    ----------

    manager_url: HttpUrl | None
        Manager service base URL used by the server for outbound calls.
        Environment variable: ``IXMP4_SERVER__MANAGER_URL``.
    toml_platforms: Path | None
        Path to the TOML file that stores configured platforms.
        Environment variable: ``IXMP4_SERVER__TOML_PLATFORMS``.
    secret_hs256: SecretStr | None
        Shared secret used for self-signed server authentication.
        Environment variable: ``IXMP4_SERVER__SECRET_HS256``.
    max_page_size: int
        Hard upper bound for paginated API responses.
        Environment variable: ``IXMP4_SERVER__MAX_PAGE_SIZE``.
    default_page_size: int
        Default number of items returned by paginated responses.
        Environment variable: ``IXMP4_SERVER__DEFAULT_PAGE_SIZE``.
    log_exceptions: Literal["never", "always", "debug"]
        Whether to write exception tracebacks to the server log.
        Environment variable: ``IXMP4_SERVER__LOG_EXCEPTIONS``.
    """

    manager_url: HttpUrl | None = Field(
        None,
        description=(
            "Manager service base URL used by the server for outbound calls. "
            "Environment variable: IXMP4_SERVER__MANAGER_URL."
        ),
    )
    toml_platforms: Path | None = Field(
        None,
        description=(
            "Path to the TOML file that stores configured platforms. "
            "Environment variable: IXMP4_SERVER__TOML_PLATFORMS."
        ),
    )
    secret_hs256: SecretStr | None = Field(
        None,
        description=(
            "Shared secret used for self-signed server authentication. "
            "Environment variable: IXMP4_SERVER__SECRET_HS256."
        ),
    )

    max_page_size: int = Field(
        10_000,
        description=(
            "Hard upper bound for paginated API responses. "
            "Environment variable: IXMP4_SERVER__MAX_PAGE_SIZE."
        ),
    )
    default_page_size: int = Field(
        5_000,
        description=(
            "Default number of items returned by paginated responses. "
            "Environment variable: IXMP4_SERVER__DEFAULT_PAGE_SIZE."
        ),
    )
    log_exceptions: Literal["never", "always", "debug"] = Field(
        "always",
        description=(
            "Whether to write exception tracebacks to the server log. "
            "Environment variable: IXMP4_SERVER__LOG_EXCEPTIONS."
        ),
    )

    @model_validator(mode="after")
    def setup(self) -> "ServerSettings":
        """Validate server paging configuration after model initialization."""
        if self.default_page_size > self.max_page_size:
            raise ValueError(
                "Default page size must be smaller or equal to maximum page size. ",
                f"{self.default_page_size} > {self.max_page_size}",
            )
        return self

    def get_self_signed_auth(self, secret_hs256: SecretStr) -> SelfSignedAuth:
        """Build a self-signed authentication strategy for server requests."""
        return SelfSignedAuth(secret_hs256.get_secret_value(), issuer="ixmp4")

    def get_manager_client(
        self, manager_url: HttpUrl, secret_hs256: SecretStr
    ) -> ManagerClient:
        """Create a manager API client using server self-signed authentication."""
        return ManagerClient(str(manager_url), self.get_self_signed_auth(secret_hs256))

    def get_toml_platforms(self) -> TomlPlatforms | None:
        """Load server platform configuration from TOML when available."""
        if self.toml_platforms is None:
            return None
        if not self.toml_platforms.exists():
            return None
        return TomlPlatforms(self.toml_platforms)


class Settings(BaseSettings):
    """Top-level ixmp4 application settings.

    All settings use the ``IXMP4_`` prefix. Nested settings use ``__`` as a
    delimiter, for example ``IXMP4_SERVER__DEFAULT_PAGE_SIZE``.


    Attributes
    ----------

    mode: Runtime mode used to select logging configuration.
        Environment variable: ``IXMP4_MODE``.
    storage_directory: Base directory for local ixmp4 state and generated
        files.
        Environment variable: ``IXMP4_STORAGE_DIRECTORY``.
    manager_url: Default manager service base URL used by the client.
        Environment variable: ``IXMP4_MANAGER_URL``.
    check_alembic_version: Whether to verify the local Alembic migration
        version before using the database.
        Environment variable: ``IXMP4_CHECK_ALEMBIC_VERSION``.
    server: Nested server configuration namespace.
        Environment variables: ``IXMP4_SERVER__*``.
    client: Nested client configuration namespace.
        Environment variables: ``IXMP4_CLIENT__*``.
    """

    mode: Literal["production"] | Literal["development"] | Literal["debug"] = Field(
        "production",
        description=(
            "Runtime mode used to select logging configuration. "
            "Environment variable: IXMP4_MODE."
        ),
    )
    storage_directory: Path = Field(
        Path("~/.local/share/ixmp4/"),
        description=(
            "Base directory for local ixmp4 state and generated files. "
            "Environment variable: IXMP4_STORAGE_DIRECTORY."
        ),
    )
    manager_url: HttpUrl = Field(
        HttpUrl("https://api.manager.ece.iiasa.ac.at/v1"),
        description=(
            "Default manager service base URL used by the client. "
            "Environment variable: IXMP4_MANAGER_URL."
        ),
    )
    check_alembic_version: bool = Field(
        True,
        description=(
            "Whether to verify the local Alembic migration version before "
            "using the database. Environment variable: "
            "IXMP4_CHECK_ALEMBIC_VERSION."
        ),
    )

    server: ServerSettings = Field(
        default_factory=ServerSettings,
        description=(
            "Nested server configuration namespace. "
            "Environment variables: IXMP4_SERVER__*."
        ),
    )
    client: ClientSettings = Field(
        default_factory=ClientSettings,
        description=(
            "Nested client configuration namespace. "
            "Environment variables: IXMP4_CLIENT__*."
        ),
    )

    model_config = SettingsConfigDict(
        env_prefix="ixmp4_",
        extra="allow",
        env_nested_delimiter="__",
        env_file=".env",
        env_file_encoding="utf-8",
    )

    @model_validator(mode="after")
    def setup(self) -> "Settings":
        """Finalize settings by creating paths, logging, and default platform file."""
        self.setup_directories()

        if self.is_in_interactive_mode():
            self.configure_logging(self.mode)

        if self.server.toml_platforms is None:
            self.server.toml_platforms = self.get_toml_platforms_path()

        return self

    def is_in_interactive_mode(self) -> bool:
        """Return ``True`` when running in a REPL or IPython-like session."""
        return _sys_has_ps1 or _in_ipython_session

    def get_credentials_path(self) -> Path:
        """Return the path of the local credentials TOML file."""
        return self.storage_directory / "credentials.toml"

    def get_credentials(self) -> Credentials:
        """Ensure and return the credentials store wrapper for local credentials."""
        credentials_config = self.get_credentials_path()
        credentials_config.touch()
        return Credentials(credentials_config)

    def get_toml_platforms_path(self) -> Path:
        """Return the path of the local platform configuration TOML file."""
        return self.storage_directory / "platforms.toml"

    def get_toml_platforms(self) -> TomlPlatforms:
        """Ensure and return the local TOML-backed platform registry."""
        platform_config = self.get_toml_platforms_path()
        platform_config.touch()
        return TomlPlatforms(platform_config)

    def setup_directories(self) -> None:
        """Create storage, database, and log directories if they do not exist."""
        self.storage_directory.mkdir(parents=True, exist_ok=True)

        self.database_dir = self.get_database_dir()
        self.database_dir.mkdir(exist_ok=True)

        self.log_dir = self.storage_directory / "log"
        self.log_dir.mkdir(exist_ok=True)

    @field_validator("storage_directory")
    @classmethod
    def validate_storage_dir(cls, v: Path) -> Path:
        """Normalize storage path by expanding user home and relative paths."""
        # translate ~/asdf into /home/user/asdf
        v = Path.expanduser(v)

        # handle relative dev paths
        if not v.is_absolute():
            v = Path.cwd() / v

        return v

    def load_logging_config(self, config: str) -> Any:
        """Load a JSON logging configuration by name from ``conf/logging``."""
        logging_config = here / f"logging/{config}.json"
        with open(logging_config) as file:
            return json.load(file)

    def configure_logging(self, config: str) -> None:
        """Configure process logging and set concrete log file destinations."""
        self.access_file = str((self.log_dir / "access.log").absolute())
        self.debug_file = str((self.log_dir / "debug.log").absolute())
        self.error_file = str((self.log_dir / "error.log").absolute())

        logging.config.dictConfig(self.load_logging_config(config))

    def get_database_dir(self) -> Path:
        """Returns the path to the local sqlite database directory."""
        return self.storage_directory / "databases"

    def get_database_path(self, name: str) -> Path:
        """Returns a :class:`Path` object for a given sqlite database name.
        Does not check whether or not the file actually exists."""

        file_name = name + ".sqlite3"
        return self.get_database_dir() / file_name

    def get_client_auth(
        self, credentials: CredentialsDict | None
    ) -> ManagerAuth | SelfSignedAuth | None:
        """Select the client authentication strategy from settings and credentials."""
        if self.client.secret_hs256 is not None:
            logger.info(
                "Using self-signed http authentication strategy because the"
                "environment variable `IXMP4_CLIENT__SECRET_HS256` is set."
            )
            return self.get_self_signed_auth(self.client.secret_hs256)
        else:
            if credentials is None:
                logger.info(
                    "Using anonymous http authentication strategy "
                    "because no local credentials were found."
                )
                return None
            else:
                logger.debug(
                    "Using manager http authentication strategy "
                    "because local credentials were found."
                )
                return self.get_manager_auth(self.manager_url, credentials)

    def get_self_signed_auth(self, secret_hs256: SecretStr) -> SelfSignedAuth:
        """Create a self-signed authentication object for API communication."""
        return SelfSignedAuth(secret_hs256.get_secret_value(), issuer="ixmp4")

    def get_manager_auth(
        self, manager_url: HttpUrl, credentials: CredentialsDict
    ) -> ManagerAuth | None:
        """Create manager authentication from username/password credentials."""
        return ManagerAuth(
            credentials["username"],
            credentials["password"],
            str(manager_url),
        )

    def get_manager_client(self, credentials: str = "default") -> ManagerClient:
        """Build a manager client using a named local credential entry."""
        cred_dict = self.get_credentials().get(credentials)
        return ManagerClient(str(self.manager_url), self.get_client_auth(cred_dict))

    def get_manager_platforms(self, credentials: str = "default") -> ManagerPlatforms:
        """Return a manager-backed platform registry for a credential profile."""
        return ManagerPlatforms(self.get_manager_client(credentials=credentials))
