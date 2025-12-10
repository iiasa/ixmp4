import json
import logging
import logging.config
import sys
from pathlib import Path
from typing import Literal

from pydantic import Field, HttpUrl, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from toolkit.auth.context import AuthorizationContext
from toolkit.auth.user import ServiceAccount, User
from toolkit.client.auth import Auth, ManagerAuth, SelfSignedAuth
from toolkit.manager.client import ManagerClient

from ixmp4.base_exceptions import ProgrammingError

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
    default_upload_chunk_size: int = 10_000
    max_concurrent_requests: int = Field(2, le=4)
    max_request_retries: int = Field(3)
    backoff_factor: int = Field(5)
    timeout: int = Field(30)
    secret_hs256: SecretStr | None = None


class ServerSettings(BaseSettings):
    manager_url: HttpUrl | None = None
    toml_platforms: Path | None = None
    secret_hs256: SecretStr | None = None

    max_page_size: int = 10_000
    default_page_size: int = 5_000

    def get_self_signed_auth(self, secret_hs256: SecretStr) -> SelfSignedAuth:
        return SelfSignedAuth(secret_hs256.get_secret_value(), issuer="ixmp4")

    def get_manager_client(
        self, manager_url: HttpUrl, secret_hs256: SecretStr
    ) -> ManagerClient:
        return ManagerClient(str(manager_url), self.get_self_signed_auth(secret_hs256))

    def get_toml_platforms(self, toml_platforms: Path) -> TomlPlatforms:
        return TomlPlatforms(toml_platforms)


class Settings(BaseSettings):
    mode: Literal["production"] | Literal["development"] | Literal["debug"] = (
        "production"
    )
    storage_directory: Path = Path("~/.local/share/ixmp4/")
    manager_url: HttpUrl = HttpUrl("https://api.manager.ece.iiasa.ac.at/v1")

    server: ServerSettings = ServerSettings()
    client: ClientSettings = ClientSettings()

    model_config = SettingsConfigDict(
        env_prefix="ixmp4_",
        extra="allow",
        env_file=".env",
        env_file_encoding="utf-8",
    )

    @model_validator(mode="after")
    def setup(self) -> "Settings":
        self.setup_directories()

        if self.is_in_interactive_mode():
            self.configure_logging(self.mode)

        return self

    def is_in_interactive_mode(self) -> bool:
        return _sys_has_ps1 or _in_ipython_session

    def get_credentials_path(self) -> Path:
        return self.storage_directory / "credentials.toml"

    def get_credentials(self) -> Credentials:
        credentials_config = self.get_credentials_path()
        credentials_config.touch()
        return Credentials(credentials_config)

    def get_toml_platforms_path(self) -> Path:
        return self.storage_directory / "platforms.toml"

    def get_toml_platforms(self) -> TomlPlatforms:
        platform_config = self.get_toml_platforms_path()
        platform_config.touch()
        return TomlPlatforms(platform_config)

    def setup_directories(self) -> None:
        self.storage_directory.mkdir(parents=True, exist_ok=True)

        self.database_dir = self.get_database_dir()
        self.database_dir.mkdir(exist_ok=True)

        self.log_dir = self.storage_directory / "log"
        self.log_dir.mkdir(exist_ok=True)

    @field_validator("storage_directory")
    @classmethod
    def validate_storage_dir(cls, v: Path) -> Path:
        # translate ~/asdf into /home/user/asdf
        v = Path.expanduser(v)

        # handle relative dev paths
        if not v.is_absolute():
            v = Path.cwd() / v

        return v

    def configure_logging(self, config: str) -> None:
        self.access_file = str((self.log_dir / "access.log").absolute())
        self.debug_file = str((self.log_dir / "debug.log").absolute())
        self.error_file = str((self.log_dir / "error.log").absolute())

        logging_config = here / f"logging/{config}.json"
        with open(logging_config) as file:
            config_dict = json.load(file)
        logging.config.dictConfig(config_dict)

    def get_database_dir(self) -> Path:
        """Returns the path to the local sqlite database directory."""
        return self.storage_directory / "databases"

    def get_database_path(self, name: str) -> Path:
        """Returns a :class:`Path` object for a given sqlite database name.
        Does not check whether or not the file actually exists."""

        file_name = name + ".sqlite3"
        return self.get_database_dir() / file_name

    def get_manager_user(
        self, manager_client: ManagerClient
    ) -> User | ServiceAccount | None:
        default_auth = manager_client.auth
        if default_auth is None or not isinstance(default_auth, Auth):
            user = None
        elif (
            getattr(default_auth, "access_token", None) is None
            or default_auth.access_token is None
        ):
            user = None
        else:
            user = default_auth.access_token.user
        return user

    def get_local_user(self, credentials: str = "default") -> User:
        cred_dict = self.get_credentials().get(credentials)
        if cred_dict is not None:
            username = cred_dict["username"]
        else:
            username = "@unknown"

        return User(id=-1, username=username, email="", groups=[], is_superuser=True)

    def get_manager_auth_context(
        self, credentials: str = "default"
    ) -> AuthorizationContext:
        manager_client = self.get_manager_client(credentials=credentials)
        user = self.get_manager_user(manager_client)

        if isinstance(user, ServiceAccount):
            raise ProgrammingError(
                "Cannot make `AuthorizationContext` with `ServiceAccount` as `user`."
            )
        return AuthorizationContext(user, manager_client)

    def get_client_auth(
        self, credentials: CredentialsDict | None
    ) -> ManagerAuth | SelfSignedAuth | None:
        if self.client.secret_hs256 is not None:
            logger.debug(
                "Using self-signed http authentication strategy because the"
                "environment variable `IXMP4_CLIENT__SECRET_HS256` is set."
            )
            return self.get_self_signed_auth(self.client.secret_hs256)
        else:
            if credentials is None:
                logger.debug(
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
        return SelfSignedAuth(secret_hs256.get_secret_value(), issuer="ixmp4")

    def get_manager_auth(
        self, manager_url: HttpUrl, credentials: CredentialsDict
    ) -> ManagerAuth | None:
        return ManagerAuth(
            credentials["username"],
            credentials["password"],
            str(manager_url),
        )

    def get_manager_client(self, credentials: str = "default") -> ManagerClient:
        cred_dict = self.get_credentials().get(credentials)
        return ManagerClient(str(self.manager_url), self.get_client_auth(cred_dict))

    def get_manager_platforms(self, credentials: str = "default") -> ManagerPlatforms:
        return ManagerPlatforms(self.get_manager_client(credentials=credentials))
