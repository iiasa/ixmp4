import json
import logging
import logging.config
import sys
from pathlib import Path
from typing import Any, Literal

from dotenv import load_dotenv
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
    default_upload_chunk_size: int = 10_000
    concurrency: int = Field(2, le=4)
    retries: int = Field(3)
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

    def get_toml_platforms(self) -> TomlPlatforms | None:
        if self.toml_platforms is None:
            return None
        if not self.toml_platforms.exists():
            return None
        return TomlPlatforms(self.toml_platforms)


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
        env_nested_delimiter="__",
        env_file=".env",
        env_file_encoding="utf-8",
    )

    @model_validator(mode="after")
    def setup(self) -> "Settings":
        load_dotenv()
        self.setup_directories()

        if self.is_in_interactive_mode():
            self.configure_logging(self.mode)

        if self.server.toml_platforms is None:
            self.server.toml_platforms = self.get_toml_platforms_path()

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

    def load_logging_config(self, config: str) -> Any:
        logging_config = here / f"logging/{config}.json"
        with open(logging_config) as file:
            return json.load(file)

    def configure_logging(self, config: str) -> None:
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
                logger.info(
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
