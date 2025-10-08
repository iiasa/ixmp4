import json
import logging
import logging.config
import sys
from pathlib import Path
from typing import Any, Literal

from pydantic import Field, HttpUrl, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from toolkit.client.auth import ManagerAuth, SelfSignedAuth
from toolkit.manager.client import ManagerClient

from ixmp4 import __file__ as __root__file__

from .credentials import Credentials
from .platforms import ManagerPlatforms, TomlPlatforms

logger = logging.getLogger(__name__)

here = Path(__file__).parent
root = Path(__root__file__).parent.parent

try:
    __IPYTHON__  # type: ignore
    _in_ipython_session = True
except NameError:
    _in_ipython_session = False

_sys_has_ps1 = hasattr(sys, "ps1")


class Settings(BaseSettings):
    mode: Literal["production"] | Literal["development"] | Literal["debug"] = (
        "production"
    )
    storage_directory: Path = Field(Path("~/.local/share/ixmp4/"))

    secret_hs256: str | None = None
    migration_db_uri: str = "sqlite:///./run/db.sqlite"
    manager_url: HttpUrl = Field(HttpUrl("https://api.manager.ece.iiasa.ac.at/v1"))

    # deprecated
    managed: bool | None = None

    max_page_size: int = 10_000
    default_page_size: int = 5_000
    client_default_upload_chunk_size: int = 10_000
    client_max_concurrent_requests: int = Field(2, le=4)
    client_max_request_retries: int = Field(3)
    client_backoff_factor: int = Field(5)
    client_timeout: int = Field(30)

    model_config = SettingsConfigDict(env_prefix="ixmp4_", extra="allow")

    # We don't pass any args or kwargs, so allow all to flow through
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)

        self.setup_directories()

        if self.is_in_interactive_mode():
            self.configure_logging(self.mode)

        logger.debug(f"Settings loaded: {self}")

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

    def get_client_auth(self) -> ManagerAuth | SelfSignedAuth | None:
        # TODO: Log Messages...
        if self.secret_hs256 is not None:
            logger.debug(
                "Using self-signed http authentication strategy because the"
                "environment variable `IXMP4_SECRET_HS256` is set."
            )
            return SelfSignedAuth(self.secret_hs256, issuer="ixmp4")
        else:
            credentials = self.get_credentials()
            default_creds = credentials.get("default")
            if default_creds is None:
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
                return ManagerAuth(
                    default_creds["username"],
                    default_creds["password"],
                    str(self.manager_url),
                )

    def get_manager_client(self) -> ManagerClient:
        return ManagerClient(str(self.manager_url), self.get_client_auth())

    def get_manager_platforms(self) -> ManagerPlatforms:
        return ManagerPlatforms(self.get_manager_client())

    def setup_directories(self) -> None:
        self.storage_directory.mkdir(parents=True, exist_ok=True)

        self.database_dir = self.storage_directory / "databases"
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
            v = root / v

        return v

    def get_server_logconf(self) -> Path:
        return here / "./logging/server.json"

    def configure_logging(self, config: str) -> None:
        self.access_file = str((self.log_dir / "access.log").absolute())
        self.debug_file = str((self.log_dir / "debug.log").absolute())
        self.error_file = str((self.log_dir / "error.log").absolute())

        logging_config = here / f"logging/{config}.json"
        with open(logging_config) as file:
            config_dict = json.load(file)
        logging.config.dictConfig(config_dict)
