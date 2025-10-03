"""
Auth modalities:
    Server/SS: SelfSigned(secret_hs256)
    Server/SS/Impersonate: ImpersonatingAuth(secret_hs256, user_id)

    Lib/Local
"""

import json
import logging
import logging.config
from pathlib import Path
from typing import Any, Literal

from httpx import ConnectError
from pydantic import Field, HttpUrl, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from toolkit.client.auth import ManagerAuth, SelfSignedAuth
from toolkit.exceptions import InvalidCredentials
from toolkit.manager import ManagerClient

from ixmp4 import __file__ as __root__file__

from .credentials import Credentials
from .platforms import TomlPlatforms

logger = logging.getLogger(__name__)

here = Path(__file__).parent
root = Path(__root__file__).parent.parent


class Settings(BaseSettings):
    mode: Literal["production"] | Literal["development"] | Literal["debug"] = (
        "production"
    )
    storage_directory: Path = Field(Path("~/.local/share/ixmp4/"))
    secret_hs256: str | None = None
    manager_url: HttpUrl = Field(HttpUrl("https://api.manager.ece.iiasa.ac.at/v1"))

    # deprecated
    managed: bool | None = None

    migration_db_uri: str = "sqlite:///./run/db.sqlite"

    max_page_size: int = 10_000
    default_page_size: int = 5_000

    client_default_upload_chunk_size: int = 10_000
    client_max_concurrent_requests: int = Field(2, le=4)
    client_max_request_retries: int = Field(3)
    client_backoff_factor: int = Field(5)
    client_timeout: int = Field(30)

    model_config = SettingsConfigDict(env_prefix="ixmp4_", extra="allow")

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)

        self.setup_directories()

        credentials_config = self.storage_directory / "credentials.toml"
        credentials_config.touch()
        self.credentials = Credentials(credentials_config)

        logger.debug(f"Settings loaded: {self}")

    def setup_directories(self) -> None:
        self.storage_directory.mkdir(parents=True, exist_ok=True)

        self.database_dir = self.storage_directory / "databases"
        self.database_dir.mkdir(exist_ok=True)

        self.log_dir = self.storage_directory / "log"
        self.log_dir.mkdir(exist_ok=True)

    def get_default_auth(self) -> ManagerAuth | None:
        default_credentials = self.credentials.get("default")
        if default_credentials is not None:
            try:
                logger.info(f"Connecting as user '{default_credentials['username']}'.")
                return ManagerAuth(
                    default_credentials["username"],
                    default_credentials["password"],
                    str(self.manager_url),
                )
            except InvalidCredentials:
                logger.warning(f"Invalid credentials for {self.manager_url}.")
            except ConnectError:
                logger.warning(f"Unable to connect to {self.manager_url}.")
        return None

    def get_self_signed_auth(self) -> SelfSignedAuth:
        return SelfSignedAuth(self.secret_hs256, issuer="ixmp4")

    def load_manager_client(self) -> ManagerClient:
        auth = self.get_default_auth()
        return ManagerClient(str(self.manager_url), auth)

    def load_self_signed_manager_client(self) -> ManagerClient:
        auth = self.get_self_signed_auth()
        return ManagerClient(str(self.manager_url), auth)

    def load_toml_platforms(self) -> TomlPlatforms:
        toml_config = self.storage_directory / "platforms.toml"
        toml_config.touch()
        return TomlPlatforms(toml_config)

    @field_validator("storage_directory")
    def validate_storage_dir(cls, v: Path) -> Path:
        # translate ~/asdf into /home/user/asdf
        v = Path.expanduser(v)

        # handle dev setup paths like ./run/foo
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
