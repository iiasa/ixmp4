import os
import logging
import logging.config
from typing import Literal
from pathlib import Path
from httpx import ConnectError

from pydantic import BaseSettings, Field, validator, HttpUrl, Extra

from ixmp4.core.exceptions import InvalidCredentials
from .credentials import Credentials
from .toml import TomlConfig
from .manager import ManagerConfig
from .auth import ManagerAuth, AnonymousAuth
from .user import local_user

logger = logging.getLogger(__name__)

here = Path(__file__).parent


class Settings(BaseSettings):
    mode: Literal["production"] | Literal["development"] | Literal[
        "debug"
    ] = "production"
    storage_directory: Path = Field("~/.local/share/ixmp4/", env="ixmp4_dir")
    secret_hs256: str = "default_secret_hs256"
    migration_db_uri: str = "sqlite:///./run/db.sqlite"
    manager_url: HttpUrl = Field("https://api.manager.ece.iiasa.ac.at/v1")
    managed: bool = True

    class Config:
        env_prefix = "ixmp4_"
        extra = Extra.allow

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        self.storage_directory.mkdir(parents=True, exist_ok=True)

        self.database_dir = self.storage_directory / "databases"
        self.database_dir.mkdir(exist_ok=True)

        self.log_dir = self.storage_directory / "log"
        self.log_dir.mkdir(exist_ok=True)

        self.configure_logging(self.mode)

        self.load_credentials()
        self.load_manager_config()
        self.load_toml_config()

    def load_credentials(self):
        credentials_config = self.storage_directory / "credentials.toml"
        credentials_config.touch()
        self.credentials = Credentials(credentials_config)

        self.default_credentials = None
        self.default_auth = None
        try:
            self.default_credentials = self.credentials.get("default")
        except KeyError:
            logger.warn("No default credentials provided.")

        if self.default_credentials is not None:
            username, password = self.default_credentials
            try:
                self.default_auth = ManagerAuth(username, password, self.manager_url)
                return
            except InvalidCredentials:
                logger.warn(
                    "Failure while requesting management service authentication: Invalid credentials."
                )
            except ConnectError:
                logger.warn(f"Unable to connect to {self.manager_url}.")

        self.default_auth = AnonymousAuth()

    def load_manager_config(self):
        self.manager = None
        if self.default_auth is not None:
            self.manager = ManagerConfig(
                self.manager_url, self.default_auth, remote=True
            )

    def load_toml_config(self):
        toml_user = self.default_auth.get_user()
        if not toml_user.is_authenticated:
            toml_user = local_user

        toml_config = self.storage_directory / "platforms.toml"
        toml_config.touch()
        self.toml = TomlConfig(toml_config, toml_user)

    @validator("storage_directory")
    def expand_user(cls, v):
        # translate ~/asdf into /home/user/asdf
        return Path.expanduser(v)

    def configure_logging(self, config: str):
        access_file = self.log_dir / "access.log"
        debug_file = self.log_dir / "debug.log"
        error_file = self.log_dir / "error.log"
        os.environ.setdefault("IXMP4_ACCESS_LOG", str(access_file.absolute()))
        os.environ.setdefault("IXMP4_DEBUG_LOG", str(debug_file.absolute()))
        os.environ.setdefault("IXMP4_ERROR_LOG", str(error_file.absolute()))

        logging_config = here / f"logging/{config}.conf"
        logging.config.fileConfig(logging_config, disable_existing_loggers=False)

    def check_credentials(self):
        if self.default_credentials is not None:
            username, password = self.default_credentials
            ManagerAuth(username, password, self.manager_url)
