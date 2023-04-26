from dotenv import load_dotenv
from pathlib import Path

from pydantic import BaseSettings, Field, validator, HttpUrl, Extra

from ixmp4.core.exceptions import InvalidCredentials
from .credentials import Credentials
from .toml import TomlConfig
from .manager import ManagerConfig
from .auth import ManagerAuth
from .user import local_user
from .base import PlatformInfo as PlatformInfo


class Settings(BaseSettings):
    mode: str = "production"
    storage_directory: Path = Field("~/.local/share/ixmp4/", env="ixmp4_dir")
    secret_hs256: str = "default_secret_hs256"
    migration_db_uri: str = "sqlite:///./run/db.sqlite"
    manager_url: HttpUrl = Field("https://api.manager.ece.iiasa.ac.at/v1")

    class Config:
        env_prefix = "ixmp4_"
        extra = Extra.allow

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        self.storage_directory.mkdir(parents=True, exist_ok=True)

        database_dir = self.storage_directory / "databases"
        database_dir.mkdir(exist_ok=True)
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
            # TODO: WARNING: No default credentials provided.
            pass

        if self.default_credentials is not None:
            try:
                username, password = self.default_credentials
                self.default_auth = ManagerAuth(username, password, self.manager_url)
            except InvalidCredentials:
                # TODO: WARNING: Default credentials invalid.
                pass

    def load_manager_config(self):
        self.manager = None
        if self.default_auth is not None:
            self.manager = ManagerConfig(
                self.manager_url, self.default_auth, remote=True
            )

    def load_toml_config(self):
        if self.default_auth is not None:
            toml_user = self.default_auth.get_user()
        else:
            toml_user = local_user
        toml_config = self.storage_directory / "platforms.toml"
        toml_config.touch()
        self.toml = TomlConfig(toml_config, toml_user)

    @validator("storage_directory")
    def expand_user(cls, v):
        # translate ~/asdf into /home/user/asdf
        return Path.expanduser(v)


load_dotenv()
# strict typechecking fails due to a bug
# https://docs.pydantic.dev/visual_studio_code/#adding-a-default-with-field
settings = Settings()  # type: ignore
