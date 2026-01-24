import os
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Generator
from unittest import mock

import pytest
from pydantic import SecretStr
from toolkit.client.auth import ManagerAuth, SelfSignedAuth

from ixmp4.conf.settings import ClientSettings, Settings


@pytest.fixture(scope="function")
def temporary_directory() -> Generator[Path, None, None]:
    with TemporaryDirectory() as temp_dir:
        yield Path(temp_dir)


@pytest.fixture(scope="function")
def empty_env() -> Generator[None, None, None]:
    with mock.patch.dict(os.environ, {}, clear=True):
        yield


@pytest.fixture(scope="class")
def tmp_working_directory() -> Generator[Path, None, None]:
    """Fixture to create and enter a temporary working directory for tests."""
    with TemporaryDirectory() as temp_dir:
        orginal_dir = os.getcwd()
        os.chdir(temp_dir)
        yield Path(temp_dir)
        os.chdir(orginal_dir)


class TestSettings:
    def test_default_storage_dir(
        self, empty_env: None, tmp_working_directory: Path
    ) -> None:
        settings = Settings()
        assert (
            Path(settings.storage_directory)
            == Path("~/.local/share/ixmp4").expanduser()
        )

    def test_default_settings(
        self, empty_env: None, temporary_directory: Path, tmp_working_directory: Path
    ) -> None:
        settings = Settings(storage_directory=temporary_directory)
        expected_storage_dir = temporary_directory

        expected_credentials_path = expected_storage_dir / "credentials.toml"
        assert settings.get_credentials().path == expected_credentials_path
        assert expected_credentials_path.exists()

        expected_platforms_path = expected_storage_dir / "platforms.toml"
        assert settings.get_toml_platforms().path == expected_platforms_path
        assert expected_platforms_path.exists()

        server_toml = settings.server.get_toml_platforms()
        assert server_toml is not None
        assert server_toml.path == expected_platforms_path

        expected_database_dir = expected_storage_dir / "databases"
        assert settings.get_database_dir() == expected_database_dir

        expected_database_path = expected_database_dir / "test.sqlite3"
        assert settings.get_database_path("test") == expected_database_path

        manager_platforms = settings.get_manager_platforms()
        assert manager_platforms.manager_client.auth is None

    def test_client_auth(
        self, empty_env: None, temporary_directory: Path, tmp_working_directory: Path
    ) -> None:
        # manager
        with (temporary_directory / "credentials.toml").open("w") as creds_file:
            creds_file.write('[default]\nusername = "user"\npassword = "password"\n')

        settings = Settings(storage_directory=temporary_directory)

        with mock.patch(
            "toolkit.client.auth.ManagerAuth.obtain_jwt", return_value=None
        ):
            with mock.patch(
                "toolkit.manager.client.ManagerClient.check_root", return_value=None
            ):
                creds_manager_client = settings.get_manager_client()

        assert isinstance(creds_manager_client.auth, ManagerAuth)

        # self signed
        settings = Settings(
            storage_directory=temporary_directory,
            client=ClientSettings(secret_hs256=SecretStr("changeme")),
        )

        with mock.patch(
            "toolkit.manager.client.ManagerClient.check_root", return_value=None
        ):
            creds_manager_client = settings.get_manager_client()
        assert isinstance(creds_manager_client.auth, SelfSignedAuth)

    def test_interactive(
        self, empty_env: None, temporary_directory: Path, tmp_working_directory: Path
    ) -> None:
        with mock.patch("ixmp4.conf.settings._sys_has_ps1", True):
            with mock.patch(
                "ixmp4.conf.settings.Settings.configure_logging"
            ) as configure_logging:
                Settings(storage_directory=temporary_directory)
                configure_logging.assert_called_with("production")
