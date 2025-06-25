import tempfile
from pathlib import Path
from typing import Protocol

import pytest

from ixmp4.conf.credentials import Credentials
from ixmp4.conf.toml import TomlConfig
from ixmp4.conf.user import local_user
from ixmp4.core.exceptions import PlatformNotFound, PlatformNotUnique


@pytest.fixture(scope="class")
def toml_config() -> TomlConfig:
    tmp = tempfile.NamedTemporaryFile()
    with tmp:
        config = TomlConfig(Path(tmp.name), local_user)
        return config


class HasPath(Protocol):
    path: Path


class TomlTest:
    def assert_toml_file(self, toml_config: HasPath, expected_toml: str) -> None:
        with toml_config.path.open() as f:
            assert f.read() == expected_toml


class TestTomlPlatforms(TomlTest):
    def test_add_platform(self, toml_config: TomlConfig) -> None:
        toml_config.add_platform("test", "test://test/")

        expected_toml = '[test]\ndsn = "test://test/"\n'
        self.assert_toml_file(toml_config, expected_toml)

        toml_config.add_platform("test2", "test2://test2/")

        expected_toml = (
            '[test]\ndsn = "test://test/"\n\n[test2]\ndsn = "test2://test2/"\n'
        )
        self.assert_toml_file(toml_config, expected_toml)

    def test_platform_unique(self, toml_config: TomlConfig) -> None:
        with pytest.raises(PlatformNotUnique):
            toml_config.add_platform("test", "test://test/")

    def test_remove_platform(self, toml_config: TomlConfig) -> None:
        toml_config.remove_platform("test")
        expected_toml = '[test2]\ndsn = "test2://test2/"\n'

        with toml_config.path.open() as f:
            assert f.read() == expected_toml

        toml_config.remove_platform("test2")
        expected_toml = ""

        with toml_config.path.open() as f:
            assert f.read() == expected_toml

    def test_remove_missing_platform(self, toml_config: TomlConfig) -> None:
        with pytest.raises(PlatformNotFound):
            toml_config.remove_platform("test")


@pytest.fixture(scope="class")
def credentials() -> Credentials:
    tmp = tempfile.NamedTemporaryFile()
    with tmp:
        credentials = Credentials(Path(tmp.name))
        return credentials


class TestTomlCredentials(TomlTest):
    def test_set_credentials(self, credentials: Credentials) -> None:
        credentials.set("test", "user", "password")
        expected_toml = '[test]\nusername = "user"\npassword = "password"\n'
        self.assert_toml_file(credentials, expected_toml)

    def test_get_credentials(self, credentials: Credentials) -> None:
        ret = credentials.get("test")
        assert ret == ("user", "password")

    def test_clear_credentials(self, credentials: Credentials) -> None:
        credentials.clear("test")
        expected_toml = ""
        self.assert_toml_file(credentials, expected_toml)

        # clearing non-exsistent credentials is fine
        credentials.clear("test")

    def test_add_credentials(self, credentials: Credentials) -> None:
        credentials.set("test", "user", "password")
        expected_toml = '[test]\nusername = "user"\npassword = "password"\n'
        self.assert_toml_file(credentials, expected_toml)
