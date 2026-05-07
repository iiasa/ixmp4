import inspect
import tempfile
from pathlib import Path
from typing import Protocol

import pytest

from ixmp4.conf.credentials import Credentials
from ixmp4.conf.platforms import TomlPlatforms
from ixmp4.core.exceptions import (
    ImproperlyConfigured,
    PlatformNotFound,
    PlatformNotUnique,
)


@pytest.fixture(scope="class")
def toml_platforms() -> TomlPlatforms:
    tmp = tempfile.NamedTemporaryFile()
    with tmp:
        config = TomlPlatforms(Path(tmp.name))
        return config


class HasPath(Protocol):
    path: Path


class TomlTest:
    def assert_toml_file(self, toml_platforms: HasPath, expected_toml: str) -> None:
        with toml_platforms.path.open() as f:
            assert inspect.cleandoc(f.read()) == inspect.cleandoc(expected_toml)


class TestTomlPlatforms(TomlTest):
    def test_add_platform(self, toml_platforms: TomlPlatforms) -> None:
        toml_platforms.add_platform("test", "test://test/")

        expected_toml = """
        [test]
        dsn = "test://test/"
        """
        self.assert_toml_file(toml_platforms, expected_toml)

        toml_platforms.add_platform("test2", "test2://test2/")
        expected_toml = """
        [test]
        dsn = "test://test/"

        [test2]
        dsn = "test2://test2/"
        """
        self.assert_toml_file(toml_platforms, expected_toml)

    def test_platform_unique(self, toml_platforms: TomlPlatforms) -> None:
        with pytest.raises(PlatformNotUnique):
            toml_platforms.add_platform("test", "test://test/")

    def test_remove_platform(self, toml_platforms: TomlPlatforms) -> None:
        toml_platforms.remove_platform("test")
        expected_toml = """
        [test2]
        dsn = "test2://test2/"
        """
        self.assert_toml_file(toml_platforms, expected_toml)

        toml_platforms.remove_platform("test2")
        expected_toml = ""

        self.assert_toml_file(toml_platforms, expected_toml)

    def test_remove_missing_platform(self, toml_platforms: TomlPlatforms) -> None:
        with pytest.raises(PlatformNotFound):
            toml_platforms.remove_platform("test")

    def test_get_platform_substitutes_dsn_env_tokens(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        platforms_toml = tmp_path / "platforms.toml"
        platforms_toml.write_text(
            '[test]\ndsn = "postgresql://user:{env:IXMP4_TEST_PASSWORD}@foo.bar/db"\n'
        )
        monkeypatch.setenv("IXMP4_TEST_PASSWORD", "s3cr3t")

        platforms = TomlPlatforms(platforms_toml)
        assert (
            platforms.get_platform("test").dsn == "postgresql://user:s3cr3t@foo.bar/db"
        )

        # Placeholder syntax must remain on disk and never be persisted with secrets.
        assert "{env:IXMP4_TEST_PASSWORD}" in platforms_toml.read_text()

    def test_get_platform_raises_for_missing_dsn_env_tokens(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        platforms_toml = tmp_path / "platforms.toml"
        platforms_toml.write_text(
            '[test]\ndsn = "postgresql://user:{env:IXMP4_MISSING_PASSWORD}@foo.bar/db"\n'
        )
        monkeypatch.delenv("IXMP4_MISSING_PASSWORD", raising=False)

        platforms = TomlPlatforms(platforms_toml)
        with pytest.raises(
            ImproperlyConfigured,
            match=r"Cannot resolve DSN environment variable placeholder\(s\).",
        ):
            platforms.get_platform("test")


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
        assert ret == {"username": "user", "password": "password"}

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
