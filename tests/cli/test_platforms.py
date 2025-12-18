import os
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Generator
from unittest import mock

import pytest
from typer.testing import CliRunner

from ixmp4.cli import app
from ixmp4.conf.settings import Settings

runner = CliRunner()


@pytest.fixture(scope="function")
def temporary_settings() -> Generator[Settings, None, None]:
    """Fixture to create settings pointing to a temporary directory
    and mocking the `Settings` constructor."""
    with TemporaryDirectory() as temp_dir:
        settings = Settings(storage_directory=Path(temp_dir))
        with mock.patch("ixmp4.conf.settings.Settings", new=lambda: settings):
            yield settings


@pytest.fixture(scope="function")
def tmp_working_directory() -> Generator[Path, None, None]:
    """Fixture to create and enter a temporary working directory for tests."""
    with TemporaryDirectory() as temp_dir:
        orginal_dir = os.getcwd()
        os.chdir(temp_dir)
        yield Path(temp_dir)
        os.chdir(orginal_dir)


class TestAddPlatformCLI:
    def test_add_platform(self, temporary_settings: Settings) -> None:
        result = runner.invoke(app, ["platforms", "add", "test"], input="y")
        assert result.exit_code == 0

        # Assert command output
        assert (
            "No DSN supplied, assuming you want to add a local sqlite database..."
            in result.stdout
        )
        assert (
            "No file at the standard filesystem location for name 'test' exists. "
            "Do you want to create a new database?"
        ) in result.stdout
        assert "Creating the database and running migrations..." in result.stdout

        assert "Platform added successfully." in result.stdout

        # Check if the database file was created
        database_file = (
            temporary_settings.storage_directory / "databases" / "test.sqlite3"
        )
        assert database_file.is_file()

        # Assert that the toml object now contains the new platform
        toml_platforms = temporary_settings.get_toml_platforms()
        platform_info = toml_platforms.get_platform("test")
        assert platform_info.name == "test"
        assert "test.sqlite3" in platform_info.dsn

    def test_add_platform_sqlite_dsn(self, temporary_settings: Settings) -> None:
        # Explicitly set the DSN to a different location
        alternative_path = (
            temporary_settings.storage_directory
            / "databases"
            / "test-alternative.sqlite3"
        )
        result = runner.invoke(
            app,
            ["platforms", "add", "test", f"--dsn=sqlite://{alternative_path}"],
        )
        assert result.exit_code == 0
        assert "Platform added successfully." in result.stdout

        # Ensure no database is created when a DSN is supplied
        assert (
            "No DSN supplied, assuming you want to add a local sqlite database..."
            not in result.stdout
        )
        assert (
            "No file at the standard filesystem location for name 'test' exists. "
            "Do you want to create a new database?"
        ) not in result.stdout
        assert "Creating the database and running migrations..." not in result.stdout

        # assert the supplied path is not a newly created database
        assert not alternative_path.is_file()

        # check the toml file
        toml_platforms = temporary_settings.get_toml_platforms()
        platform_info = toml_platforms.get_platform("test")
        assert platform_info.name == "test"
        assert str(alternative_path) in platform_info.dsn

    def test_add_platform_from_anywhere(
        self, temporary_settings: Settings, tmp_working_directory: Path
    ) -> None:
        # Ensure we are NOT in the ixmp4 root directory
        assert not (tmp_working_directory / "ixmp4" / "db" / "migrations").exists()

        # Assert platform creation still works
        result = runner.invoke(app, ["platforms", "add", "test"], input="y")
        assert result.exit_code == 0
        assert "Platform added successfully." in result.stdout
        assert (
            temporary_settings.storage_directory / "databases" / "test.sqlite3"
        ).is_file()

    def test_add_platform_duplicate(self, temporary_settings: Settings) -> None:
        runner.invoke(app, ["platforms", "add", "test"], input="y").stdout
        # Ensure the first platform is created
        assert (
            temporary_settings.storage_directory / "databases" / "test.sqlite3"
        ).is_file()

        # Assert failure when trying to add the same platform again
        result = runner.invoke(app, ["platforms", "add", "test"])
        assert result.exit_code == 2
        assert (
            "Invalid value: Platform with name 'test' already exists."
        ) in result.output


class TestPlatformGenerateCLI:
    def test_generate_platform_data(self) -> None:
        # Create a test platform
        runner.invoke(app, ["platforms", "add", "test-generate"], input="y")

        # Run generate command with small numbers for testing
        result = runner.invoke(
            app,
            [
                "platforms",
                "generate",
                "test-generate",
                "--models",
                "2",
                "--runs",
                "3",
                "--regions",
                "5",
                "--variables",
                "10",
                "--units",
                "3",
                "--datapoints",
                "50",
            ],
            input="y",
        )

        # We simply test whether the generate command runs without error
        assert result.exit_code == 0

    def test_generate_platform_not_found(self) -> None:
        result = runner.invoke(app, ["platforms", "generate", "nonexistent-platform"])

        # We simply test whether the generate command errors
        assert result.exit_code > 0
