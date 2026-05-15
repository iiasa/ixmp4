import os
import re
from io import StringIO
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from typing import Generator
from unittest import mock

import pytest
from rich.console import Console
from typer.testing import CliRunner

from ixmp4.cli import app
from ixmp4.conf.settings import Settings


@pytest.fixture(scope="class")
def temporary_settings() -> Generator[Settings, None, None]:
    """Fixture to create settings pointing to a temporary directory
    and mocking the `Settings` constructor."""
    with TemporaryDirectory() as temp_dir:
        settings = Settings(storage_directory=Path(temp_dir))
        with mock.patch("ixmp4.conf.settings.Settings", new=settings):
            yield settings


@pytest.fixture(scope="class")
def runner(temporary_settings: Settings) -> CliRunner:
    return CliRunner(
        env={"IXMP4_STORAGE_DIRECTORY": str(temporary_settings.storage_directory)},
    )


@pytest.fixture(scope="function")
def tmp_working_directory() -> Generator[Path, None, None]:
    """Fixture to create and enter a temporary working directory for tests."""
    with TemporaryDirectory() as temp_dir:
        orginal_dir = os.getcwd()
        os.chdir(temp_dir)
        yield Path(temp_dir)
        os.chdir(orginal_dir)


class TestPlatformCLI:
    def test_root_command_prints_banner(
        self, runner: CliRunner, temporary_settings: Settings
    ) -> None:
        result = runner.invoke(app, [])

        assert result.exit_code == 0
        assert "_____  ____  __ ___ _ _" in result.output
        assert "Version" in result.output
        assert "Mode" in result.output
        assert str(temporary_settings.storage_directory) in result.output
        assert "Usage:" in result.output
        assert "Show this message and exit." in result.output
        assert "platforms" in result.output

    def test_add_platform(
        self, runner: CliRunner, temporary_settings: Settings
    ) -> None:
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

    def test_add_platform_sqlite_dsn(
        self, runner: CliRunner, temporary_settings: Settings
    ) -> None:
        # Explicitly set the DSN to a different location
        alternative_path = (
            temporary_settings.storage_directory
            / "databases"
            / "test-alternative-path.sqlite3"
        )
        result = runner.invoke(
            app,
            [
                "platforms",
                "add",
                "test-alternative",
                f"--dsn=sqlite://{alternative_path}",
            ],
        )
        assert result.exit_code == 0
        assert "Platform added successfully." in result.stdout

        # Ensure no database is created when a DSN is supplied
        assert (
            "No DSN supplied, assuming you want to add a local sqlite database..."
            not in result.stdout
        )
        assert (
            "No file at the standard filesystem location for name "
            "'test-alternative' exists. "
            "Do you want to create a new database?"
        ) not in result.stdout
        assert "Creating the database and running migrations..." not in result.stdout

        # assert the supplied path is not a newly created database
        assert not alternative_path.is_file()

        # check the toml file
        toml_platforms = temporary_settings.get_toml_platforms()
        platform_info = toml_platforms.get_platform("test-alternative")
        assert platform_info.name == "test-alternative"
        assert str(alternative_path) in platform_info.dsn

    def test_add_platform_from_anywhere(
        self,
        runner: CliRunner,
        temporary_settings: Settings,
        tmp_working_directory: Path,
    ) -> None:
        # Ensure we are NOT in the ixmp4 root directory
        assert not (tmp_working_directory / "ixmp4" / "db" / "migrations").exists()

        # Assert platform creation still works
        result = runner.invoke(app, ["platforms", "add", "test-anywhere"], input="y")
        assert result.exit_code == 0
        assert "Platform added successfully." in result.stdout
        assert (
            temporary_settings.storage_directory / "databases" / "test-anywhere.sqlite3"
        ).is_file()

    def test_add_platform_duplicate(
        self, runner: CliRunner, temporary_settings: Settings
    ) -> None:
        # Assert failure when trying to add the same platform again
        result = runner.invoke(app, ["platforms", "add", "test"])
        assert result.exit_code == 2
        assert (
            "Invalid value: Platform with name 'test' already exists."
        ) in result.output

    def test_list_platforms(
        self, runner: CliRunner, temporary_settings: Settings
    ) -> None:
        result = runner.invoke(app, ["platforms", "list"])
        assert result.exit_code == 0

        assert "via toml file" in result.output
        assert "databases/test" in result.output
        assert "databases/test-alternative" in result.output
        assert "databases/test-anywhere" in result.output

        assert "via manager api" in result.output

    def test_remove_platform(
        self, runner: CliRunner, temporary_settings: Settings
    ) -> None:
        # Remove platform and delete sqlite file
        result = runner.invoke(
            app, ["platforms", "remove", "test-anywhere"], input="y\ny"
        )
        assert result.exit_code == 0
        assert (
            "Are you sure you want to remove the platform 'test-anywhere' with dsn"
            in result.output
        )
        assert "Do you want to remove the associated database file at" in result.output
        assert "Database file deleted." in result.output

        # Remove platform without deleting sqlite file
        alternative_path = (
            temporary_settings.storage_directory
            / "databases"
            / "test-alternative-path.sqlite3"
        )
        alternative_path.touch()

        result = runner.invoke(
            app, ["platforms", "remove", "test-alternative"], input="y\nn"
        )
        assert result.exit_code == 0
        assert (
            "Are you sure you want to remove the platform 'test-alternative' with dsn"
            in result.output
        )
        assert "Do you want to remove the associated database file at" in result.output
        assert "Database file left intact." in result.output
        assert alternative_path.exists()  # check file still exists

        # test platforms dont show up in the list
        result = runner.invoke(app, ["platforms", "list"])
        assert result.exit_code == 0

        assert "test-anywhere" not in result.output
        assert "test-alternative" not in result.output
        assert not (
            temporary_settings.storage_directory / "databases" / "test-anywhere.sqlite3"
        ).exists()

    def test_remove_platform_not_found(
        self, runner: CliRunner, temporary_settings: Settings
    ) -> None:
        result = runner.invoke(
            app, ["platforms", "remove", "nonexistent-platform"], input="y\ny"
        )
        assert result.exit_code > 0
        assert "Platform 'nonexistent-platform' does not exist." in result.output


class TestPlatformGenerateCLI:
    def test_generate_platform_data(self, runner: CliRunner) -> None:
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

    def test_generate_platform_not_found(self, runner: CliRunner) -> None:
        result = runner.invoke(app, ["platforms", "generate", "nonexistent-platform"])

        # We simply test whether the generate command errors
        assert result.exit_code > 0
