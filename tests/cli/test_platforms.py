from pathlib import Path

from typer.testing import CliRunner

from ixmp4.cli import platforms
from ixmp4.conf import settings

runner = CliRunner()


class TestAddPlatformCLI:
    def test_add_platform(self, clean_storage_directory: Path) -> None:
        result = runner.invoke(platforms.app, ["add", "test"], input="y")
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
        database_file = clean_storage_directory / "databases" / "test.sqlite3"
        assert database_file.is_file()

        # Assert that the toml object now contains the new platform
        platform_info = settings.toml.get_platform("test")
        assert platform_info.name == "test"
        assert "test.sqlite3" in platform_info.dsn

    def test_add_platform_sqlite_dsn(self, clean_storage_directory: Path) -> None:
        # Explicitly set the DSN to a different location
        alternative_path = (
            clean_storage_directory / "databases" / "test-alternative.sqlite3"
        )
        result = runner.invoke(
            platforms.app,
            ["add", "test", f"--dsn=sqlite://{alternative_path}"],
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
        assert not alternative_path.is_file()

    def test_add_platform_from_anywhere(
        self, clean_storage_directory: Path, tmp_working_directory: Path
    ) -> None:
        # Ensure we are NOT in the ixmp4 root directory
        assert not (tmp_working_directory / "ixmp4" / "db" / "migrations").exists()

        # Assert platform creation still works
        result = runner.invoke(platforms.app, ["add", "test"], input="y")
        assert result.exit_code == 0
        assert "Platform added successfully." in result.stdout
        assert (clean_storage_directory / "databases" / "test.sqlite3").is_file()

    def test_add_platform_duplicate(self, clean_storage_directory: Path) -> None:
        runner.invoke(platforms.app, ["add", "test"], input="y").stdout
        # Ensure the first platform is created
        assert (clean_storage_directory / "databases" / "test.sqlite3").is_file()

        # Assert failure when trying to add the same platform again
        result = runner.invoke(platforms.app, ["add", "test"])
        assert result.exit_code == 2
        assert (
            "Invalid value: Platform with name 'test' already exists."
        ) in result.output


class TestPlatformGenerateCLI:
    def test_generate_platform_data(self) -> None:
        # Create a test platform
        runner.invoke(platforms.app, ["add", "test-generate"], input="y")

        # Run generate command with small numbers for testing
        result = runner.invoke(
            platforms.app,
            [
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
        result = runner.invoke(platforms.app, ["generate", "nonexistent-platform"])

        # We simply test whether the generate command errors
        assert result.exit_code == 2
