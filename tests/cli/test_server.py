import json
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Generator
from unittest import mock

import pytest
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


@pytest.fixture(scope="function")
def runner(temporary_settings: Settings) -> CliRunner:
    return CliRunner(
        env={"IXMP4_STORAGE_DIRECTORY": str(temporary_settings.storage_directory)},
    )


class TestServerCLI:
    def test_server_start(
        self, runner: CliRunner, temporary_settings: Settings
    ) -> None:
        with mock.patch(
            "ixmp4.server.v1.V1HttpApi.on_startup", side_effect=KeyboardInterrupt
        ):
            result = runner.invoke(app, ["server", "start"])
        assert "Started server process" in result.output

    def test_server_dump_schema(
        self, runner: CliRunner, temporary_settings: Settings
    ) -> None:
        result = runner.invoke(app, ["server", "dump-schema"])
        assert result.exit_code == 0
        schema = json.loads(result.stdout)
        assert schema["info"]["title"] == "IXMP4"
