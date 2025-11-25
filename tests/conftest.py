import os
import pathlib
from collections.abc import Generator
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import mock

import pytest

from ixmp4.conf import settings
from tests.backends import clean_postgres_database as clean_postgres_database

settings.configure_logging("debug")
test_dir = pathlib.Path(__file__).parent
fixture_dir = test_dir / "fixtures"


def pytest_addoption(parser: pytest.Parser) -> None:
    """Called to set up the pytest command line parser.
    We can add our own options here."""

    parser.addoption(
        "--backend",
        action="store",
        default="sqlite,rest-sqlite",
    )
    parser.addoption(
        "--postgres-dsn",
        action="store",
        default="postgresql+psycopg://postgres:postgres@localhost:5432/test",
    )


@pytest.fixture(scope="class")
def fake_time():
    frozen_time = datetime.now(tz=timezone.utc)

    with mock.patch("ixmp4.services.Service.get_datetime", lambda s: frozen_time):
        yield frozen_time


def reload_settings(storage_directory: Path) -> None:
    """Reload the settings from the provided storage_directory
    to ensure that the test environment is clean."""
    settings.storage_directory = storage_directory
    settings.setup_directories()


@pytest.fixture(scope="function")
def clean_storage_directory() -> Generator[Path, None, None]:
    """Fixture to create a temporary ixmp4 storage directory for tests."""
    orginial_storage_dir = settings.storage_directory

    with TemporaryDirectory() as temp_dir:
        reload_settings(Path(temp_dir))
        yield settings.storage_directory

    # Restore the original settings
    reload_settings(orginial_storage_dir)


@pytest.fixture(scope="function")
def tmp_working_directory() -> Generator[Path, None, None]:
    """Fixture to create and enter a temporary working directory for tests."""
    with TemporaryDirectory() as temp_dir:
        orginal_dir = os.getcwd()
        os.chdir(temp_dir)
        yield Path(temp_dir)
        os.chdir(orginal_dir)
