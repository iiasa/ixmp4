import pathlib
from collections.abc import Generator
from datetime import datetime, timezone
from unittest import mock

import pytest

from ixmp4.conf.settings import Settings
from tests.backends import clean_postgres_database as clean_postgres_database
from tests.profiling import profiled as profiled

from .auth import mock_manager_client as mock_manager_client
from .auth import none_user as none_user
from .auth import platform_gated as platform_gated
from .auth import platform_private as platform_private
from .auth import platform_public as platform_public
from .auth import staffuser_alice as staffuser_alice
from .auth import staffuser_bob as staffuser_bob
from .auth import superuser_sarah as superuser_sarah
from .auth import user_carina as user_carina
from .auth import user_dave as user_dave
from .auth import user_eve as user_eve

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


@pytest.fixture(scope="session")
def settings() -> Settings:
    return Settings()


@pytest.fixture(scope="session", autouse=True)
def debug_logging(settings: Settings) -> None:
    settings.configure_logging("debug")


@pytest.fixture(scope="class")
def fake_time() -> Generator[datetime, None, None]:
    frozen_time = datetime.now(tz=timezone.utc)

    with mock.patch("ixmp4.services.Service.get_datetime", lambda s: frozen_time):
        yield frozen_time


@pytest.fixture(scope="class")
def auth_ctx() -> None:
    return None


@pytest.fixture(scope="class")
def platform_info() -> None:
    return None
