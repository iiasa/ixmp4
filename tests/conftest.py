import cProfile
import os
import pstats
from collections.abc import Callable, Generator
from contextlib import _GeneratorContextManager, contextmanager
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, TypeAlias

import pytest

from ixmp4 import Platform
from ixmp4.conf import settings
from ixmp4.conf.base import PlatformInfo
from ixmp4.core.exceptions import ProgrammingError
from ixmp4.data.backend import RestTestBackend, SqliteTestBackend
from ixmp4.data.backend.db import PostgresTestBackend

from .fixtures import BigIamcDataset, MediumIamcDataset

backend_choices = ("sqlite", "postgres", "rest-sqlite", "rest-postgres")
backend_fixtures = {
    "rest_platform_med": ["rest-sqlite", "rest-postgres"],
    "platform_med": ["sqlite", "postgres", "rest-sqlite", "rest-postgres"],
    "platform_big": ["sqlite", "postgres", "rest-sqlite", "rest-postgres"],
    "db_platform_big": ["sqlite", "postgres"],
    "platform": ["sqlite", "postgres", "rest-sqlite", "rest-postgres"],
    "db_platform": ["sqlite", "postgres"],
    "rest_platform": ["rest-sqlite", "rest-postgres"],
    "sqlite_platform": ["sqlite"],
}


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
        default="postgresql://postgres:postgres@localhost:5432/test",
    )


class Backends:
    """Defines creation, setup and teardown for all types of backends."""

    postgres_dsn: str

    def __init__(self, postgres_dsn: str) -> None:
        self.postgres_dsn = postgres_dsn

    @contextmanager
    def rest_sqlite(self) -> Generator[RestTestBackend, Any, None]:
        with self.sqlite() as backend:
            rest = RestTestBackend(backend)
            rest.setup()
            yield rest
            rest.close()
            rest.teardown()

    @contextmanager
    def rest_postgresql(self) -> Generator[RestTestBackend, Any, None]:
        with self.postgresql() as backend:
            rest = RestTestBackend(backend)
            rest.setup()
            yield rest
            rest.close()
            rest.teardown()

    @contextmanager
    def postgresql(self) -> Generator[PostgresTestBackend, Any, None]:
        pgsql = PostgresTestBackend(
            PlatformInfo(
                name="postgres-test",
                dsn=self.postgres_dsn,
            ),
        )
        pgsql.setup()
        yield pgsql
        pgsql.close()
        pgsql.teardown()

    @contextmanager
    def sqlite(self) -> Generator[SqliteTestBackend, Any, None]:
        sqlite = SqliteTestBackend(
            PlatformInfo(name="sqlite-test", dsn="sqlite:///:memory:")
        )
        sqlite.setup()
        yield sqlite
        sqlite.close()
        sqlite.teardown()


def get_backend_context(
    type: str, postgres_dsn: str
) -> (
    _GeneratorContextManager[RestTestBackend]
    | _GeneratorContextManager[PostgresTestBackend]
    | _GeneratorContextManager[SqliteTestBackend]
):
    backends = Backends(postgres_dsn)

    bctx: (
        _GeneratorContextManager[RestTestBackend]
        | _GeneratorContextManager[PostgresTestBackend]
        | _GeneratorContextManager[SqliteTestBackend]
    )
    if type == "rest-sqlite":
        bctx = backends.rest_sqlite()
    elif type == "rest-postgres":
        bctx = backends.rest_postgresql()
    elif type == "sqlite":
        bctx = backends.sqlite()
    elif type == "postgres":
        bctx = backends.postgresql()
    return bctx


def platform_fixture(request: pytest.FixtureRequest) -> Generator[Platform, Any, None]:
    type = request.param
    postgres_dsn = request.config.option.postgres_dsn
    bctx = get_backend_context(type, postgres_dsn)

    with bctx as backend:
        yield Platform(_backend=backend)


# function scope fixtures
rest_platform = pytest.fixture(platform_fixture, name="rest_platform")
db_platform = pytest.fixture(platform_fixture, name="db_platform")
sqlite_platform = pytest.fixture(platform_fixture, name="sqlite_platform")
platform = pytest.fixture(platform_fixture, name="platform")

big = BigIamcDataset()
medium = MediumIamcDataset()


def td_platform_fixture(
    td: BigIamcDataset | MediumIamcDataset,
) -> Callable[[pytest.FixtureRequest], Generator[Platform, Any, None]]:
    def platform_with_td(
        request: pytest.FixtureRequest,
    ) -> Generator[Platform, Any, None]:
        type = request.param
        postgres_dsn = request.config.option.postgres_dsn
        bctx = get_backend_context(type, postgres_dsn)

        with bctx as backend:
            platform = Platform(_backend=backend)
            td.load_dataset(platform)
            yield platform

    return platform_with_td


# class scope fixture with big test data
db_platform_big = pytest.fixture(
    td_platform_fixture(big), scope="class", name="db_platform_big"
)

platform_med = pytest.fixture(
    td_platform_fixture(medium), scope="class", name="platform_med"
)

rest_platform_med = pytest.fixture(
    td_platform_fixture(medium), scope="class", name="rest_platform_med"
)


def pytest_generate_tests(metafunc: pytest.Metafunc) -> Any:
    # This is called for every test. Only get/set command line arguments
    # if the argument is specified in the list of test "fixturenames".

    # parse '--backend' option
    be_args = metafunc.config.option.backend.split(",")
    backend_types = [t.strip() for t in be_args]
    for bt in backend_types:
        if bt not in backend_choices:
            raise ProgrammingError(f"'{bt}' not a valid backend")

    # the `backend_fixtures` dict tells us which backends are allowed
    # for which fixtures
    for fixturename, allowed_types in backend_fixtures.items():
        pres_types = [t for t in backend_types if t in allowed_types]
        if fixturename in metafunc.fixturenames:
            metafunc.parametrize(fixturename, pres_types, indirect=True)


@pytest.fixture(scope="function")
def profiled(
    request: pytest.FixtureRequest,
) -> Generator[Callable[[], _GeneratorContextManager[None]]]:
    """Use this fixture for profiling tests:
    ```
    def test(profiled):
        # setup() ...
        with profiled():
            complex_procedure()
        # teardown() ...
    ```
    Profiler output will be written to '.profiles/{testname}.prof'
    """

    testname = request.node.name
    pr = cProfile.Profile()

    @contextmanager
    def profiled() -> Generator[None, Any, None]:
        pr.enable()
        yield
        pr.disable()

    yield profiled
    ps = pstats.Stats(pr)
    Path(".profiles").mkdir(parents=True, exist_ok=True)
    ps.dump_stats(f".profiles/{testname}.prof")


Profiled: TypeAlias = Callable[[], _GeneratorContextManager[None]]


@pytest.fixture(scope="function")
def clean_storage_directory() -> Generator[Path, None, None]:
    """Fixture to create a temporary ixmp4 storage directory for tests."""
    orginial_storage_dir = settings.storage_directory

    with TemporaryDirectory() as temp_dir:
        settings.storage_directory = Path(temp_dir)
        settings.setup_directories()
        settings.load_toml_config()
        yield settings.storage_directory

    settings.storage_directory = orginial_storage_dir


@pytest.fixture(scope="function")
def tmp_working_directory() -> Generator[Path, None, None]:
    """Fixture to create and enter a temporary working directory for tests."""
    with TemporaryDirectory() as temp_dir:
        orginal_dir = os.getcwd()
        os.chdir(temp_dir)
        yield Path(temp_dir)
        os.chdir(orginal_dir)
