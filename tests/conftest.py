import cProfile
import pstats
from contextlib import contextmanager
from pathlib import Path

import pytest

from ixmp4 import Platform
from ixmp4.conf.base import PlatformInfo
from ixmp4.core.exceptions import ProgrammingError
from ixmp4.data.backend import RestTestBackend, SqliteTestBackend
from ixmp4.data.backend.db import PostgresTestBackend

from .fixtures import MediumIamcDataset

backend_choices = ("sqlite", "postgres", "rest-sqlite", "rest-postgres")
backend_fixtures = {
    "ro_platform_med": ["sqlite", "postgres", "rest-sqlite", "rest-postgres"],
    "platform": ["sqlite", "postgres", "rest-sqlite", "rest-postgres"],
    "db_platform": ["sqlite", "postgres"],
    "rest_platform": ["rest-sqlite", "rest-postgres"],
    "sqlite_platform": ["sqlite"],
}


def pytest_addoption(parser):
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


@contextmanager
def yield_rest_sqlite_platform(*args):
    sqlite = SqliteTestBackend(
        PlatformInfo(name="sqlite-test", dsn="sqlite:///:memory:")
    )
    yield Platform(_backend=RestTestBackend(sqlite))
    sqlite.close()


@contextmanager
def yield_rest_postgresql_platform(dsn):
    pgsql = PostgresTestBackend(
        PlatformInfo(
            name="postgres-test",
            dsn=dsn,
        ),
    )
    yield Platform(_backend=RestTestBackend(pgsql))
    pgsql.close()


@contextmanager
def yield_postgresql_platform(dsn):
    pgsql = PostgresTestBackend(
        PlatformInfo(
            name="postgres-test",
            dsn=dsn,
        ),
    )
    yield Platform(_backend=pgsql)
    pgsql.close()


@contextmanager
def yield_sqlite_platform(*args):
    sqlite = SqliteTestBackend(
        PlatformInfo(name="sqlite-test", dsn="sqlite:///:memory:")
    )
    yield Platform(_backend=sqlite)
    sqlite.close()


def get_platform(gen):
    def fixture(request):
        with gen(request.config.options.postgres_dsn) as p:
            yield p

    return fixture


rest_sqlite_platform = pytest.fixture(
    get_platform(yield_rest_sqlite_platform), name="rest_sqlite_platform"
)
session_rest_sqlite_platform = pytest.fixture(
    get_platform(yield_rest_sqlite_platform), name="session_rest_sqlite_platform"
)

sqlite_platform = pytest.fixture(
    get_platform(yield_sqlite_platform), name="sqlite_platform"
)
session_sqlite_platform = pytest.fixture(
    get_platform(yield_sqlite_platform), name="session_sqlite_platform"
)

rest_postgresql_platform = pytest.fixture(
    get_platform(yield_rest_postgresql_platform), name="rest_postgresql_platform"
)
session_rest_postgresql_platform = pytest.fixture(
    get_platform(yield_rest_postgresql_platform),
    name="session_rest_postgresql_platform",
)

postgresql_platform = pytest.fixture(
    get_platform(yield_postgresql_platform), name="postgresql_platform"
)
session_postgresql_platform = pytest.fixture(
    get_platform(yield_postgresql_platform), name="session_postgresql_platform"
)


def get_platform_fixture(request, prefix=""):
    type = request.param
    if type == "rest-sqlite":
        return request.getfixturevalue(prefix + "rest_sqlite_platform")
    elif type == "rest-postgres":
        return request.getfixturevalue(prefix + "rest_postgresql_platform")
    elif type == "sqlite":
        return request.getfixturevalue(prefix + "sqlite_platform")
    elif type == "postgres":
        return request.getfixturevalue(prefix + "postgresql_platform")


rest_platform = pytest.fixture(get_platform_fixture, name="rest_platform")
db_platform = pytest.fixture(get_platform_fixture, name="db_platform")
platform = pytest.fixture(get_platform_fixture, name="platform")

medium = MediumIamcDataset()


@pytest.fixture(scope="session")
def ro_platform_med(request):
    """Session wide platform fixture pre-loaded with medium size test data."""
    p = get_platform_fixture(request, prefix="session_")
    medium.load_dataset(p)
    return p


def pytest_generate_tests(metafunc):
    # This is called for every test. Only get/set command line arguments
    # if the argument is specified in the list of test "fixturenames".

    # parse '--backend' option
    be_args = metafunc.config.option.backend.split(",")
    backend_types = [t.strip() for t in be_args]
    for bt in backend_types:
        if bt not in backend_choices:
            raise ProgrammingError(f"'{bt}' not a valid backend")

    for fixturename, allowed_types in backend_fixtures.items():
        pres_types = [t for t in backend_types if t in allowed_types]
        if fixturename in metafunc.fixturenames:
            metafunc.parametrize(
                fixturename, pres_types, indirect=True, scope="function"
            )


@pytest.fixture(scope="function")
def profiled(request):
    testname = request.node.name
    pr = cProfile.Profile()

    @contextmanager
    def profiled():
        pr.enable()
        yield
        pr.disable()

    yield profiled
    ps = pstats.Stats(pr)
    Path(".profiles").mkdir(parents=True, exist_ok=True)
    ps.dump_stats(f".profiles/{testname}.prof")
