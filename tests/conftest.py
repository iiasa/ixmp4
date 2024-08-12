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

backend_choices = ("sqlite", "postgres", "rest-sqlite", "rest-postgres")


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


@pytest.fixture
def rest_sqlite_platform():
    sqlite = SqliteTestBackend(
        PlatformInfo(name="sqlite-test", dsn="sqlite:///:memory:")
    )
    yield Platform(_backend=RestTestBackend(sqlite))
    sqlite.close()


@pytest.fixture
def rest_postgresql_platform(pytestconfig):
    pgsql = PostgresTestBackend(
        PlatformInfo(
            name="postgres-test",
            dsn=pytestconfig.option.postgres_dsn,
        ),
    )
    yield Platform(_backend=RestTestBackend(pgsql))
    pgsql.close()


@pytest.fixture
def postgresql_platform(pytestconfig):
    pgsql = PostgresTestBackend(
        PlatformInfo(
            name="postgres-test",
            dsn=pytestconfig.option.postgres_dsn,
        ),
    )
    yield Platform(_backend=pgsql)
    pgsql.close()


@pytest.fixture
def sqlite_platform():
    sqlite = SqliteTestBackend(
        PlatformInfo(name="sqlite-test", dsn="sqlite:///:memory:")
    )
    yield Platform(_backend=sqlite)
    sqlite.close()


@pytest.fixture
def rest_platform(request):
    type = request.param
    if type == "rest-sqlite":
        return request.getfixturevalue(rest_sqlite_platform.__name__)
    elif type == "rest-postgres":
        return request.getfixturevalue(rest_postgresql_platform.__name__)


@pytest.fixture
def db_platform(request):
    type = request.param
    if type == "sqlite":
        return request.getfixturevalue(sqlite_platform.__name__)
    elif type == "postgres":
        return request.getfixturevalue(postgresql_platform.__name__)


@pytest.fixture
def platform(request):
    type = request.param
    if type == "rest-sqlite":
        return request.getfixturevalue(rest_sqlite_platform.__name__)
    elif type == "rest-postgres":
        return request.getfixturevalue(rest_postgresql_platform.__name__)
    elif type == "sqlite":
        return request.getfixturevalue(sqlite_platform.__name__)
    elif type == "postgres":
        return request.getfixturevalue(postgresql_platform.__name__)


def pytest_generate_tests(metafunc):
    # This is called for every test. Only get/set command line arguments
    # if the argument is specified in the list of test "fixturenames".

    # parse '--backend' option
    be_args = metafunc.config.option.backend.split(",")
    backend_types = [t.strip() for t in be_args]
    for bt in backend_types:
        if bt not in backend_choices:
            raise ProgrammingError(f"'{bt}' not a valid backend")

    rest_backend_types = [t for t in backend_types if t.startswith("rest")]
    db_backend_types = [t for t in backend_types if not t.startswith("rest")]
    run_sqlite = "sqlite" in backend_types

    # parameterize tests with platform fixture requests
    if "platform" in metafunc.fixturenames:
        metafunc.parametrize("platform", backend_types, indirect=True)

    if "db_platform" in metafunc.fixturenames:
        metafunc.parametrize("db_platform", db_backend_types, indirect=True)

    if "rest_platform" in metafunc.fixturenames:
        metafunc.parametrize("rest_platform", rest_backend_types, indirect=True)

    if "sqlite_platform" in metafunc.fixturenames:
        metafunc.parametrize(
            "sqlite_platform",
            ["sqlite"] if run_sqlite else [],
            indirect=True,
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
