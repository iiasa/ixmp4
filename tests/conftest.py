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

from .fixtures import BigIamcDataset

backend_choices = ("sqlite", "postgres", "rest-sqlite", "rest-postgres")
backend_fixtures = {
    "platform_big": ["sqlite", "postgres", "rest-sqlite", "rest-postgres"],
    "db_platform_big": ["sqlite", "postgres"],
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


class Backends:
    postgres_dsn: str

    def __init__(self, postgres_dsn: str) -> None:
        self.postgres_dsn = postgres_dsn

    @contextmanager
    def rest_sqlite(self):
        with self.sqlite() as backend:
            rest = RestTestBackend(backend)
            yield rest
            rest.close()

    @contextmanager
    def rest_postgresql(self):
        with self.postgresql() as backend:
            rest = RestTestBackend(backend)
            yield rest
            rest.close()

    @contextmanager
    def postgresql(self):
        pgsql = PostgresTestBackend(
            PlatformInfo(
                name="postgres-test",
                dsn=self.postgres_dsn,
            ),
        )
        yield pgsql
        pgsql.close()

    @contextmanager
    def sqlite(self):
        sqlite = SqliteTestBackend(
            PlatformInfo(name="sqlite-test", dsn="sqlite:///:memory:")
        )
        yield sqlite
        sqlite.close()


def get_backend_context(type, postgres_dsn):
    backends = Backends(postgres_dsn)

    if type == "rest-sqlite":
        bctx = backends.rest_sqlite()
    elif type == "rest-postgres":
        bctx = backends.rest_postgresql()
    elif type == "sqlite":
        bctx = backends.sqlite()
    elif type == "postgres":
        bctx = backends.postgresql()
    return bctx


def platform_fixture(request):
    type = request.param
    postgres_dsn = request.config.option.postgres_dsn
    bctx = get_backend_context(type, postgres_dsn)

    with bctx as backend:
        backend._create_all()
        yield Platform(_backend=backend)
        backend.session.rollback()
        backend._drop_all()


rest_platform = pytest.fixture(platform_fixture, name="rest_platform")
db_platform = pytest.fixture(platform_fixture, name="db_platform")
sqlite_platform = pytest.fixture(platform_fixture, name="sqlite_platform")
platform = pytest.fixture(platform_fixture, name="platform")

big = BigIamcDataset()


def platform_td_big(request):
    type = request.param
    postgres_dsn = request.config.option.postgres_dsn
    bctx = get_backend_context(type, postgres_dsn)

    with bctx as backend:
        backend._create_all()
        platform = Platform(_backend=backend)
        big.load_dataset(platform)
        yield platform
        backend.session.rollback()
        backend._drop_all()


db_platform_big = pytest.fixture(
    platform_td_big, scope="session", name="db_platform_big"
)


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
            metafunc.parametrize(fixturename, pres_types, indirect=True)


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
