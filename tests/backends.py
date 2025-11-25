import contextlib
from typing import Generator, Sequence

import pytest
import sqlalchemy as sa
from sqlalchemy.exc import OperationalError

from ixmp4.backend import Backend
from ixmp4.core.platform import Platform
from ixmp4.db.models import get_metadata
from ixmp4.transport import DirectTransport, HttpxTransport, Transport

backend_choices = ("sqlite", "postgres", "rest-sqlite", "rest-postgres")


def validate_backend_type(type_: str) -> str:
    if type_ not in backend_choices:
        raise ValueError(
            f"Backend type '{type_}' is not valid. Expected one of: "
            + ", ".join(backend_choices),
        )
    else:
        return type_


def get_requested_backends(req_or_meta: pytest.FixtureRequest | pytest.Metafunc):
    be_args = req_or_meta.config.option.backend.split(",")
    backend_types = [validate_backend_type(t.strip()) for t in be_args]
    return backend_types


def get_active_backends(
    cand: Sequence[str], request: pytest.FixtureRequest
) -> list[str]:
    return list(set(cand) & set(get_requested_backends(request)))


def get_sorted_tables(meta: sa.MetaData, tables: list[str] | None):
    if tables is None:
        return meta.sorted_tables

    sorted_tables = []
    for table in meta.sorted_tables:
        if table.name in tables:
            sorted_tables.append(table)
    return sorted_tables


def create_model_tables(
    bind: sa.Engine | sa.Connection, tables: list[str] | None = None
):
    meta = get_metadata()
    meta.create_all(bind=bind, tables=meta.sorted_tables, checkfirst=True)


def drop_model_tables(bind: sa.Engine | sa.Connection, tables: list[str] | None = None):
    meta = get_metadata()
    print(list(reversed(get_sorted_tables(meta, tables))))
    meta.drop_all(
        bind=bind, tables=reversed(get_sorted_tables(meta, tables)), checkfirst=True
    )


@contextlib.contextmanager
def postgresql_transport(
    dsn: str,
    dirty_tables: list[str] | None = None,
    create_tables: bool = True,
) -> Generator[DirectTransport, None, None]:
    pgsql = DirectTransport.from_dsn(dsn)
    if create_tables:
        create_model_tables(pgsql.session.bind.engine)
    yield pgsql
    pgsql.close()
    drop_model_tables(pgsql.session.bind.engine, dirty_tables)


@contextlib.contextmanager
def sqlite_transport(
    dirty_tables: list[str] | None = None,
    create_tables: bool = True,
) -> Generator[DirectTransport, None, None]:
    sqlite = DirectTransport.from_dsn("sqlite:///:memory:")
    if create_tables:
        create_model_tables(sqlite.session.bind.engine)
    yield sqlite
    sqlite.close()
    drop_model_tables(sqlite.session.bind.engine, dirty_tables)


@contextlib.contextmanager
def httpx_sqlite_transport(
    dirty_tables: list[str] | None = None,
    create_tables: bool = True,
) -> Generator[HttpxTransport, None, None]:
    with sqlite_transport(
        dirty_tables=dirty_tables, create_tables=create_tables
    ) as direct:
        httpx_sqlite = HttpxTransport.from_direct(direct)
        yield httpx_sqlite


@contextlib.contextmanager
def httpx_postgresql_transport(
    dsn: str,
    dirty_tables: list[str] | None = None,
    create_tables: bool = True,
) -> Generator[HttpxTransport, None, None]:
    with postgresql_transport(
        dsn, dirty_tables=dirty_tables, create_tables=create_tables
    ) as direct:
        httpx_pgsql = HttpxTransport.from_direct(direct)
        yield httpx_pgsql


def transport(
    request: pytest.FixtureRequest,
    dirty_tables: list[str] | None = None,
    create_tables: bool = True,
) -> Generator[Transport, None, None]:
    postgres_dsn = request.config.option.postgres_dsn
    type = request.param

    if type == "rest-sqlite":
        tpt_ctx = httpx_sqlite_transport(
            dirty_tables=dirty_tables, create_tables=create_tables
        )
    elif type == "rest-postgres":
        tpt_ctx = httpx_postgresql_transport(
            postgres_dsn, dirty_tables=dirty_tables, create_tables=create_tables
        )
    elif type == "sqlite":
        tpt_ctx = sqlite_transport(
            dirty_tables=dirty_tables, create_tables=create_tables
        )
    elif type == "postgres":
        tpt_ctx = postgresql_transport(
            postgres_dsn, dirty_tables=dirty_tables, create_tables=create_tables
        )

    return tpt_ctx


default_backends = ["sqlite", "postgres", "rest-sqlite", "rest-postgres"]


def get_transport_fixture(
    backends: Sequence[str] | None = None,
    scope: str = "function",
    dirty_tables: list[str] | None = None,
    create_tables: bool = True,
):
    if backends is None:
        backends = default_backends.copy()

    def transport_fixture(
        request: pytest.FixtureRequest, clean_postgres_database: None
    ) -> Generator[Platform, None, None]:
        try:
            with transport(
                request, dirty_tables=dirty_tables, create_tables=create_tables
            ) as t:
                yield t
        except OperationalError as e:
            pytest.skip("Database is not reachable: " + str(e))

    return pytest.fixture(params=backends, scope=scope)(transport_fixture)


def get_platform_fixture(
    backends: Sequence[str] | None = None,
    scope: str = "function",
):
    if backends is None:
        backends = default_backends.copy()

    def platform_fixture(
        request: pytest.FixtureRequest, clean_postgres_database: None
    ) -> Generator[Platform, None, None]:
        with transport(request) as t:
            yield Platform(_backend=Backend(t))

    return pytest.fixture(params=backends, scope=scope)(platform_fixture)


@pytest.fixture(scope="session", autouse=True)
def clean_postgres_database(
    request: pytest.FixtureRequest,
) -> None:
    if "postgres" in get_active_backends(["postgres"], request):
        postgres_dsn = request.config.option.postgres_dsn
        engine = sa.create_engine(postgres_dsn)
        try:
            with engine.connect() as conn:
                conn.execute(
                    sa.text("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
                )
                conn.commit()
        except OperationalError:
            pass
