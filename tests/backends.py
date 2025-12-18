import contextlib
from typing import Callable, Generator, Iterator, Literal, Sequence, cast

import pytest
import sqlalchemy as sa
from sqlalchemy.exc import OperationalError
from toolkit.auth.context import AuthorizationContext, PlatformProtocol

from ixmp4.backend import Backend
from ixmp4.conf.settings import Settings
from ixmp4.core.platform import Platform
from ixmp4.db.models import get_metadata
from ixmp4.server import Ixmp4Server
from ixmp4.transport import (
    AuthorizedTransport,
    DirectTransport,
    HttpxTransport,
    Transport,
)

backend_choices = ("sqlite", "postgres", "rest-sqlite", "rest-postgres")
BackendTypeStr = Literal["sqlite", "postgres", "rest-sqlite", "rest-postgres"]
ScopeStr = Literal["session", "package", "module", "class", "function"]


def validate_backend_type(
    type_: str,
) -> BackendTypeStr:
    if type_ not in backend_choices:
        raise ValueError(
            f"Backend type '{type_}' is not valid. Expected one of: "
            + ", ".join(backend_choices),
        )
    else:
        return cast(BackendTypeStr, type_)


def get_requested_backends(
    req_or_meta: pytest.FixtureRequest | pytest.Metafunc,
) -> list[BackendTypeStr]:
    be_args = req_or_meta.config.option.backend.split(",")
    backend_types = [validate_backend_type(t.strip()) for t in be_args]
    return backend_types


def get_active_backends(
    cand: Sequence[BackendTypeStr], request: pytest.FixtureRequest
) -> list[BackendTypeStr]:
    return list(set(cand) & set(get_requested_backends(request)))


def get_sorted_tables(meta: sa.MetaData, tables: list[str] | None) -> list[sa.Table]:
    if tables is None:
        return meta.sorted_tables

    sorted_tables = []
    for table in meta.sorted_tables:
        if table.name in tables:
            sorted_tables.append(table)
    return sorted_tables


def create_model_tables(bind: sa.Engine | sa.Connection) -> None:
    meta = get_metadata()
    meta.create_all(bind=bind, tables=meta.sorted_tables, checkfirst=True)


def drop_model_tables(bind: sa.Engine | sa.Connection) -> None:
    meta = get_metadata()
    meta.drop_all(
        bind=bind,
        checkfirst=True,
    )


def get_direct_transport(
    dsn: str,
    auth_ctx: AuthorizationContext | None,
    platform_info: PlatformProtocol | None,
) -> DirectTransport:
    if auth_ctx is None:
        return DirectTransport.from_dsn(dsn)
    else:
        if platform_info is None:
            raise ValueError(
                "Provide the `platform_info` fixture to test authorized transports."
            )
        return AuthorizedTransport.from_dsn(
            dsn, auth_ctx=auth_ctx, platform=platform_info
        )


@contextlib.contextmanager
def postgresql_transport(
    dsn: str,
    *,
    settings: Settings,
    auth_ctx: AuthorizationContext | None,
    platform_info: PlatformProtocol | None,
    create_tables: bool = True,
) -> Generator[DirectTransport, None, None]:
    pgsql = get_direct_transport(dsn, auth_ctx, platform_info)
    assert pgsql.session.bind is not None
    if create_tables:
        create_model_tables(pgsql.session.bind.engine)
    yield pgsql
    pgsql.close()
    drop_model_tables(pgsql.session.bind.engine)


@contextlib.contextmanager
def sqlite_transport(
    *,
    settings: Settings,
    auth_ctx: AuthorizationContext | None,
    platform_info: PlatformProtocol | None,
    create_tables: bool = True,
) -> Generator[DirectTransport, None, None]:
    sqlite = get_direct_transport("sqlite:///:memory:", auth_ctx, platform_info)
    assert sqlite.session.bind is not None
    if create_tables:
        create_model_tables(sqlite.session.bind.engine)
    yield sqlite
    sqlite.close()
    # dropping tables explicitly on sqlite results in a ResourceWarning.
    # since the db is in memory, we can just skip dropping the tables.
    # drop_model_tables(sqlite.session.bind.engine, dirty_tables)


@contextlib.contextmanager
def httpx_sqlite_transport(
        *,

    settings: Settings,
    auth_ctx: AuthorizationContext | None,
    platform_info: PlatformProtocol | None,
    create_tables: bool = True,
) -> Generator[HttpxTransport, None, None]:
    with sqlite_transport(
        settings=settings,
        auth_ctx=auth_ctx,
        platform_info=platform_info,
        create_tables=create_tables,
    ) as direct:
        with test_server(direct, settings) as server:
            httpx_sqlite = HttpxTransport.from_asgi(server.asgi_app, settings.client)
            httpx_sqlite.direct = direct
            yield httpx_sqlite


@contextlib.contextmanager
def httpx_postgresql_transport(
    dsn: str,
        *,

    settings: Settings,
    auth_ctx: AuthorizationContext | None,
    platform_info: PlatformProtocol | None,
    create_tables: bool = True,
) -> Generator[HttpxTransport, None, None]:
    with postgresql_transport(
        dsn,
        settings=settings,
        auth_ctx=auth_ctx,
        platform_info=platform_info,
        create_tables=create_tables,
    ) as direct:
        with test_server(direct, settings) as server:
            httpx_pgsql = HttpxTransport.from_asgi(server.asgi_app, settings.client)
            httpx_pgsql.direct = direct
            yield httpx_pgsql


@contextlib.contextmanager
def test_server(direct: DirectTransport, settings: Settings) -> Iterator[Ixmp4Server]:
    # NOTE: This currently re-builds the api for every test setup
    # TODO: upstream litestar adjustment to make this better?

    async def get_direct() -> DirectTransport:
        return direct

    server = Ixmp4Server(settings.server, override_transport=get_direct, debug=True)
    server.simulate_startup()
    yield server


def transport(
    request: pytest.FixtureRequest,
    create_tables: bool = True,
) -> contextlib._GeneratorContextManager[Transport, None, None]:
    postgres_dsn = request.config.option.postgres_dsn
    type = request.param
    active_backends = get_active_backends([type], request)
    settings = request.getfixturevalue("settings")
    auth_ctx = request.getfixturevalue("auth_ctx")
    platform_info = request.getfixturevalue("platform_info")

    if type not in active_backends:
        pytest.skip("Transport backend is not active. ")

    if type == "rest-sqlite":
        return httpx_sqlite_transport(
            settings=settings,
            auth_ctx=auth_ctx,
            platform_info=platform_info,
            create_tables=create_tables,
        )
    elif type == "rest-postgres":
        return httpx_postgresql_transport(
            postgres_dsn,
            settings=settings,
            auth_ctx=auth_ctx,
            platform_info=platform_info,
            create_tables=create_tables,
        )
    elif type == "sqlite":
        return sqlite_transport(
            settings=settings,
            auth_ctx=auth_ctx,
            platform_info=platform_info,
            create_tables=create_tables,
        )
    elif type == "postgres":
        return postgresql_transport(
            postgres_dsn,
            settings=settings,
            auth_ctx=auth_ctx,
            platform_info=platform_info,
            create_tables=create_tables,
        )
    else:
        raise ValueError(
            f"Backend type '{type}' is not valid. Expected one of: "
            + ", ".join(backend_choices),
        )


default_backends = ["sqlite", "postgres", "rest-sqlite", "rest-postgres"]


def get_transport_fixture(
    backends: Sequence[str] | None = None,
    scope: ScopeStr = "function",
    create_tables: bool = True,
) -> Callable[..., Transport]:
    if backends is None:
        backends = default_backends

    def transport_fixture(
        request: pytest.FixtureRequest,
    ) -> Generator[Transport, None, None]:
        try:
            with transport(
                request,
                create_tables=create_tables,
            ) as t:
                yield t
        except OperationalError as e:
            pytest.skip("Database is not reachable: " + str(e))

    return pytest.fixture(None, params=list(backends), scope=scope)(transport_fixture)


def get_platform_fixture(
    backends: Sequence[str] | None = None,
    scope: ScopeStr = "function",
) -> Callable[..., Platform]:
    if backends is None:
        backends = default_backends.copy()

    def platform_fixture(
        request: pytest.FixtureRequest,
    ) -> Generator[Platform, None, None]:
        with transport(request) as t:
            yield Platform(_backend=Backend(t))

    return pytest.fixture(params=list(backends), scope=scope)(platform_fixture)


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
