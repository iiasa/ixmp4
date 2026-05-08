import datetime
from email.utils import format_datetime
from types import SimpleNamespace
from typing import Any, cast
from unittest import mock

import httpx
import pytest
import sqlalchemy as sa
from pydantic import SecretStr
from toolkit.auth.context import AuthorizationContext, PlatformProtocol

import ixmp4.transport as transport_module
from ixmp4._version import __version__
from ixmp4.base_exceptions import ImproperlyConfigured
from ixmp4.conf.settings import ClientSettings
from ixmp4.core.exceptions import OperationNotSupported, ProgrammingError
from ixmp4.transport import (
    AuthorizedTransport,
    DirectTransport,
    HttpxTransport,
    Transport,
    cached_create_engine,
)


class MockManagerAuth:
    def __init__(self, base_url: str, user: object | None = None) -> None:
        self.client = SimpleNamespace(base_url=base_url)
        self.access_token = SimpleNamespace(user=user)


def fake_executor(max_workers: int) -> SimpleNamespace:
    return SimpleNamespace(max_workers=max_workers, shutdown=lambda wait=False: None)


def test_transport_base_methods_raise_or_describe() -> None:
    transport = Transport()

    assert str(transport) == "<Transport>"
    with pytest.raises(NotImplementedError):
        transport.check_versioning_compatiblity()


def test_cached_create_engine_uses_cache() -> None:
    cached_create_engine.cache_clear()

    engine = cached_create_engine("sqlite:///:memory:")
    cached = cached_create_engine("sqlite:///:memory:")

    assert engine is cached
    assert isinstance(engine.pool, sa.pool.impl.NullPool)


def test_direct_transport_handles_bound_and_unbound_sessions() -> None:
    unbound = DirectTransport(transport_module.Session())
    assert unbound.get_database_url() is None
    assert unbound.get_engine_info() == ""
    assert str(unbound) == "<DirectTransport >"

    bound = DirectTransport.from_dsn("sqlite:///:memory:")
    assert str(bound.get_database_url()) == "sqlite:///:memory:"
    assert "dialect=sqlite" in bound.get_engine_info()
    assert str(bound).startswith("<DirectTransport dialect=sqlite")
    bound.close()


def test_direct_transport_check_dsn_and_invalid_dsn() -> None:
    assert (
        DirectTransport.check_dsn("postgresql://user:pass@example.test/db")
        == "postgresql+psycopg://user:pass@example.test/db"
    )
    assert DirectTransport.check_dsn("sqlite:///:memory:") == "sqlite:///:memory:"

    with pytest.raises(ProgrammingError, match="Unsupported database dialect"):
        DirectTransport.from_dsn("mysql://example.test/db")


def test_direct_transport_from_dsn_uses_postgresql_engine(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []
    fake_engine = sa.create_engine("sqlite:///:memory:")

    def fake_create_postgresql_engine(
        cls: type[DirectTransport], dsn: str
    ) -> sa.Engine:
        calls.append(dsn)
        return fake_engine

    monkeypatch.setattr(
        DirectTransport,
        "create_postgresql_engine",
        classmethod(fake_create_postgresql_engine),
    )

    transport = DirectTransport.from_dsn("postgresql://user:pass@example.test/db")

    assert calls == ["postgresql+psycopg://user:pass@example.test/db"]
    assert transport.session.bind is fake_engine
    transport.close()


def test_direct_transport_versioning_requires_postgresql() -> None:
    transport = DirectTransport.from_dsn("sqlite:///:memory:")

    with pytest.raises(OperationNotSupported, match="Versioning is only enabled"):
        transport.check_versioning_compatiblity()

    transport.close()


def test_direct_transport_check_alembic_version_succeeds_when_matching(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_controller = SimpleNamespace(
        get_database_revision=lambda: "rev1",
        get_head_revision=lambda: "rev1",
        list_revisions=lambda: [SimpleNamespace(revision="rev1")],
    )
    monkeypatch.setattr(
        transport_module.DirectTransport,
        "get_alembic_controller",
        lambda self, dsn: fake_controller,
    )

    engine = sa.create_engine("sqlite:///:memory:")
    with engine.begin() as conn:
        conn.execute(sa.text("CREATE TABLE alembic_version (version_num VARCHAR(32))"))
        conn.execute(
            sa.text("INSERT INTO alembic_version (version_num) VALUES ('rev1')")
        )

    session = transport_module.Session(bind=engine)
    transport = DirectTransport(session, check_alembic_version=True)
    transport.close()


def test_direct_transport_check_alembic_version_raises_on_mismatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_controller = SimpleNamespace(
        get_database_revision=lambda: "different",
        get_head_revision=lambda: "expected",
        list_revisions=lambda: [
            SimpleNamespace(revision="expected"),
            SimpleNamespace(revision="base"),
        ],
    )
    monkeypatch.setattr(
        transport_module.DirectTransport,
        "get_alembic_controller",
        lambda self, dsn: fake_controller,
    )

    engine = sa.create_engine("sqlite:///:memory:")
    with engine.begin() as conn:
        conn.execute(sa.text("CREATE TABLE alembic_version (version_num VARCHAR(32))"))
        conn.execute(
            sa.text("INSERT INTO alembic_version (version_num) VALUES ('different')")
        )

    session = transport_module.Session(bind=engine)
    with pytest.raises(ImproperlyConfigured, match="Database schema version mismatch"):
        DirectTransport(session, check_alembic_version=True)


def test_direct_transport_check_alembic_version_can_be_skipped(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_controller = SimpleNamespace(
        get_database_revision=lambda: "different",
        get_head_revision=lambda: "expected",
        list_revisions=lambda: [SimpleNamespace(revision="expected")],
    )
    monkeypatch.setattr(
        transport_module.DirectTransport,
        "get_alembic_controller",
        lambda self, dsn: fake_controller,
    )

    engine = sa.create_engine("sqlite:///:memory:")
    with engine.begin() as conn:
        conn.execute(sa.text("CREATE TABLE alembic_version (version_num VARCHAR(32))"))
        conn.execute(
            sa.text("INSERT INTO alembic_version (version_num) VALUES ('different')")
        )

    session = transport_module.Session(bind=engine)
    transport = DirectTransport(session, check_alembic_version=False)
    transport.close()


def test_direct_transport_check_alembic_version_requires_table(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine = sa.create_engine("sqlite:///:memory:")
    session = transport_module.Session(bind=engine)

    with pytest.raises(ImproperlyConfigured, match="alembic_version"):
        DirectTransport(session, check_alembic_version=True)


def test_direct_transport_check_alembic_version_older_db_revision_guidance(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_controller = SimpleNamespace(
        get_database_revision=lambda: "old-rev",
        get_head_revision=lambda: "new-rev",
        list_revisions=lambda: [
            SimpleNamespace(revision="new-rev"),
            SimpleNamespace(revision="old-rev"),
        ],
    )
    monkeypatch.setattr(
        transport_module.DirectTransport,
        "get_alembic_controller",
        lambda self, dsn: fake_controller,
    )

    engine = sa.create_engine("sqlite:///:memory:")
    with engine.begin() as conn:
        conn.execute(sa.text("CREATE TABLE alembic_version (version_num VARCHAR(32))"))
        conn.execute(
            sa.text("INSERT INTO alembic_version (version_num) VALUES ('old-rev')")
        )

    session = transport_module.Session(bind=engine)
    with pytest.raises(
        ImproperlyConfigured,
        match="Upgrade the database.*or downgrade ixmp4",
    ):
        DirectTransport(session, check_alembic_version=True)


def test_direct_transport_check_alembic_version_requires_head_revision(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_controller = SimpleNamespace(
        get_database_revision=lambda: "rev1",
        get_head_revision=lambda: None,
        list_revisions=lambda: [SimpleNamespace(revision="rev1")],
    )
    monkeypatch.setattr(
        transport_module.DirectTransport,
        "get_alembic_controller",
        lambda self, dsn: fake_controller,
    )

    engine = sa.create_engine("sqlite:///:memory:")
    with engine.begin() as conn:
        conn.execute(sa.text("CREATE TABLE alembic_version (version_num VARCHAR(32))"))
        conn.execute(
            sa.text("INSERT INTO alembic_version (version_num) VALUES ('rev1')")
        )

    session = transport_module.Session(bind=engine)
    with pytest.raises(
        ImproperlyConfigured,
        match="Could not determine the expected alembic revision",
    ):
        DirectTransport(session, check_alembic_version=True)


def test_direct_transport_check_alembic_version_rejects_non_unique_head(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_controller = SimpleNamespace(
        get_database_revision=lambda: "rev1",
        get_head_revision=lambda: ("head-1",),
        list_revisions=lambda: [SimpleNamespace(revision="rev1")],
    )
    monkeypatch.setattr(
        transport_module.DirectTransport,
        "get_alembic_controller",
        lambda self, dsn: fake_controller,
    )

    engine = sa.create_engine("sqlite:///:memory:")
    with engine.begin() as conn:
        conn.execute(sa.text("CREATE TABLE alembic_version (version_num VARCHAR(32))"))
        conn.execute(
            sa.text("INSERT INTO alembic_version (version_num) VALUES ('rev1')")
        )

    session = transport_module.Session(bind=engine)
    with pytest.raises(
        ImproperlyConfigured,
        match="Could not determine a unique expected alembic revision",
    ):
        DirectTransport(session, check_alembic_version=True)


def test_direct_transport_check_alembic_version_requires_current_revision(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_controller = SimpleNamespace(
        get_database_revision=lambda: None,
        get_head_revision=lambda: "rev1",
        list_revisions=lambda: [SimpleNamespace(revision="rev1")],
    )
    monkeypatch.setattr(
        transport_module.DirectTransport,
        "get_alembic_controller",
        lambda self, dsn: fake_controller,
    )

    engine = sa.create_engine("sqlite:///:memory:")
    with engine.begin() as conn:
        conn.execute(sa.text("CREATE TABLE alembic_version (version_num VARCHAR(32))"))

    session = transport_module.Session(bind=engine)
    with pytest.raises(
        ImproperlyConfigured,
        match="no alembic revision entry was found in 'alembic_version'",
    ):
        DirectTransport(session, check_alembic_version=True)


def test_authorized_transport_string_includes_user_and_platform() -> None:
    session = transport_module.Session(bind=sa.create_engine("sqlite:///:memory:"))
    transport = AuthorizedTransport(
        session=session,
        auth_ctx=cast(AuthorizationContext, SimpleNamespace(user="alice")),
        platform=cast(PlatformProtocol, SimpleNamespace(id="demo-platform")),
    )

    assert transport.unauthorized_transport.session is session
    assert "user=alice" in str(transport)
    assert "platform=demo-platform" in str(transport)

    transport.close()


def test_httpx_transport_check_root_rejects_manager_url_mismatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(transport_module, "ManagerAuth", MockManagerAuth)
    monkeypatch.setattr(transport_module, "ThreadPoolExecutor", fake_executor)
    monkeypatch.setattr(
        HttpxTransport, "raise_service_exception", lambda self, response: None
    )

    response = mock.Mock()
    response.json.return_value = {
        "slug": "demo",
        "name": "Demo",
        "version": __version__,
        "is_managed": True,
        "manager_url": "https://manager.server.test/api",
        "utcnow": "2024-01-01T00:00:00Z",
    }
    client = SimpleNamespace(
        base_url="https://platform.server.test/api",
        auth=MockManagerAuth("https://manager.client.test/api"),
        request=mock.Mock(return_value=response),
    )

    with pytest.raises(ImproperlyConfigured, match="mismatching Manager URL"):
        HttpxTransport(cast(Any, client), ClientSettings())


def test_httpx_transport_from_url_uses_self_signed_auth_when_configured(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(HttpxTransport, "check_root", lambda self: None)
    monkeypatch.setattr(transport_module, "ThreadPoolExecutor", fake_executor)

    captured: dict[str, object] = {}

    def fake_client(**kwargs: object) -> SimpleNamespace:
        captured.update(kwargs)
        return SimpleNamespace(base_url=kwargs["base_url"], auth=kwargs["auth"])

    monkeypatch.setattr(httpx, "Client", fake_client)

    settings = ClientSettings(
        concurrency=3,
        retries=5,
        timeout=12,
        secret_hs256=SecretStr("changeme"),
    )
    transport = HttpxTransport.from_url("https://platform.server.test/api", settings)

    assert str(transport.url) == "https://platform.server.test/api"
    assert captured["base_url"] == "https://platform.server.test/api"
    assert captured["http2"] is True
    assert captured["auth"].__class__.__name__ == "SelfSignedAuth"


def test_httpx_transport_from_asgi_sets_direct_transport(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(transport_module, "ThreadPoolExecutor", fake_executor)
    captured: dict[str, object] = {}

    def fake_test_client(**kwargs: object) -> SimpleNamespace:
        captured.update(kwargs)
        return SimpleNamespace(base_url=kwargs["base_url"], auth=None)

    monkeypatch.setattr(transport_module, "TestClient", fake_test_client)

    direct = DirectTransport.from_dsn("sqlite:///:memory:")
    transport = HttpxTransport.from_asgi(
        asgi=mock.sentinel.asgi,
        settings=ClientSettings(),
        direct=direct,
        raise_server_exceptions=False,
    )

    assert captured["app"] is mock.sentinel.asgi
    assert captured["raise_server_exceptions"] is False
    assert transport.direct is direct
    assert "user=None" in str(transport)
    direct.close()


def test_httpx_transport_string_uses_manager_user(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(transport_module, "ManagerAuth", MockManagerAuth)
    monkeypatch.setattr(transport_module, "ThreadPoolExecutor", fake_executor)

    transport = HttpxTransport(
        client=cast(
            Any,
            SimpleNamespace(
                base_url="https://platform.server.test/api",
                auth=MockManagerAuth("https://manager.client.test/api", user="alice"),
            ),
        ),
        settings=ClientSettings(),
        check_root=False,
    )

    assert "user=alice" in str(transport)


def test_httpx_transport_request_retries_429_with_retry_after(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sleep_calls: list[float] = []
    monkeypatch.setattr(
        transport_module, "time", SimpleNamespace(sleep=sleep_calls.append)
    )
    monkeypatch.setattr(transport_module, "ThreadPoolExecutor", fake_executor)

    responses = [
        SimpleNamespace(status_code=429, headers={"retry-after": "2"}),
        SimpleNamespace(status_code=200, headers={}),
    ]
    client = SimpleNamespace(
        base_url="https://platform.server.test/api",
        auth=None,
        request=mock.Mock(side_effect=responses),
    )
    transport = HttpxTransport(
        client=cast(Any, client),
        settings=ClientSettings(retries=3),
        check_root=False,
    )

    response = transport.request("GET", "/demo")

    assert response.status_code == 200
    assert client.request.call_count == 2
    assert sleep_calls == [2.0]


def test_httpx_transport_request_returns_last_429_after_retry_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sleep_calls: list[float] = []
    monkeypatch.setattr(
        transport_module, "time", SimpleNamespace(sleep=sleep_calls.append)
    )
    monkeypatch.setattr(transport_module, "ThreadPoolExecutor", fake_executor)

    responses = [
        SimpleNamespace(status_code=429, headers={}),
        SimpleNamespace(status_code=429, headers={}),
        SimpleNamespace(status_code=429, headers={}),
    ]
    client = SimpleNamespace(
        base_url="https://platform.server.test/api",
        auth=None,
        request=mock.Mock(side_effect=responses),
    )
    transport = HttpxTransport(
        client=cast(Any, client),
        settings=ClientSettings(retries=2),
        check_root=False,
    )

    response = transport.request("GET", "/demo")

    assert response.status_code == 429
    assert client.request.call_count == 3
    assert sleep_calls == [0.5, 1.0]


def test_get_retry_delay_seconds_parses_http_date() -> None:
    client = SimpleNamespace(
        base_url="https://platform.server.test/api",
        auth=None,
    )
    transport = HttpxTransport(
        client=cast(Any, client),
        settings=ClientSettings(retries=2),
        check_root=False,
    )
    # Use a date in the past so delay clamps to 0.0 regardless of wall-clock time.
    http_date = format_datetime(
        datetime.datetime(2000, 1, 1, tzinfo=datetime.timezone.utc)
    )
    mock_response = httpx.Response(
        status_code=429,
        text="Too many requests.",
        headers={"retry-after": http_date},
    )
    assert transport.get_retry_delay_seconds(mock_response, 0) == 0.0
