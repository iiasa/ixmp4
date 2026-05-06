"""Unit tests for ixmp4.server.v1 – get_platform, on_startup,
yield_session, get_transport, and the service_exception_handler."""

import asyncio
from types import SimpleNamespace
from unittest import mock

import pytest
import sqlalchemy as sa

from ixmp4.conf.settings import ServerSettings
from ixmp4.core.exceptions import Forbidden, PlatformNotFound
from ixmp4.transport import AuthorizedTransport, DirectTransport


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_settings(**overrides: object) -> ServerSettings:
    return ServerSettings(**overrides)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# yield_session
# ---------------------------------------------------------------------------


def test_yield_session_provides_and_closes_a_session() -> None:
    """Lines 100-106: yield_session yields an ORM session then cleans up."""
    from ixmp4.server.v1 import yield_session

    async def _run() -> None:
        async with yield_session("sqlite:///:memory:") as session:
            assert session is not None
            # must still be usable (not yet closed)
            session.execute(sa.text("SELECT 1"))

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# get_transport
# ---------------------------------------------------------------------------


def test_get_transport_yields_direct_transport_when_unauthenticated() -> None:
    """Line 121 (else branch): no auth → DirectTransport is yielded."""
    from ixmp4.server.v1 import get_transport

    platform = SimpleNamespace(dsn="sqlite:///:memory:")
    request = SimpleNamespace(auth=None)

    async def _run() -> None:
        gen = get_transport(platform, request)  # type: ignore[arg-type]
        transport = await gen.__anext__()
        assert isinstance(transport, DirectTransport)
        await gen.aclose()

    asyncio.run(_run())


def test_get_transport_yields_authorized_transport_when_authenticated() -> None:
    """Lines 113-117: auth present → AuthorizedTransport is yielded."""
    from ixmp4.server.v1 import get_transport

    platform = SimpleNamespace(dsn="sqlite:///:memory:")
    request = SimpleNamespace(auth=SimpleNamespace(user="alice"))

    async def _run() -> None:
        gen = get_transport(platform, request)  # type: ignore[arg-type]
        transport = await gen.__anext__()
        assert isinstance(transport, AuthorizedTransport)
        await gen.aclose()

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# get_platform
# ---------------------------------------------------------------------------


def _make_state(
    manager_platforms: object = None,
    toml_platforms: object = None,
) -> SimpleNamespace:
    return SimpleNamespace(
        manager_platforms=manager_platforms,
        toml_platforms=toml_platforms,
    )


def test_get_platform_returns_platform_from_toml_sources() -> None:
    """Lines 142-144: platform found in toml_platforms is returned."""
    from ixmp4.server.v1 import get_platform

    fake_platform = SimpleNamespace(dsn="sqlite:///:memory:")
    toml = SimpleNamespace(get_platform=mock.Mock(return_value=fake_platform))
    state = _make_state(toml_platforms=toml)
    request = SimpleNamespace(auth=None)

    async def _run() -> PlatformNotFound | None:
        result = await get_platform(state, "myplatform", request)  # type: ignore[arg-type]
        assert result is fake_platform
        return None

    asyncio.run(_run())


def test_get_platform_raises_when_not_found() -> None:
    """Lines 146-147: platform not found in any source → PlatformNotFound."""
    from ixmp4.server.v1 import get_platform

    toml = SimpleNamespace(
        get_platform=mock.Mock(side_effect=PlatformNotFound("not here"))
    )
    state = _make_state(toml_platforms=toml)
    request = SimpleNamespace(auth=None)

    async def _run() -> None:
        with pytest.raises(PlatformNotFound):
            await get_platform(state, "ghost", request)  # type: ignore[arg-type]

    asyncio.run(_run())


def test_get_platform_raises_when_dsn_is_http_url() -> None:
    """Lines 146-147: platform with http:// DSN is treated as not-found."""
    from ixmp4.server.v1 import get_platform

    fake_platform = SimpleNamespace(dsn="https://remote.server/api")
    toml = SimpleNamespace(get_platform=mock.Mock(return_value=fake_platform))
    state = _make_state(toml_platforms=toml)
    request = SimpleNamespace(auth=None)

    async def _run() -> None:
        with pytest.raises(PlatformNotFound):
            await get_platform(state, "remote", request)  # type: ignore[arg-type]

    asyncio.run(_run())


def test_get_platform_checks_manager_platforms_before_toml() -> None:
    """Lines 131-140: manager_platforms is checked first when auth is present."""
    from ixmp4.server.v1 import get_platform

    manager_platform = SimpleNamespace(dsn="sqlite:///:memory:")
    manager = SimpleNamespace(
        get_platform=mock.Mock(return_value=manager_platform)
    )
    toml = SimpleNamespace(get_platform=mock.Mock())
    state = _make_state(manager_platforms=manager, toml_platforms=toml)

    auth = mock.Mock()
    auth.has_access_permission = mock.Mock()
    request = SimpleNamespace(auth=auth)

    async def _run() -> None:
        result = await get_platform(state, "managed", request)  # type: ignore[arg-type]
        assert result is manager_platform
        toml.get_platform.assert_not_called()

    asyncio.run(_run())


def test_get_platform_falls_back_to_toml_when_manager_raises_not_found() -> None:
    """Lines 132-140: PlatformNotFound from manager_platforms is suppressed,
    then toml_platforms is tried."""
    from ixmp4.server.v1 import get_platform

    toml_platform = SimpleNamespace(dsn="sqlite:///:memory:")
    manager = SimpleNamespace(
        get_platform=mock.Mock(side_effect=PlatformNotFound("not in manager"))
    )
    toml = SimpleNamespace(get_platform=mock.Mock(return_value=toml_platform))
    state = _make_state(manager_platforms=manager, toml_platforms=toml)

    auth = mock.Mock()
    auth.has_access_permission = mock.Mock()
    request = SimpleNamespace(auth=auth)

    async def _run() -> None:
        result = await get_platform(state, "myplatform", request)  # type: ignore[arg-type]
        assert result is toml_platform

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# V1HttpApi.on_startup
# ---------------------------------------------------------------------------


def test_on_startup_without_manager_url_sets_state_to_none() -> None:
    """Lines 218-221: when manager_url is None, manager state is set to None."""
    from ixmp4.server.v1 import V1HttpApi

    settings = _make_settings()
    assert settings.manager_url is None

    api = V1HttpApi(settings, override_transport=mock.AsyncMock(), service_classes=[])
    app_state: dict[str, object] = {}
    app = mock.Mock()
    app.state = SimpleNamespace()

    api.on_startup(app)

    assert app.state.manager_client is None
    assert app.state.manager_platforms is None
    assert app.state.settings is settings


def test_on_startup_with_manager_url_creates_manager_client(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Lines 205-216: with manager_url + secret, ManagerClient is created."""
    import ixmp4.server.v1 as v1_module
    from ixmp4.server.v1 import V1HttpApi
    from pydantic import SecretStr

    fake_client = mock.Mock()
    fake_platforms = mock.Mock()
    monkeypatch.setattr(v1_module, "ManagerClient", mock.Mock(return_value=fake_client))
    monkeypatch.setattr(
        v1_module, "ManagerPlatforms", mock.Mock(return_value=fake_platforms)
    )

    settings = _make_settings(
        manager_url="https://manager.test/api",
        secret_hs256=SecretStr("a-sufficiently-long-secret-key-1234"),
    )
    api = V1HttpApi(settings, override_transport=mock.AsyncMock(), service_classes=[])
    app = mock.Mock()
    app.state = SimpleNamespace()

    api.on_startup(app)

    assert app.state.manager_client is fake_client
    assert app.state.manager_platforms is fake_platforms
    assert app.state.settings is settings


# ---------------------------------------------------------------------------
# V1HttpApi.service_exception_handler
# ---------------------------------------------------------------------------


def test_service_exception_handler_returns_correct_status_and_body() -> None:
    """Lines 228-233: exception is converted to a Response with the right status."""
    from ixmp4.server.v1 import V1HttpApi

    exc = PlatformNotFound("test-platform")
    request = mock.Mock()

    response = V1HttpApi.service_exception_handler(request, exc)

    assert response.status_code == exc.http_status_code


def test_service_exception_handler_works_for_forbidden() -> None:
    """service_exception_handler handles any Ixmp4Error subclass."""
    from ixmp4.server.v1 import V1HttpApi

    exc = Forbidden("not allowed")
    request = mock.Mock()

    response = V1HttpApi.service_exception_handler(request, exc)

    assert response.status_code == exc.http_status_code
