"""Unit tests for ixmp4.server.v1 – get_platform, on_startup,
yield_session, get_transport, and the service_exception_handler."""

import asyncio
from types import SimpleNamespace
from unittest import mock

import pytest
import sqlalchemy as sa
from pydantic import SecretStr

import ixmp4.server.v1 as v1_module
from ixmp4.conf.settings import ServerSettings
from ixmp4.core.exceptions import Forbidden, PlatformNotFound
from ixmp4.server.v1 import V1HttpApi
from ixmp4.transport import AuthorizedTransport, DirectTransport


def _make_settings(**overrides: object) -> ServerSettings:
    return ServerSettings(**overrides)  # type: ignore[arg-type]


def _make_state(
    manager_platforms: object = None,
    toml_platforms: object = None,
) -> SimpleNamespace:
    return SimpleNamespace(
        manager_platforms=manager_platforms,
        toml_platforms=toml_platforms,
    )


class TestYieldSession:
    def test_yield_session_provides_and_closes_a_session(self) -> None:
        """yield_session yields an ORM session then cleans up."""
        from ixmp4.server.v1 import yield_session

        async def _run() -> None:
            async with yield_session("sqlite:///:memory:") as session:
                assert session is not None
                session.execute(sa.text("SELECT 1"))

        asyncio.run(_run())


class TestGetTransport:
    def test_get_transport_yields_direct_transport_when_unauthenticated(self) -> None:
        """no auth → DirectTransport is yielded."""
        from ixmp4.server.v1 import get_transport

        platform = SimpleNamespace(dsn="sqlite:///:memory:")
        request = SimpleNamespace(auth=None)

        async def _run() -> None:
            gen = get_transport(platform, request)  # type: ignore[arg-type]
            transport = await gen.__anext__()
            assert isinstance(transport, DirectTransport)
            await gen.aclose()

        asyncio.run(_run())

    def test_get_transport_yields_authorized_transport_when_authenticated(self) -> None:
        """auth present → AuthorizedTransport is yielded."""
        from ixmp4.server.v1 import get_transport

        platform = SimpleNamespace(dsn="sqlite:///:memory:")
        request = SimpleNamespace(auth=SimpleNamespace(user="alice"))

        async def _run() -> None:
            gen = get_transport(platform, request)  # type: ignore[arg-type]
            transport = await gen.__anext__()
            assert isinstance(transport, AuthorizedTransport)
            await gen.aclose()

        asyncio.run(_run())


class TestGetPlatform:
    def test_get_unauthorized_platform_returns_platform_from_toml_sources(self) -> None:
        """unauthorized resolver returns a platform from toml_platforms."""
        from ixmp4.server.v1 import get_unauthorized_platform

        fake_platform = SimpleNamespace(dsn="sqlite:///:memory:")
        toml = SimpleNamespace(get_platform=mock.Mock(return_value=fake_platform))
        state = _make_state(toml_platforms=toml)

        async def _run() -> PlatformNotFound | None:
            result = await get_unauthorized_platform(state, "myplatform")  # type: ignore[arg-type]
            assert result is fake_platform
            return None

        asyncio.run(_run())

    def test_get_unauthorized_platform_raises_when_not_found(self) -> None:
        """unauthorized resolver raises when platform is not found."""
        from ixmp4.server.v1 import get_unauthorized_platform

        toml = SimpleNamespace(
            get_platform=mock.Mock(side_effect=PlatformNotFound("not here"))
        )
        state = _make_state(toml_platforms=toml)

        async def _run() -> None:
            with pytest.raises(PlatformNotFound):
                await get_unauthorized_platform(state, "ghost")  # type: ignore[arg-type]

        asyncio.run(_run())

    def test_get_unauthorized_platform_raises_when_dsn_is_http_url(self) -> None:
        """unauthorized resolver treats HTTP DSNs as not-found."""
        from ixmp4.server.v1 import get_unauthorized_platform

        fake_platform = SimpleNamespace(dsn="https://remote.server/api")
        toml = SimpleNamespace(get_platform=mock.Mock(return_value=fake_platform))
        state = _make_state(toml_platforms=toml)

        async def _run() -> None:
            with pytest.raises(PlatformNotFound):
                await get_unauthorized_platform(state, "remote")  # type: ignore[arg-type]

        asyncio.run(_run())

    def test_get_platform_returns_platform_from_toml_sources(self) -> None:
        """platform found in toml_platforms is returned."""
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

    def test_get_platform_raises_when_not_found(self) -> None:
        """platform not found in any source → PlatformNotFound."""
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

    def test_get_platform_raises_when_dsn_is_http_url(self) -> None:
        """platform with http:// DSN is treated as not-found."""
        from ixmp4.server.v1 import get_platform

        fake_platform = SimpleNamespace(dsn="https://remote.server/api")
        toml = SimpleNamespace(get_platform=mock.Mock(return_value=fake_platform))
        state = _make_state(toml_platforms=toml)
        request = SimpleNamespace(auth=None)

        async def _run() -> None:
            with pytest.raises(PlatformNotFound):
                await get_platform(state, "remote", request)  # type: ignore[arg-type]

        asyncio.run(_run())

    def test_get_platform_checks_manager_platforms_before_toml(self) -> None:
        """manager_platforms is checked first when auth is present."""
        from ixmp4.server.v1 import get_platform

        manager_platform = SimpleNamespace(
            dsn="sqlite:///:memory:", slug="test-platform"
        )
        manager = SimpleNamespace(get_platform=mock.Mock(return_value=manager_platform))
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

    def test_get_platform_falls_back_to_toml_when_manager_raises_not_found(
        self,
    ) -> None:
        """PlatformNotFound from manager_platforms is suppressed,
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

    def test_get_platform_keeps_manager_permission_check(self) -> None:
        """authorized resolver still checks manager access permissions."""
        from ixmp4.server.v1 import get_platform

        manager_platform = SimpleNamespace(
            dsn="sqlite:///:memory:", slug="test-platform"
        )
        manager = SimpleNamespace(get_platform=mock.Mock(return_value=manager_platform))
        state = _make_state(manager_platforms=manager, toml_platforms=None)

        auth = mock.Mock(user=None)
        auth.has_access_permission = mock.Mock(side_effect=Forbidden("not allowed"))
        request = SimpleNamespace(auth=auth)

        async def _run() -> None:
            with pytest.raises(Forbidden):
                await get_platform(state, "managed", request)  # type: ignore[arg-type]

        asyncio.run(_run())


class TestV1HttpApiOnStartup:
    def test_on_startup_without_manager_url_sets_state_to_none(self) -> None:
        """when manager_url is None, manager state is set to None."""
        from ixmp4.server.v1 import V1HttpApi

        settings = _make_settings()
        assert settings.manager_url is None

        api = V1HttpApi(
            settings, override_transport=mock.AsyncMock(), service_classes=[]
        )
        app = mock.Mock()
        app.state = SimpleNamespace()

        api.on_startup(app)

        assert app.state.manager_client is None
        assert app.state.manager_platforms is None
        assert app.state.settings is settings

    def test_on_startup_with_manager_url_creates_manager_client(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """with manager_url + secret, ManagerClient is created."""

        fake_client = mock.Mock()
        fake_platforms = mock.Mock()
        monkeypatch.setattr(
            v1_module, "ManagerClient", mock.Mock(return_value=fake_client)
        )
        monkeypatch.setattr(
            v1_module, "ManagerPlatforms", mock.Mock(return_value=fake_platforms)
        )

        settings = _make_settings(
            manager_url="https://manager.test/api",
            secret_hs256=SecretStr("a-sufficiently-long-secret-key-1234"),
        )
        api = V1HttpApi(
            settings, override_transport=mock.AsyncMock(), service_classes=[]
        )
        app = mock.Mock()
        app.state = SimpleNamespace()

        api.on_startup(app)

        assert app.state.manager_client is fake_client
        assert app.state.manager_platforms is fake_platforms
        assert app.state.settings is settings
