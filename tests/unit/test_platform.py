from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast
from unittest import mock

import pytest
import sqlalchemy as sa
from pydantic import HttpUrl

import ixmp4.core.platform as platform_module
from ixmp4.base_exceptions import ImproperlyConfigured, PlatformNotFound
from ixmp4.conf.platforms import PlatformConnectionInfo
from ixmp4.conf.settings import Settings
from ixmp4.core.platform import Platform
from ixmp4.transport import Transport


class _PlatformConnectionInfo:
    name = "dev"
    dsn = "postgresql://user:{env:MISSING}@db/test"
    url = "https://example.test"


class _SettingsStub:
    client = SimpleNamespace()
    check_alembic_version = True

    def get_credentials(self) -> dict[str, dict[str, str]]:
        return {"default": {"username": "u", "password": "p"}}

    def get_client_auth(self, cred_dict: dict[str, str]) -> object:
        return object()


class DummyTransport(Transport):
    def check_versioning_compatiblity(self) -> None:
        return None


def patch_platform_dependencies(monkeypatch: pytest.MonkeyPatch) -> type:
    class FakeBackend:
        def __init__(self, transport: object):
            self.transport = transport

    monkeypatch.setattr(platform_module, "Backend", FakeBackend)

    for attr in (
        "RunServiceFacade",
        "PlatformIamcData",
        "ModelServiceFacade",
        "RegionServiceFacade",
        "ScenarioServiceFacade",
        "UnitServiceFacade",
        "PlatformRunMetaFacade",
    ):
        monkeypatch.setattr(
            platform_module,
            attr,
            lambda backend, attr=attr: SimpleNamespace(name=attr, backend=backend),
        )

    return FakeBackend


def test_platform_init_with_name_uses_init_backend(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    FakeBackend = patch_platform_dependencies(monkeypatch)
    settings = Settings(storage_directory=tmp_path)
    backend = FakeBackend(mock.sentinel.transport)

    monkeypatch.setattr(
        Platform,
        "init_backend",
        lambda self, name: backend,
    )

    platform = Platform("demo", settings=settings)

    assert platform.backend is backend
    assert platform.settings is settings


def test_platform_init_with_transport_wraps_backend(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    FakeBackend = patch_platform_dependencies(monkeypatch)
    settings = Settings(storage_directory=tmp_path)
    transport = DummyTransport()

    platform = Platform(transport, settings=settings)

    assert isinstance(platform.backend, FakeBackend)
    assert platform.backend.transport is transport


def test_platform_init_with_backend_uses_backend_as_is(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    FakeBackend = patch_platform_dependencies(monkeypatch)
    settings = Settings(storage_directory=tmp_path)
    backend = FakeBackend(mock.sentinel.transport)

    platform = Platform(backend, settings=settings)

    assert platform.backend is backend


def test_platform_init_with_invalid_input_raises_type_error(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    patch_platform_dependencies(monkeypatch)
    settings = Settings(storage_directory=tmp_path)

    with pytest.raises(
        TypeError,
        match=r"must be a string \(platform name\), Transport, or Backend, not int",
    ):
        Platform(123, settings=settings)  # type: ignore[arg-type]


def test_platform_init_backend_prefers_toml_connection(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    FakeBackend = patch_platform_dependencies(monkeypatch)
    settings = Settings(storage_directory=tmp_path)
    platform = Platform(FakeBackend(mock.sentinel.transport), settings=settings)

    ci_toml = SimpleNamespace(dsn="sqlite:///:memory:")
    monkeypatch.setattr(platform, "get_toml_platform_ci", lambda name: ci_toml)

    def manager_should_not_be_called(name: str) -> None:
        raise AssertionError("manager lookup should not be called")

    monkeypatch.setattr(
        platform,
        "get_manager_platform_ci",
        manager_should_not_be_called,
    )
    monkeypatch.setattr(
        platform,
        "get_transport",
        lambda ci: mock.sentinel.transport,
    )

    backend = platform.init_backend("demo")

    assert isinstance(backend, FakeBackend)
    assert backend.transport is mock.sentinel.transport


def test_platform_init_backend_falls_back_to_manager(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    FakeBackend = patch_platform_dependencies(monkeypatch)
    settings = Settings(storage_directory=tmp_path)
    platform = Platform(FakeBackend(mock.sentinel.transport), settings=settings)

    ci_manager = SimpleNamespace(dsn="sqlite:///:memory:")
    monkeypatch.setattr(platform, "get_toml_platform_ci", lambda name: None)
    monkeypatch.setattr(platform, "get_manager_platform_ci", lambda name: ci_manager)
    monkeypatch.setattr(
        platform,
        "get_transport",
        lambda ci: mock.sentinel.transport,
    )

    backend = platform.init_backend("demo")

    assert isinstance(backend, FakeBackend)
    assert backend.transport is mock.sentinel.transport


def test_platform_init_backend_raises_when_not_found(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    FakeBackend = patch_platform_dependencies(monkeypatch)
    settings = Settings(storage_directory=tmp_path)
    platform = Platform(FakeBackend(mock.sentinel.transport), settings=settings)

    monkeypatch.setattr(platform, "get_toml_platform_ci", lambda name: None)
    monkeypatch.setattr(platform, "get_manager_platform_ci", lambda name: None)

    with pytest.raises(PlatformNotFound, match="Platform 'demo' was not found"):
        platform.init_backend("demo")


def test_platform_get_transport_uses_httpx_for_http_dsn(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    FakeBackend = patch_platform_dependencies(monkeypatch)
    settings = Settings(storage_directory=tmp_path)
    platform = Platform(FakeBackend(mock.sentinel.transport), settings=settings)

    credentials = SimpleNamespace()
    platform.settings = cast(
        Settings,
        SimpleNamespace(
            client=mock.sentinel.client,
            check_alembic_version=True,
            get_credentials=lambda: SimpleNamespace(get=lambda key: credentials),
            get_client_auth=lambda cred: mock.sentinel.auth,
        ),
    )

    calls: list[dict[str, object]] = []

    def fake_from_url(
        url: str,
        settings: object,
        auth: object,
    ) -> object:
        calls.append({"url": url, "settings": settings, "auth": auth})
        return mock.sentinel.transport

    monkeypatch.setattr("ixmp4.core.platform.HttpxTransport.from_url", fake_from_url)

    result = platform.get_transport(
        SimpleNamespace(dsn="https://platform.example/v1"),
        http_credentials="demo",
    )

    assert result is mock.sentinel.transport
    assert calls == [
        {
            "url": "https://platform.example/v1",
            "settings": mock.sentinel.client,
            "auth": mock.sentinel.auth,
        }
    ]


def test_platform_get_transport_uses_direct_transport_for_database_dsn(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    FakeBackend = patch_platform_dependencies(monkeypatch)
    settings = Settings(storage_directory=tmp_path)
    platform = Platform(FakeBackend(mock.sentinel.transport), settings=settings)

    platform.settings = cast(
        Settings,
        SimpleNamespace(
            check_alembic_version=True,
        ),
    )

    calls: list[dict[str, object]] = []

    def fake_from_dsn(
        cls: type,
        /,
        dsn: str,
        *args: object,
        **kwargs: object,
    ) -> object:
        calls.append({"dsn": dsn, "kwargs": kwargs})
        return mock.sentinel.transport

    monkeypatch.setattr(
        "ixmp4.core.platform.DirectTransport.from_dsn",
        classmethod(fake_from_dsn),
    )

    result = platform.get_transport(SimpleNamespace(dsn="sqlite:///:memory:"))

    assert result is mock.sentinel.transport
    assert calls == [
        {
            "dsn": "sqlite:///:memory:",
            "kwargs": {"check_alembic_version": True},
        }
    ]


def test_get_transport_falls_back_to_http_on_direct_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    platform = Platform.__new__(Platform)
    platform.settings = cast(Settings, _SettingsStub())

    def _raise_direct_error(dsn: str, **kw: Any) -> object:
        raise ImproperlyConfigured(
            "Cannot resolve DSN environment variable placeholder(s)."
        )

    expected_transport = object()

    def _http_from_url(url: str, settings: object, auth: object) -> object:
        assert url == "https://example.test"
        return expected_transport

    monkeypatch.setattr(
        "ixmp4.core.platform.DirectTransport.from_dsn", _raise_direct_error
    )
    monkeypatch.setattr("ixmp4.core.platform.HttpxTransport.from_url", _http_from_url)

    transport = platform.get_transport(
        cast(PlatformConnectionInfo, _PlatformConnectionInfo())
    )
    assert transport is expected_transport


def test_get_transport_falls_back_to_http_on_sqlalchemy_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    platform = Platform.__new__(Platform)
    platform.settings = cast(Settings, _SettingsStub())

    def _raise_direct_error(dsn: str, **kw: Any) -> object:
        raise sa.exc.OperationalError("SELECT 1", {}, Exception("boom"))

    expected_transport = object()

    def _http_from_url(url: str, settings: object, auth: object) -> object:
        assert url == "https://example.test"
        return expected_transport

    monkeypatch.setattr(
        "ixmp4.core.platform.DirectTransport.from_dsn", _raise_direct_error
    )
    monkeypatch.setattr("ixmp4.core.platform.HttpxTransport.from_url", _http_from_url)

    transport = platform.get_transport(
        cast(PlatformConnectionInfo, _PlatformConnectionInfo())
    )
    assert transport is expected_transport


def test_get_transport_does_not_fallback_on_value_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    platform = Platform.__new__(Platform)
    platform.settings = cast(Settings, _SettingsStub())

    def _raise_direct_error(dsn: str, **kw: Any) -> object:
        raise ValueError("Unrelated incident.")

    monkeypatch.setattr(
        "ixmp4.core.platform.DirectTransport.from_dsn", _raise_direct_error
    )

    with pytest.raises(ValueError):
        platform.get_transport(cast(PlatformConnectionInfo, _PlatformConnectionInfo()))


def test_platform_fails_with_invalid_first_arg() -> None:
    with pytest.raises(TypeError):
        Platform(123)  # type: ignore[arg-type]


def test_platform_overrides_settings(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "ixmp4.core.platform.Platform.init_backend",
        lambda *a, **k: mock.Mock(),
    )

    ovr_settings = Settings(manager_url=HttpUrl("https://custom.manager.ac.at/v1/"))
    platform = Platform("fake-platform", settings=ovr_settings)

    assert platform.settings is ovr_settings
    assert platform.settings.manager_url == ovr_settings.manager_url
