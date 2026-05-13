from types import SimpleNamespace
from typing import cast

import pytest
import sqlalchemy as sa

from ixmp4.base_exceptions import ImproperlyConfigured
from ixmp4.conf.platforms import PlatformConnectionInfo
from ixmp4.conf.settings import Settings
from ixmp4.core.platform import Platform


class _PlatformConnectionInfo:
    name = "dev"
    dsn = "postgresql://user:{env:MISSING}@db/test"
    url = "https://example.test"


class _SettingsStub:
    client = SimpleNamespace()

    def get_credentials(self) -> dict[str, dict[str, str]]:
        return {"default": {"username": "u", "password": "p"}}

    def get_client_auth(self, cred_dict: dict[str, str]) -> object:
        return object()


def test_get_transport_falls_back_to_http_on_direct_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    platform = Platform.__new__(Platform)
    platform.settings = cast(Settings, _SettingsStub())

    def _raise_direct_error(dsn: str) -> object:
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

    def _raise_direct_error(dsn: str) -> object:
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

    def _raise_direct_error(dsn: str) -> object:
        raise ValueError("Unrelated incident.")

    monkeypatch.setattr(
        "ixmp4.core.platform.DirectTransport.from_dsn", _raise_direct_error
    )

    with pytest.raises(ValueError):
        platform.get_transport(cast(PlatformConnectionInfo, _PlatformConnectionInfo()))
