from typing import cast

import httpx
import pytest
from litestar.app import Litestar

from ixmp4.transport import HttpxTransport, Transport
from tests.api.base import api_transport

transport = api_transport


class TestPlatformRootApi:
    @pytest.fixture(scope="class")
    def client(self, transport: Transport) -> httpx.Client:
        if isinstance(transport, HttpxTransport):
            return transport.http_client
        pytest.skip("transport does not provide an httpx client")

    @pytest.fixture(scope="class")
    def app(self, client: httpx.Client) -> Litestar:
        app = getattr(client, "app", None)
        if app is not None:
            return cast(Litestar, app)
        pytest.skip("httpx client does not provide an asgi app")

    def test_root_endpoint_works_without_token_auth(
        self, client: httpx.Client, app: Litestar
    ) -> None:
        assert client.headers.get("Authorization") is None

        toml_platforms = getattr(app.state, "toml_platforms", None)
        assert toml_platforms is not None

        local_platform_slug = next(
            (
                platform.slug
                for platform in toml_platforms.list_platforms()
                if not platform.dsn.startswith("http")
            ),
            None,
        )
        assert local_platform_slug is not None

        root_url = client.base_url.copy_with(path=f"/v1/{local_platform_slug}/")
        response = client.get(root_url)

        assert response.status_code == 200, response.text
        payload = response.json()

        assert {"name", "version", "is_managed", "manager_url", "utcnow"} <= set(
            payload
        )
        assert isinstance(payload["name"], str)
        assert isinstance(payload["version"], str)
        assert isinstance(payload["is_managed"], bool)
