from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Generator, cast

import httpx
import pytest
from litestar.app import Litestar

from ixmp4.conf.settings import Settings
from ixmp4.server import Ixmp4Server
from ixmp4.transport import HttpxTransport
from tests.backends import build_rest_server


class TestPlatformRootApi:
    @pytest.fixture(scope="session")
    def settings(self) -> Generator[Settings]:
        with TemporaryDirectory() as temp_dir:
            settings = Settings(storage_directory=Path(temp_dir))
            settings.get_toml_platforms_path().write_text(
                '[test]\ndsn = "sqlite:///:memory:"\n'
            )
            yield settings

    @pytest.fixture(scope="session")
    def server(self, settings: Settings) -> Ixmp4Server:
        server, _ = build_rest_server(settings)
        return server

    @pytest.fixture(scope="session")
    def client(self, server: Ixmp4Server, settings: Settings) -> httpx.Client:
        transport = HttpxTransport.from_asgi(server.asgi_app, settings.client)
        return transport.http_client

    @pytest.fixture(scope="session")
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
