from typing import Generator

import httpx
import pytest
from pydantic import SecretStr

from ixmp4.conf.settings import ServerSettings, Settings

from .base import ServerTest, api_transport

transport = api_transport


class TestServerWithSecret(ServerTest):
    @pytest.fixture(scope="session")
    def settings(self) -> Generator[Settings, None, None]:
        settings = Settings(server=ServerSettings(secret_hs256=SecretStr("testsecret")))
        yield settings

    def test_server_returns_400_on_invalid_header_format(
        self, client: httpx.Client
    ) -> None:
        res = client.request("GET", "/", headers={"Authorization": "bogus.token.sig"})
        json = res.json()
        assert res.status_code == 400
        assert json["name"] == "BadRequest"

    def test_server_returns_401_on_invalid_token(self, client: httpx.Client) -> None:
        res = client.request(
            "GET", "/", headers={"Authorization": "Bearer bogus.token.sig"}
        )
        json = res.json()
        assert res.status_code == 401
        assert json["name"] == "InvalidToken"

    def test_server_returns_401_on_missing_token(self, client: httpx.Client) -> None:
        assert client.headers.get("Authorization", None) is None
        res = client.request("GET", "/")
        json = res.json()
        assert res.status_code == 401
        assert json["name"] == "Unauthorized"
