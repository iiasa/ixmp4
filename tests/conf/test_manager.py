import pytest
from toolkit.manager.mock import MockManagerClient

from ixmp4.conf.platforms import ManagerPlatforms
from ixmp4.core.exceptions import (
    ImproperlyConfigured,
)
from tests.fixtures import get_manager_fixtures


@pytest.fixture()
def mock_manager_client() -> MockManagerClient:
    return MockManagerClient(get_manager_fixtures())


class TestManagerPlatforms:
    def test_get_platform_substitutes_dsn_env_tokens(
        self, monkeypatch: pytest.MonkeyPatch, mock_manager_client: MockManagerClient
    ) -> None:
        monkeypatch.setenv("IXMP4_DIR", "/tmp/ixmp4")
        manager_platforms = ManagerPlatforms(mock_manager_client)

        platform = manager_platforms.get_platform("dev-public")
        assert platform.dsn == "sqlite:////tmp/ixmp4/databases/dev-public.sqlite3"

        # Ensure substitution happens on returned copies only.
        cached_platform = mock_manager_client.ixmp4.cached_list()[0]
        assert "{env:IXMP4_DIR}" in cached_platform.dsn

    def test_get_platform_raises_for_missing_dsn_env_tokens(
        self, monkeypatch: pytest.MonkeyPatch, mock_manager_client: MockManagerClient
    ) -> None:
        monkeypatch.delenv("IXMP4_DIR", raising=False)
        manager_platforms = ManagerPlatforms(mock_manager_client)

        with pytest.raises(
            ImproperlyConfigured,
            match=r"Cannot resolve DSN environment variable placeholder\(s\).",
        ):
            manager_platforms.get_platform("dev-public")
