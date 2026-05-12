import pytest
from toolkit.manager.mock import MockManagerClient

from ixmp4.conf.platforms import ManagerPlatforms
from ixmp4.core.exceptions import (
    ImproperlyConfigured,
)
from ixmp4.transport import DirectTransport
from tests.fixtures import get_manager_fixtures


@pytest.fixture()
def mock_manager_client() -> MockManagerClient:
    return MockManagerClient(get_manager_fixtures())


class TestManagerPlatforms:
    def test_get_platform_returns_dsn_with_placeholders(
        self, monkeypatch: pytest.MonkeyPatch, mock_manager_client: MockManagerClient
    ) -> None:
        monkeypatch.setenv("IXMP4_DIR", "/tmp/ixmp4")
        manager_platforms = ManagerPlatforms(mock_manager_client)

        # get_platform returns DSN with placeholders (not resolved)
        platform = manager_platforms.get_platform("dev-public")
        assert platform.dsn == "sqlite:///{env:IXMP4_DIR}/databases/dev-public.sqlite3"

        # Original cached platform still has placeholders
        cached_platform = mock_manager_client.ixmp4.cached_list()[0]
        assert "{env:IXMP4_DIR}" in cached_platform.dsn

        # Env var substitution happens when creating the engine
        transport = DirectTransport.from_dsn(platform.dsn)
        assert transport is not None

    def test_get_platform_returns_unresolved_dsn_for_missing_env_tokens(
        self, monkeypatch: pytest.MonkeyPatch, mock_manager_client: MockManagerClient
    ) -> None:
        monkeypatch.delenv("IXMP4_DIR", raising=False)
        manager_platforms = ManagerPlatforms(mock_manager_client)

        # get_platform should NOT raise - it returns the raw DSN with placeholders
        platform = manager_platforms.get_platform("dev-public")
        assert platform.dsn == "sqlite:///{env:IXMP4_DIR}/databases/dev-public.sqlite3"

        # Error is raised when trying to create the engine
        with pytest.raises(
            ImproperlyConfigured,
            match=r"Cannot resolve DSN environment variable placeholder\(s\).",
        ):
            DirectTransport.from_dsn(platform.dsn)
