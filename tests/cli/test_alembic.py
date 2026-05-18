from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Generator
from unittest import mock

import pytest
from toolkit.manager.mock import MockManagerClient
from typer.testing import CliRunner

from ixmp4.cli import app
from ixmp4.cli.alembic import collect_platforms, get_alembic_controller
from ixmp4.conf.settings import Settings
from ixmp4.core.exceptions import ImproperlyConfigured


@pytest.fixture(scope="function")
def temporary_settings(
    mock_manager_client: MockManagerClient,
) -> Generator[Settings, None, None]:
    """Fixture to create settings pointing to a temporary directory
    and mocking the `Settings` constructor."""
    with TemporaryDirectory() as temp_dir:
        settings = Settings(storage_directory=Path(temp_dir))
        with mock.patch.object(
            settings, "get_manager_client", return_value=mock_manager_client
        ):
            with mock.patch("ixmp4.conf.settings.Settings", new=settings):
                yield settings


@pytest.fixture(scope="function")
def runner(temporary_settings: Settings) -> CliRunner:
    return CliRunner(
        env={"IXMP4_STORAGE_DIRECTORY": str(temporary_settings.storage_directory)},
    )


class TestAlembicTargets:
    def test_collect_platforms(
        self,
        runner: CliRunner,
        temporary_settings: Settings,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        runner.invoke(app, ["platforms", "add", "test-1"], input="y")
        runner.invoke(app, ["platforms", "add", "test-2"], input="y")

        toml_platforms = collect_platforms(
            temporary_settings, platform=[], toml=True, manager=False
        )
        assert len(toml_platforms) == 2
        assert toml_platforms[0].name == "test-1"
        assert toml_platforms[1].name == "test-2"

        monkeypatch.setenv("IXMP4_DIR", "/tmp/ixmp4")

        manager_platforms = collect_platforms(
            temporary_settings, platform=[], toml=False, manager=True
        )
        assert len(manager_platforms) == 3
        assert manager_platforms[0].slug == "dev-public"
        assert manager_platforms[1].slug == "dev-gated"
        assert manager_platforms[2].slug == "dev-private"

        select_platforms = collect_platforms(
            temporary_settings,
            platform=["dev-public", "test-2"],
            toml=False,
            manager=False,
        )
        assert len(select_platforms) == 2
        assert select_platforms[0].slug == "dev-public"
        assert select_platforms[1].name == "test-2"

    def test_alembic_compare(
        self, runner: CliRunner, temporary_settings: Settings
    ) -> None:
        runner.invoke(app, ["platforms", "add", "test-1"], input="y")
        runner.invoke(app, ["platforms", "add", "test-2"], input="y")
        result = runner.invoke(app, ["alembic", "--toml", "compare"], input="y")

        assert result.exit_code == 0
        assert "databases/test-1.sqlite3" in result.output
        assert "databases/test-2.sqlite3" in result.output

    def test_collect_platforms_manager_does_not_require_env_tokens(
        self,
        temporary_settings: Settings,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.delenv("IXMP4_DIR", raising=False)

        manager_platforms = collect_platforms(
            temporary_settings, platform=[], toml=False, manager=True
        )
        assert len(manager_platforms) == 3
        assert manager_platforms[0].slug == "dev-public"
        assert "{env:IXMP4_DIR}" in manager_platforms[0].dsn


def test_get_alembic_controller_resolves_dsn_env_tokens(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured_dsn: str | None = None

    class FakeAlembicController:
        def __init__(self, dsn: str, *_args: object, **_kwargs: object) -> None:
            nonlocal captured_dsn
            captured_dsn = dsn

    monkeypatch.setattr("ixmp4.cli.alembic.AlembicController", FakeAlembicController)
    monkeypatch.setenv("IXMP4_DB_PASSWORD", "pw")

    get_alembic_controller("postgresql://u:{env:IXMP4_DB_PASSWORD}@db/test")
    assert captured_dsn == "postgresql://u:pw@db/test"


def test_get_alembic_controller_raises_for_missing_dsn_env_tokens(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("IXMP4_DB_PASSWORD", raising=False)

    with pytest.raises(
        ImproperlyConfigured,
        match=r"Cannot resolve DSN environment variable placeholder\(s\).",
    ):
        get_alembic_controller("postgresql://u:{env:IXMP4_DB_PASSWORD}@db/test")
