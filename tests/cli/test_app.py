from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Generator
from unittest import mock

import pytest
from typer.testing import CliRunner

import ixmp4.cli as cli
from ixmp4.cli import app
from ixmp4.conf.settings import Settings


@pytest.fixture(scope="function")
def temporary_settings() -> Generator[Settings, None, None]:
    with TemporaryDirectory() as temp_dir:
        yield Settings(storage_directory=Path(temp_dir))


@pytest.fixture(scope="function")
def runner(temporary_settings: Settings) -> CliRunner:
    return CliRunner(
        env={"IXMP4_STORAGE_DIRECTORY": str(temporary_settings.storage_directory)},
    )


class TestAppCLI:
    def test_login_saves_credentials(
        self,
        runner: CliRunner,
        temporary_settings: Settings,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        class FakeManagerAuth:
            def __init__(self, username: str, password: str, manager_url: str):
                assert username == "alice"
                assert password == "secret"
                assert manager_url == str(temporary_settings.manager_url)
                self.access_token = mock.Mock(user=mock.Mock(username=username))

        monkeypatch.setattr(cli, "ManagerAuth", FakeManagerAuth)

        result = runner.invoke(
            app, ["login", "alice", "--password", "secret"], input="y\n"
        )

        assert result.exit_code == 0
        assert "Successfully authenticated as user 'alice'." in result.output
        assert "Done." in result.output
        assert temporary_settings.get_credentials().get("default") == {
            "username": "alice",
            "password": "secret",
        }

    def test_login_rejects_invalid_credentials(
        self, runner: CliRunner, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        def raise_invalid_credentials(
            username: str, password: str, manager_url: str
        ) -> None:
            raise cli.InvalidCredentials()

        monkeypatch.setattr(cli, "ManagerAuth", raise_invalid_credentials)

        result = runner.invoke(app, ["login", "alice", "--password", "secret"])

        assert result.exit_code == 2
        assert "The credentials you provided are not valid." in result.output

    def test_logout_clears_saved_credentials(
        self, runner: CliRunner, temporary_settings: Settings
    ) -> None:
        temporary_settings.get_credentials().set("default", "alice", "secret")

        result = runner.invoke(app, ["logout"], input="y\n")

        assert result.exit_code == 0
        assert "Done." in result.output
        assert temporary_settings.get_credentials().get("default") is None

    def test_test_command_dry_run_uses_default_options(self, runner: CliRunner) -> None:
        result = runner.invoke(
            app,
            ["test", "false", "false", "true", "tests/cli/test_app.py", "-q"],
        )

        assert result.exit_code == 0
        assert "pytest " in result.output
        assert "--ignore=tests/data" in result.output
        assert "--benchmark-skip" in result.output
        assert "tests/cli/test_app.py -q" in result.output

    def test_test_command_dry_run_can_enable_backends_and_benchmarks(
        self, runner: CliRunner
    ) -> None:
        result = runner.invoke(app, ["test", "true", "true", "true", "-q"])

        assert result.exit_code == 0
        assert "pytest " in result.output
        assert "--ignore=tests/data" not in result.output
        assert "--benchmark-skip" not in result.output
        assert "--benchmark-group-by=func" in result.output
        assert "--benchmark-columns=min" in result.output
