import runpy
from unittest import mock

import pytest

import ixmp4.cli as cli


def test_main_entrypoint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = mock.Mock()
    monkeypatch.setattr(cli, "app", app)
    runpy.run_module("ixmp4.__main__", run_name="__main__")
    app.assert_called_once_with()
