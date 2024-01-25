"""
Check the CLI help command on how to use it:

.. code:: bash

    ixmp4 --help
    ixmp4 platforms --help
    ixmp4 test --help
    ixmp4 server --help

"""

from typing import Optional

import typer

from ixmp4.cli import platforms
from ixmp4.conf import settings
from ixmp4.conf.auth import ManagerAuth
from ixmp4.core.exceptions import InvalidCredentials

from . import utils

app = typer.Typer()
app.add_typer(platforms.app, name="platforms")

try:
    from . import server

    app.add_typer(server.app, name="server")
except ImportError:
    # No server installed
    pass


@app.command()
def login(
    username: str = typer.Argument(..., help="Your username."),
    password: str = typer.Option(
        ...,
        help="Your password. Will be saved in plain-text.",
        prompt=True,
        hide_input=True,
    ),
):
    try:
        auth = ManagerAuth(username, password, str(settings.manager_url))
        user = auth.get_user()
        utils.good(f"Successfully authenticated as user '{user.username}'.")
        if typer.confirm(
            text=(
                "Are you sure you want to save your credentials in plain-text for "
                "future use?"
            )
        ):
            settings.credentials.set("default", username, password)

    except InvalidCredentials:
        raise typer.BadParameter(
            "The credentials you provided are not valid. (Wrong username or password?)"
        )


@app.command()
def logout():
    if typer.confirm(
        "Are you sure you want to log out and delete locally saved credentials?"
    ):
        settings.credentials.clear("default")


try:
    import pytest

    @app.command(
        context_settings={"allow_extra_args": True, "ignore_unknown_options": True}
    )
    def test(
        ctx: typer.Context,
        with_backend: Optional[bool] = False,
        with_benchmarks: Optional[bool] = False,
        dry: Optional[bool] = False,
    ):
        opts = [
            "--cov-report",
            "xml:.coverage.xml",
            "--cov-report",
            "term",
            "--cov=ixmp4",
            "-rsx",
        ] + ctx.args

        if not with_backend:
            opts += ["--ignore=tests/data"]

        if not with_benchmarks:
            opts += ["--benchmark-skip"]
        else:
            opts += ["--benchmark-group-by=func", "--benchmark-columns=min"]

        if dry:
            utils.echo("pytest " + " ".join(opts))
            raise typer.Exit(0)
        exit_code = pytest.main(opts)
        raise typer.Exit(
            code=exit_code.value
            if isinstance(exit_code, pytest.ExitCode)
            else exit_code
        )

except ImportError:
    # No pytest installed
    pass
