from pathlib import Path

import typer
from toolkit.db.alembic import AlembicCli, AlembicController
from typing_extensions import Annotated

from ixmp4.rewrite.conf import settings
from ixmp4.rewrite.conf.platforms import PlatformConnectionInfo
from ixmp4.rewrite.db import __file__ as db_module_dir
from ixmp4.rewrite.db.models import get_metadata
from ixmp4.rewrite.exceptions import PlatformNotFound, ServiceException

from . import utils

migration_script_directory = (Path(db_module_dir).parent / "migrations").absolute()

app = AlembicCli()


def get_alembic_controller(dsn: str) -> AlembicController:
    return AlembicController(
        dsn,
        str(migration_script_directory),
        f"{get_metadata.__module__}:{get_metadata.__name__}",
    )


def get_target_by_name(name: str) -> PlatformConnectionInfo:
    toml_platforms = settings.get_toml_platforms()
    platform: PlatformConnectionInfo
    try:
        platform = toml_platforms.get_platform(name)
    except PlatformNotFound as pnf:
        try:
            manager_platforms = settings.get_manager_platforms()
            platform = manager_platforms.get_platform(name)
        except ServiceException as se:
            utils.echo(
                "Exception occured during manager request, "
                "cannot access manager platforms:"
            )
            utils.error(str(se))
            raise pnf
    if platform.dsn.startswith("http"):
        raise typer.BadParameter(
            f"Platform '{name}' is a http platform and "
            "thus cannot be an alembic command target."
        )

    return platform


def collect_targets(
    platform: list[str] | None, toml: bool, manager: bool
) -> list[PlatformConnectionInfo]:
    candidates: list[PlatformConnectionInfo] = []
    if platform is not None:
        for pn in platform:
            candidates.append(get_target_by_name(pn))

    if toml:
        toml_platforms = settings.get_toml_platforms()
        candidates += toml_platforms.list_platforms()

    if manager:
        try:
            manager_platforms = settings.get_manager_platforms()
            candidates += manager_platforms.list_platforms()
        except ServiceException as e:
            utils.echo(
                "Exception occured during manager request, "
                "cannot access manager platforms:"
            )
            utils.error(str(e))

    targets: list[PlatformConnectionInfo] = []
    for c in candidates:
        if c.dsn.startswith("http"):
            utils.echo(f"Skipping '{c.name}' because it is an API platform. ")
        else:
            targets.append(c)

    return targets


@app.callback(invoke_without_command=True)
def alembic(
    ctx: typer.Context,
    platform: Annotated[
        list[str] | None,
        typer.Option(
            "--platform",
            "-p",
            help="Use a platform name(s) as an alembic command target.",
        ),
    ] = None,
    toml: Annotated[
        bool,
        typer.Option(
            help=("Use platforms from the platfroms.toml file as alembic targets."),
        ),
    ] = False,
    manager: Annotated[
        bool,
        typer.Option(
            help=("Use platforms from the manager api as alembic targets."),
        ),
    ] = False,
) -> None:
    """Runs alembic commands on platform databases.
    Requires '--platform/-p', '--toml' or '--manager' to
    choose command targets."""

    settings.configure_logging("alembic")
    targets = collect_targets(platform, toml, manager)

    if len(targets) == 0:
        raise typer.BadParameter(
            "No targets found. Are you missing one of: "
            "'--platform/-p', '--toml', '--manager'?"
        )

    controllers = [get_alembic_controller(target.dsn) for target in targets]
    ctx.ensure_object(dict)
    ctx.obj["controllers"] = controllers
