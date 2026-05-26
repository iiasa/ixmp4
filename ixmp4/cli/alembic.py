import typer
from toolkit.db.alembic import AlembicCli, AlembicController
from typing_extensions import Annotated

from ixmp4.base_exceptions import PlatformNotFound, ServiceException
from ixmp4.conf.platforms import PlatformConnectionInfo, resolve_dsn_env_tokens
from ixmp4.conf.settings import Settings
from ixmp4.data.versions.squash import squash_version_records
from ixmp4.db import get_alembic_controller

app = AlembicCli(
    help="Manages alembic migrations on selected platforms via subcommands."
)


def get_connection_info(settings: Settings, name: str) -> PlatformConnectionInfo:
    toml_platforms = settings.get_toml_platforms()
    platform: PlatformConnectionInfo
    try:
        platform = toml_platforms.get_platform(name)
    except PlatformNotFound as pnf:
        try:
            manager_platforms = settings.get_manager_platforms()
            platform = manager_platforms.get_platform(name)
        except ServiceException as se:
            typer.echo(
                "Exception occurred during manager request, "
                "cannot access manager platforms:"
            )
            typer.secho(str(se), fg=typer.colors.RED, err=True)
            raise pnf
    if platform.dsn.startswith("http"):
        raise typer.BadParameter(
            f"Platform '{name}' is a http platform and "
            "thus cannot be a database target for alembic."
        )

    return platform


def collect_platforms(
    settings: Settings, *, platform: list[str] | None, toml: bool, manager: bool
) -> list[PlatformConnectionInfo]:
    candidates: list[PlatformConnectionInfo] = []
    if platform is not None:
        for pn in platform:
            candidates.append(get_connection_info(settings, pn))

    if toml:
        toml_platforms = settings.get_toml_platforms()
        candidates += toml_platforms.list_platforms()

    if manager:
        try:
            manager_platforms = settings.get_manager_platforms()
            candidates += manager_platforms.list_platforms()
        except ServiceException as e:
            typer.echo(
                "Exception occurred during manager request, "
                "cannot access manager platforms:"
            )
            typer.secho(str(e), fg=typer.colors.RED, err=True)

    platforms: list[PlatformConnectionInfo] = []
    for c in candidates:
        if c.dsn.startswith("http"):
            typer.echo(f"Skipping '{c.name}' because it is an API platform. ")
        else:
            platforms.append(c)

    return platforms


@app.callback()
def alembic(
    ctx: typer.Context,
    platform: Annotated[
        list[str] | None,
        typer.Option(
            "--platform",
            "-p",
            help="Use (a) platform name(s) as an alembic command target.",
        ),
    ] = None,
    toml: Annotated[
        bool,
        typer.Option(
            help=("Use platforms from the platforms.toml file as alembic targets."),
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
    choose command target databases."""
    settings = Settings()
    settings.configure_logging("alembic")
    platforms = collect_platforms(
        settings, platform=platform, toml=toml, manager=manager
    )

    if len(platforms) == 0:
        raise typer.BadParameter(
            "No platforms found. Are you missing one of: "
            "'--platform/-p', '--toml', '--manager'?"
        )

    dsns = [resolve_dsn_env_tokens(platform.dsn) for platform in platforms]
    controllers = [get_alembic_controller(dsn) for dsn in dsns]
    ctx.ensure_object(dict)
    ctx.obj["controllers"] = controllers


@app.command("squash-versions")
def squash_versions(ctx: typer.Context) -> None:
    """Squash version records so they only reference checkpoint transactions."""

    controllers: list[AlembicController] = ctx.obj["controllers"]
    for controller in controllers:
        typer.echo(
            f"Squashing version records on '{controller.url.render_as_string()}'..."
        )
        engine = controller.get_engine()
        with engine.connect() as conn:
            squash_version_records(conn)
            conn.commit()
        typer.secho("Done.", fg=typer.colors.GREEN)
