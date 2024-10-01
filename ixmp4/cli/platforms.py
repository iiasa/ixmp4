import re
from itertools import cycle
from pathlib import Path
from typing import Generator, Optional

import typer
from rich.progress import Progress, track
from sqlalchemy.exc import OperationalError

from ixmp4.conf import settings
from ixmp4.conf.auth import SelfSignedAuth
from ixmp4.conf.manager import ManagerConfig, ManagerPlatformInfo
from ixmp4.conf.toml import TomlPlatformInfo
from ixmp4.core.exceptions import PlatformNotFound
from ixmp4.core.platform import Platform
from ixmp4.data.generator import MockDataGenerator
from ixmp4.db.utils import alembic, sqlite

from . import utils

app = typer.Typer()


def validate_name(name: str):
    match = re.match(r"^[\w\-_]*$", name)
    if match is None:
        raise typer.BadParameter("Platform name must be slug-like.")
    else:
        return name


def validate_dsn(dsn: str | None):
    if dsn is None:
        return None
    match = re.match(r"^(sqlite|postgresql\+psycopg|https|http)(\:\/\/)", dsn)
    if match is None:
        raise typer.BadParameter(
            "Platform dsn must be a valid URl or database connection string."
        )
    else:
        return dsn


def prompt_sqlite_dsn(name: str):
    path = sqlite.get_database_path(name)
    dsn = sqlite.get_dsn(path)
    if path.exists():
        if typer.confirm(
            f"A file at the standard filesystem location for name '{name}' already "
            "exists. Do you want to add the existing file to the platform registry?"
        ):
            return dsn
        else:
            raise typer.Exit()
    else:
        if typer.confirm(
            f"No file at the standard filesystem location for name '{name}' exists. "
            "Do you want to create a new database?"
        ):
            alembic.upgrade_database(dsn, "head")
            return dsn
        else:
            raise typer.Exit()


@app.command(help="Adds a new platform to ixmp4's toml registry.")
def add(
    name: str = typer.Argument(
        ...,
        help="The string identifier of the platform to add. Must be slug-like.",
        callback=validate_name,
    ),
    dsn: Optional[str] = typer.Option(
        None,
        help="Data source name. Can be a http(s) URL or a database connection string.",
        callback=validate_dsn,
    ),
):
    try:
        settings.toml.get_platform(name)
        raise typer.BadParameter(
            f"Platform with name '{name}' already exists. "
            "Choose another name or remove the existing platform."
        )
    except PlatformNotFound:
        pass

    if dsn is None:
        utils.echo(
            "No DSN supplied, assuming you want to add a local sqlite database..."
        )
        dsn = prompt_sqlite_dsn(name)

    settings.toml.add_platform(name, dsn)
    utils.good("\nPlatform added successfully.")


def prompt_sqlite_removal(dsn: str):
    path = Path(dsn.replace("sqlite://", ""))
    path_str = typer.style(path, fg=typer.colors.CYAN)
    if typer.confirm(
        "Do you want to remove the associated database file at " f"{path_str} as well?"  # type: ignore
    ):
        path.unlink()
        utils.echo("\nDatabase file deleted.")
    else:
        utils.echo("\nDatabase file left intact.")


@app.command(help="Removes a platform from ixmp4's toml registry.")
def remove(
    name: str = typer.Argument(
        ..., help="The string identifier of the platform to remove."
    ),
):
    try:
        platform = settings.toml.get_platform(name)
    except PlatformNotFound:
        raise typer.BadParameter(f"Platform '{name}' does not exist.")

    if typer.confirm(
        f"Are you sure you want to remove the platform '{platform.name}' with dsn "
        f"'{platform.dsn}'?"
    ):
        if platform.dsn.startswith("sqlite://"):
            prompt_sqlite_removal(platform.dsn)
        settings.toml.remove_platform(name)


def tabulate_toml_platforms(platforms: list[TomlPlatformInfo]):
    toml_path_str = typer.style(settings.toml.path, fg=typer.colors.CYAN)
    utils.echo(f"\nPlatforms registered in '{toml_path_str}'")
    if len(platforms):
        utils.echo("\nName".ljust(21) + "DSN")
        for p in platforms:
            utils.important(_shorten(p.name, 20), nl=False)
            utils.echo(_shorten(p.dsn, 60))
    utils.echo("Total: " + typer.style(str(len(platforms)), fg=typer.colors.GREEN))


def tabulate_manager_platforms(
    platforms: list[ManagerPlatformInfo],
):
    manager_url_str = typer.style(settings.manager.url, fg=typer.colors.CYAN)
    utils.echo(f"\nPlatforms accessible via '{manager_url_str}'")
    utils.echo("\nName".ljust(21) + "Access".ljust(10) + "Notice")
    for p in platforms:
        utils.important(_shorten(p.name, 20), nl=False)
        utils.echo(str(p.accessibility.value.lower()).ljust(10), nl=False)
        if p.notice is not None:
            utils.echo(_shorten(p.notice, 58), nl=False)
        utils.echo()
    utils.echo("Total: " + typer.style(str(len(platforms)), fg=typer.colors.GREEN))


@app.command("list", help="Lists all registered platforms.")
def list_():
    tabulate_toml_platforms(settings.toml.list_platforms())
    if settings.manager is not None:
        tabulate_manager_platforms(settings.manager.list_platforms())


@app.command(
    help=(
        "Migrates all database platforms from your local toml file to the newest "
        "revision."
    )
)
def upgrade():
    if settings.managed:
        utils.echo(
            f"Establishing self-signed admin connection to '{settings.manager_url}'."
        )
        manager_conf = ManagerConfig(
            str(settings.manager_url),
            SelfSignedAuth(settings.secret_hs256),
            remote=False,
        )
        platform_list = manager_conf.list_platforms()
    else:
        platform_list = settings.toml.list_platforms()

    for p in platform_list:
        if p.dsn.startswith("http"):
            # This should probably never happen unless the manager registers an
            # external rest platform.
            utils.echo(f"Skipping '{p.name}' because it is a REST platform.")
        else:
            utils.echo(f"Upgrading platform '{p.name}' with dsn '{p.dsn}'...")
            try:
                alembic.upgrade_database(p.dsn, "head")
            except OperationalError as e:
                utils.echo(f"Skipping '{p.name}' because of an error: {str(e)}")


@app.command(
    help=(
        "Stamps all database platforms from your local toml file with the given "
        "revision."
    )
)
def stamp(revision: str) -> None:
    for c in settings.toml.list_platforms():
        if c.dsn.startswith("http"):
            utils.echo(f"Skipping '{c.name}' because it is a REST platform.")
        else:
            utils.echo(
                f"Stamping platform '{c.name}' with dsn '{c.dsn}' to '{revision}'..."
            )
            alembic.stamp_database(c.dsn, revision)


@app.command(
    help="Generates mock test data and loads it into a platform. "
    "Experimental and meant as a development tool. Use at your own risk. "
)
def generate(
    platform_name: str,
    num_models: int = typer.Option(
        10, "--models", help="Number of mock models to generate."
    ),
    num_runs: int = typer.Option(40, "--runs", help="Number of mock runs to generate."),
    num_regions: int = typer.Option(
        200, "--regions", help="Number of mock regions to generate."
    ),
    num_variables: int = typer.Option(
        1000, "--variables", help="Number of mock variables to generate."
    ),
    num_units: int = typer.Option(
        40, "--units", help="Number of mock units to generate."
    ),
    num_datapoints: int = typer.Option(
        30_000, "--datapoints", help="Number of mock datapoints to generate."
    ),
):
    try:
        platform = Platform(platform_name)
    except PlatformNotFound:
        raise typer.BadParameter(f"Platform '{platform_name}' does not exist.")

    typer.echo("This command will generate:\n")
    lines = []
    for name, value in [
        ("Model(s)", num_models),
        ("Run(s)", num_runs),
        ("Region(s)", num_regions),
        ("Variable(s)", num_variables),
        ("Units(s)", num_units),
        ("Datapoint(s)", num_datapoints),
    ]:
        value_str = typer.style(str(value), fg=typer.colors.CYAN)
        lines.append(f" - {value_str} {name} ")
    typer.echo("\n".join(lines))
    typer.echo(
        f"...and load them into the platform '{platform_name}' "
        f"(DSN: {platform.backend.info.dsn}).\n"
    )

    if typer.confirm("Are you sure?"):
        generator = MockDataGenerator(
            platform,
            num_models,
            num_runs,
            num_regions,
            num_variables,
            num_units,
            num_datapoints,
        )
        generate_data(generator)
        utils.good("Done!")


def create_cycle(generator: Generator, name: str, total: int):
    return cycle(
        [
            m
            for m in track(
                generator,
                description=f"Generating {name}(s)...",
                total=total,
            )
        ]
    )


def generate_data(generator: MockDataGenerator):
    model_names = create_cycle(
        generator.yield_model_names(), "Model", generator.num_models
    )
    runs = create_cycle(generator.yield_runs(model_names), "Run", generator.num_runs)
    regions = create_cycle(generator.yield_regions(), "Region", generator.num_regions)
    units = create_cycle(generator.yield_units(), "Unit", generator.num_units)
    variable_names = create_cycle(
        generator.yield_variable_names(), "Variable", generator.num_variables
    )
    with Progress() as progress:
        task = progress.add_task(
            description="Generating Datapoint(s)...", total=generator.num_datapoints
        )
        for df in generator.yield_datapoints(runs, variable_names, units, regions):
            progress.advance(task, len(df))


def _shorten(value: str, length: int):
    """Shorten and adjust a string to a given length adding `...` if necessary"""
    if len(value) > length - 4:
        value = value[: length - 4] + "..."
    return value.ljust(length)
