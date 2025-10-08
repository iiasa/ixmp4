import re
from collections.abc import Generator, Iterator
from itertools import cycle
from pathlib import Path
from typing import Any, Optional, TypeVar

import typer
from rich.console import Console
from rich.progress import Progress, track
from rich.table import Table
from toolkit.db.alembic import AlembicCli, AlembicController

from ixmp4.rewrite.conf import settings
from ixmp4.rewrite.conf.platforms import PlatformConnectionInfo
from ixmp4.rewrite.core.platform import Platform
from ixmp4.rewrite.data.generator import MockDataGenerator
from ixmp4.rewrite.db import __file__ as db_module_dir
from ixmp4.rewrite.db import sqlite
from ixmp4.rewrite.db.models import get_metadata
from ixmp4.rewrite.exceptions import PlatformNotFound

from . import utils

app = typer.Typer()
console = Console()

migration_script_directory = (
    Path(db_module_dir) / "migrations" / "versions"
).absolute()


@app.command()
def alembic_(ctx: typer.Context, name: str) -> None:
    toml_platforms = settings.get_toml_platforms()
    platform: PlatformConnectionInfo
    try:
        platform = toml_platforms.get_platform(name)
    except PlatformNotFound:
        manager_platforms = settings.get_manager_platforms()
        platform = manager_platforms.get_platform(name)

    if platform.dsn.startswith("http"):
        raise typer.BadParameter(
            f"Platform '{name}' is an http platform and cannot be migrated."
        )

    controller = AlembicController(
        platform.dsn,
        str(migration_script_directory),
        f"{get_metadata.__module__}:{get_metadata.__name__}",
    )
    alembic_cli = AlembicCli(controller=controller)
    alembic_cli()


def validate_name(name: str) -> str:
    match = re.match(r"^[\w\-_]*$", name)
    if match is None:
        raise typer.BadParameter("Platform name must be slug-like.")
    else:
        return name


def validate_dsn(dsn: str | None) -> str | None:
    if dsn is None:
        return None
    match = re.match(r"^(sqlite|postgresql\+psycopg|https|http)(\:\/\/)", dsn)
    if match is None:
        raise typer.BadParameter(
            "Platform dsn must be a valid URL or database connection string."
        )
    else:
        return dsn


def prompt_sqlite_dsn(name: str) -> str:
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
            utils.echo("Creating the database and running migrations...")

            controller = AlembicController(
                dsn,
                str(migration_script_directory),
                f"{get_metadata.__module__}:{get_metadata.__name__}",
            )
            controller.upgrade_database("head")

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
) -> None:
    toml_platforms = settings.get_toml_platforms()
    try:
        toml_platforms.get_platform(name)
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

    toml_platforms.add_platform(name, dsn)
    utils.good("\nPlatform added successfully.")


def prompt_sqlite_removal(dsn: str) -> None:
    path = Path(dsn.replace("sqlite://", ""))
    path_str = typer.style(path, fg=typer.colors.CYAN)
    if typer.confirm(
        f"Do you want to remove the associated database file at {path_str} as well?"
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
) -> None:
    toml_platforms = settings.get_toml_platforms()
    try:
        platform = toml_platforms.get_platform(name)
    except PlatformNotFound:
        raise typer.BadParameter(f"Platform '{name}' does not exist.")

    if typer.confirm(
        f"Are you sure you want to remove the platform '{platform.name}' with dsn "
        f"'{platform.dsn}'?"
    ):
        if platform.dsn.startswith("sqlite://"):
            prompt_sqlite_removal(platform.dsn)
        toml_platforms.remove_platform(name)


@app.command("list", help="Lists all registered platforms.")
def list_() -> None:
    # TOML Platforms
    toml_platforms = settings.get_toml_platforms()

    toml_path_str = typer.style(toml_platforms.path, fg=typer.colors.CYAN)
    utils.echo(f"\nPlatforms registered in '{toml_path_str}':")

    toml_table = Table("Name", "DSN")
    for tp in toml_platforms.list_platforms():
        toml_table.add_row(tp.name, tp.dsn)
    console.print(toml_table)
    utils.echo(
        "Total: " + typer.style(str(toml_table.row_count), fg=typer.colors.GREEN)
    )

    # Manager Platforms
    manager_platforms = settings.get_manager_platforms()

    manager_url_str = typer.style(settings.manager_url, fg=typer.colors.CYAN)
    utils.echo(f"\nPlatforms accessible via '{manager_url_str}':")

    manager_table = Table("Name", "Access", "Notice")
    for mp in manager_platforms.list_platforms():
        manager_table.add_row(mp.name, mp.accessibility.value.lower(), mp.notice)

    console.print(manager_table)
    utils.echo(
        "Total: " + typer.style(str(manager_table.row_count), fg=typer.colors.GREEN)
    )


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
) -> None:
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


T = TypeVar("T")


def create_cycle(
    generator: Generator[T, Any, None], name: str, total: int
) -> Iterator[T]:
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


def generate_data(generator: MockDataGenerator) -> None:
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
