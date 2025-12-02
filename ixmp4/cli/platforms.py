import re
from collections.abc import Generator, Iterator
from itertools import cycle
from pathlib import Path
from typing import Any, TypeVar

import sqlalchemy as sa
import typer
from rich import box
from rich.console import Console
from rich.progress import Progress, track
from rich.table import Column, Table
from typing_extensions import Annotated

from ixmp4.base_exceptions import PlatformNotFound, ServiceException
from ixmp4.conf import settings
from ixmp4.core.platform import Platform
from ixmp4.data.generator import MockDataGenerator
from ixmp4.db import __file__ as db_module_dir

from . import utils
from .alembic import get_alembic_controller

migration_script_directory = (Path(db_module_dir).parent / "migrations").absolute()


app = typer.Typer()
console = Console()


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
    path = settings.get_database_path(name)
    dsn = path.absolute().as_uri().replace("file://", "sqlite:///")

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
            utils.echo("Creating the database and running migrations... \n")

            settings.configure_logging("alembic")
            controller = get_alembic_controller(dsn)
            controller.upgrade_database("head")

            return dsn
        else:
            raise typer.Exit()


@app.command(help="Adds a new platform to ixmp4's toml registry.")
def add(
    name: Annotated[
        str,
        typer.Argument(
            help="The string identifier of the platform to add. Must be slug-like.",
            callback=validate_name,
        ),
    ],
    dsn: Annotated[
        str | None,
        typer.Option(
            help="Data source name. Can be a http(s) URL or a database connection string.",
            callback=validate_dsn,
        ),
    ] = None,
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
    name: Annotated[
        str,
        typer.Argument(help="The string identifier of the platform to remove."),
    ],
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
    toml_results = toml_platforms.list_platforms()

    toml_table = Table(
        "Name",
        "DSN",
        box=box.SIMPLE,
        title="via toml file " + toml_path_str,
        title_justify="left",
        caption="Total: " + typer.style(str(len(toml_results)), fg=typer.colors.GREEN),
        caption_justify="left",
    )
    for tp in toml_results:
        toml_table.add_row(tp.name, sa.make_url(tp.dsn).render_as_string())

    console.print()
    console.print(toml_table)

    # Manager Platforms
    try:
        manager_platforms = settings.get_manager_platforms()
        manager_results = manager_platforms.list_platforms()
    except ServiceException as e:
        utils.echo(
            "Exception occurred during manager request, cannot access manager platforms:"
        )
        utils.error(str(e))
        raise typer.Exit()

    manager_url_str = typer.style(settings.manager_url, fg=typer.colors.CYAN)
    utils.echo()

    manager_table = Table(
        "Slug",
        Column("Name", max_width=24, no_wrap=True),
        "Access",
        Column("Notice", max_width=48, no_wrap=True),
        box=box.SIMPLE,
        title="via manager api " + manager_url_str,
        title_justify="left",
        caption="Total: "
        + typer.style(str(len(manager_results)), fg=typer.colors.GREEN),
        caption_justify="left",
    )
    for mp in manager_results:
        manager_table.add_row(
            mp.slug, mp.name, str(mp.accessibility).lower(), mp.notice
        )
    console.print()
    console.print(manager_table)
    console.print()


@app.command(
    help="Generates mock test data and loads it into a platform. "
    "Experimental and meant as a development tool. Use at your own risk. "
)
def generate(
    platform_name: str,
    num_models: Annotated[
        int, typer.Option("--models", help="Number of mock models to generate.")
    ] = 10,
    num_runs: Annotated[
        int, typer.Option("--runs", help="Number of mock runs to generate.")
    ] = 40,
    num_regions: Annotated[
        int, typer.Option("--regions", help="Number of mock regions to generate.")
    ] = 200,
    num_variables: Annotated[
        int,
        typer.Option("--variables", help="Number of mock variables to generate."),
    ] = 1000,
    num_units: Annotated[
        int, typer.Option("--units", help="Number of mock units to generate.")
    ] = 40,
    num_datapoints: Annotated[
        int,
        typer.Option("--datapoints", help="Number of mock datapoints to generate."),
    ] = 30_000,
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
