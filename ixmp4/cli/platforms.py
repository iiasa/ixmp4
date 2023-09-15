import re
from pathlib import Path
from typing import Optional

import typer

from ixmp4.conf import settings
from ixmp4.conf.manager import ManagerPlatformInfo
from ixmp4.conf.toml import TomlPlatformInfo
from ixmp4.core.exceptions import PlatformNotFound
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
            f"A file at the standard filesystem location for name '{name}' already exists. "
            "Do you want to add the existing file to the platform registry?"
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
        help="A data source name. Can be a http(s) URl or a database connection string.",
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
        "Do you want to remove the associated database file at "
        f"{path_str} aswell?"  # type: ignore
    ):
        path.unlink()
        utils.echo("\nDatabase file deleted.")
    else:
        utils.echo("\nDatabase file left intact.")


@app.command(help="Removes a platform from ixmp4's toml registry.")
def remove(
    name: str = typer.Argument(
        ..., help="The string identifier of the platform to remove."
    )
):
    try:
        platform = settings.toml.get_platform(name)
    except PlatformNotFound:
        raise typer.BadParameter(f"Platform '{name}' does not exist.")

    if typer.confirm(
        f"Are you sure you want to remove the platform '{platform.name}' with dsn '{platform.dsn}'?"
    ):
        if platform.dsn.startswith("sqlite://"):
            prompt_sqlite_removal(platform.dsn)
        settings.toml.remove_platform(name)


def tabulate_platforms(
    platforms: list[TomlPlatformInfo] | list[ManagerPlatformInfo],
):
    utils.echo("Platform".ljust(15) + "DSN\n".ljust(15))
    total = 0
    for p in platforms:
        name = p.name
        if len(name) > 12:
            name = name[:9] + "..."
        name = name.ljust(15)

        utils.important(name, nl=False)
        utils.echo(p.dsn.ljust(10))
        total += 1

    utils.info("\n" + str(total), nl=False)
    utils.echo(" total \n")


@app.command("list", help="Lists all registered platforms.")
def list_():
    toml_path_str = typer.style(settings.toml.path, fg=typer.colors.CYAN)
    toml_platforms = settings.toml.list_platforms()
    utils.echo(f"Platforms in '{toml_path_str}':")
    tabulate_platforms(toml_platforms)

    if settings.manager is not None:
        manager_url_str = typer.style(settings.manager.url, fg=typer.colors.CYAN)
        manager_platforms = settings.manager.list_platforms()
        utils.echo(f"Platforms accessible via '{manager_url_str}':")
        tabulate_platforms(manager_platforms)


@app.command(
    help="Migrates all database platforms from your local toml file to the newest revision."
)
def upgrade():
    for c in settings.toml.list_platforms():
        if c.dsn.startswith("http"):
            utils.echo(f"Skipping '{c.name}' because it is a REST platform.")
        else:
            utils.echo(f"Upgrading platform '{c.name}' with dsn '{c.dsn}'...")
            alembic.upgrade_database(c.dsn, "head")


@app.command(
    help="Stamps all database platforms from your local toml file with the given revision."
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
