from pathlib import Path

from rich import box
from rich.console import Console
from rich.table import Table
from rich.text import Text

from ixmp4 import __version__
from ixmp4.conf.settings import Settings

IXMP4_FIGLET = """
 _____  ____  __ ___ _ _
|_ _\\ \\/ /  \\/  | _ \\ | |
 | | >  <| |\\/| |  _/_  _|
|___/_/\\_\\_|  |_|_|   |_|

"""


def _format_path(path: Path | None) -> str:
    if path is None:
        return "<unset>"
    return str(path)


def make_banner_panel(settings: Settings) -> Table:
    figlet = Text(IXMP4_FIGLET.rstrip("\n"), style="bold cyan")

    config_table = Table(
        show_header=False,
        box=box.SIMPLE,
    )
    config_table.add_column(style="bold white")
    config_table.add_column(style="green")
    config_table.add_row("Version", __version__)
    config_table.add_row("Mode", settings.mode)
    config_table.add_row("Manager URL", str(settings.manager_url))
    config_table.add_row("Storage Directory", _format_path(settings.storage_directory))

    server_table = Table(
        show_header=False,
        title="Server Settings",
        title_justify="left",
        title_style="bold cyan",
        box=box.SIMPLE,
    )
    server_table.add_row("Platforms TOML", _format_path(settings.server.toml_platforms))
    server_table.add_row("Manager URL", str(settings.server.manager_url))
    server_table.add_row("HS256 Secret", str(settings.server.secret_hs256))
    server_table.add_row("Max Page Size", str(settings.server.max_page_size))
    server_table.add_row("Default Page Size", str(settings.server.default_page_size))

    client_table = Table(
        show_header=False,
        title="Client Settings",
        title_justify="left",
        title_style="bold cyan",
        box=box.SIMPLE,
    )
    client_table.add_row("Concurrency", str(settings.client.concurrency))
    client_table.add_row("Retries", str(settings.client.retries))
    client_table.add_row("Timeout", str(settings.client.timeout))
    client_table.add_row("HS256 Secret", str(settings.client.secret_hs256))

    content = Table.grid()
    content.add_row(figlet)
    content.add_row(config_table)
    content.add_row(server_table)
    content.add_row(client_table)

    return content


def print_banner(
    console: Console | None = None, settings: Settings | None = None
) -> None:
    if console is None:
        console = Console()
    if settings is None:
        settings = Settings()

    console.print(make_banner_panel(settings))
