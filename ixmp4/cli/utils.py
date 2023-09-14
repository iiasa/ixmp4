from functools import partial

import typer

echo = typer.echo
good = partial(typer.secho, fg=typer.colors.GREEN)
bad = partial(typer.secho, fg=typer.colors.RED)
error = partial(typer.secho, fg=typer.colors.RED, err=True)
info = partial(typer.secho, fg=typer.colors.CYAN)
important = partial(typer.secho, fg=typer.colors.MAGENTA)
secondary = partial(typer.secho, fg=typer.colors.BRIGHT_BLACK)
