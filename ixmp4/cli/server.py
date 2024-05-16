import json
from typing import Optional

import typer
import uvicorn  # type: ignore[import]
from fastapi.openapi.utils import get_openapi

from ixmp4.conf import settings
from ixmp4.server import v1

from . import utils

app = typer.Typer()


@app.command()
def start(
    host: str = typer.Option(default="127.0.0.1", help="The hostname to bind to."),
    port: int = typer.Option(default=9000, help="Requested server port."),
    workers: int = typer.Option(default=1, help="How many worker threads to start."),
    reload: bool = typer.Option(default=False, help="Wether to hot-reload."),
) -> None:
    """Starts the ixmp4 web api."""
    log_config = settings.get_server_logconf()
    uvicorn.run(
        "ixmp4.server:app",
        host=host,
        port=port,
        reload=reload,
        workers=workers,
        log_config=str(log_config.absolute()),
    )


@app.command()
def dump_schema(output_file: Optional[typer.FileTextWrite] = typer.Option(None, "-o")):
    schema = get_openapi(
        title=v1.title,
        version=v1.version,
        openapi_version=v1.openapi_version,
        description=v1.description,
        routes=v1.routes,
    )
    if output_file is None:
        utils.echo(json.dumps(schema))
    else:
        json.dump(
            schema,
            output_file,
        )
