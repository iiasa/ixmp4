import json
from typing import Optional

import typer
import uvicorn
from fastapi.openapi.utils import get_openapi

from ixmp4.conf import settings
from ixmp4.server import Ixmp4Server

from . import utils

app = typer.Typer()


@app.command()
def start(
    host: str = typer.Option(default="127.0.0.1", help="The hostname to bind to."),
    port: int = typer.Option(default=9000, help="Requested server port."),
    workers: int = typer.Option(default=1, help="How many worker threads to start."),
    reload: bool = typer.Option(default=False, help="Whether to hot-reload."),
) -> None:
    """Starts the ixmp4 web api."""
    log_config = settings.get_server_logconf()
    uvicorn.run(
        Ixmp4Server(
            settings.secret_hs256,
            settings.get_toml_platforms_path(),
            settings.get_manager_client(),
        ),
        host=host,
        port=port,
        reload=reload,
        workers=workers,
        log_config=str(log_config.absolute()),
    )


@app.command()
def dump_schema(
    output_file: Optional[typer.FileTextWrite] = typer.Option(None, "-o"),
) -> None:
    app = Ixmp4Server("schema_secret")
    schema = get_openapi(
        title=app.title,
        version=app.version,
        openapi_version=app.openapi_version,
        description=app.description,
        routes=app.routes,
    )
    if output_file is None:
        utils.echo(json.dumps(schema))
    else:
        json.dump(
            schema,
            output_file,
        )
