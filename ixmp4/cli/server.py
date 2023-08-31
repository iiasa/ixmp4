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
    host: Optional[str] = typer.Option(
        "127.0.0.1",
        help="The hostname to bind to.",
    ),
    port: Optional[int] = typer.Option(
        9000,
        help="Requested server port.",
    ),
) -> None:
    """Starts the ixmp4 web api."""
    reload = settings.mode != "production"
    uvicorn.run(
        "ixmp4.server:app",
        host=host,
        port=port,
        reload=reload,
        log_config="ixmp4/conf/logging/server.conf",
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
