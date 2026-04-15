import json

import typer

from ixmp4.conf.settings import Settings
from ixmp4.server import Ixmp4Server

try:
    import uvicorn

    _server_is_installed = True
except ImportError:
    _server_is_installed = False

app = typer.Typer(help="Enables use of the http server.")


if _server_is_installed:

    @app.command()
    def start(
        host: str = typer.Option(default="127.0.0.1", help="The hostname to bind to."),
        port: int = typer.Option(default=9000, help="Requested server port."),
        workers: int = typer.Option(
            default=1, help="How many worker threads to start."
        ),
        reload: bool = typer.Option(default=False, help="Whether to hot-reload."),
        debug: bool = typer.Option(default=False, help="Use debug mode."),
    ) -> None:
        """Starts the ixmp4 http server."""
        settings = Settings()
        log_config = settings.load_logging_config("server")
        if debug:
            log_config["root"]["level"] = "DEBUG"

        server = Ixmp4Server(settings.server, debug=debug)

        uvicorn.run(
            server.asgi_app,
            host=host,
            port=port,
            reload=reload,
            workers=workers,
            log_config=log_config,
        )

    @app.command()
    def dump_schema(
        output_file: typer.FileTextWrite | None = typer.Option(None, "-o"),
    ) -> None:
        settings = Settings()
        server = Ixmp4Server(settings.server)
        schema = server.asgi_app.openapi_schema.to_schema()
        if output_file is None:
            typer.echo(json.dumps(schema, default=str))
        else:
            json.dump(schema, output_file, default=str)
