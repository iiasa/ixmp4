import typer
import uvicorn

from ixmp4.conf.settings import Settings
from ixmp4.server import Ixmp4Server

app = typer.Typer()


@app.command()
def start(
    host: str = typer.Option(default="127.0.0.1", help="The hostname to bind to."),
    port: int = typer.Option(default=9000, help="Requested server port."),
    workers: int = typer.Option(default=1, help="How many worker threads to start."),
    reload: bool = typer.Option(default=False, help="Whether to hot-reload."),
    debug: bool = typer.Option(default=False, help="Use debug mode."),
) -> None:
    """Starts the ixmp4 web api."""
    settings = Settings()
    server = Ixmp4Server(settings.server, debug=debug)

    uvicorn.run(
        server.asgi_app,
        host=host,
        port=port,
        reload=reload,
        workers=workers,
    )


# TODO
# @app.command()
# def dump_schema(
#     output_file: Optional[typer.FileTextWrite] = typer.Option(None, "-o"),
# ) -> None:
#     app = Ixmp4Server("schema_secret")
#     schema = get_openapi(
#         title=app.title,
#         version=app.version,
#         openapi_version=app.openapi_version,
#         description=app.description,
#         routes=app.routes,
#     )
#     if output_file is None:
#         utils.echo(json.dumps(schema))
#     else:
#         json.dump(
#             schema,
#             output_file,
#         )
