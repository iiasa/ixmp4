"""Importable ASGI application factory for the ixmp4 server."""

from litestar import Litestar

from ixmp4.conf.settings import Settings
from ixmp4.server import Ixmp4Server


def create_app() -> Litestar:
    """Create the ixmp4 ASGI application from the current settings.

    Creates its own `Settings` object, so configuration is read from the environment.
    """
    settings = Settings()
    return Ixmp4Server(settings.server, debug=settings.mode == "debug").asgi_app
