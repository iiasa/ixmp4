"""

Run the web api with:

.. code:: bash

   ixmp4 server start [--host 127.0.0.1] [--port 8000]

This will start ixmp4’s asgi server. Check
``http://127.0.0.1:8000/v1/<platform>/docs/``.

"""

import logging
from typing import TYPE_CHECKING, Any, Awaitable, Callable

from litestar import Litestar
from litestar.config.cors import CORSConfig
from litestar.openapi import OpenAPIConfig
from litestar.openapi.plugins import ScalarRenderPlugin
from litestar.openapi.spec.components import Components
from litestar.openapi.spec.security_scheme import SecurityScheme
from toolkit.auth.user import User
from toolkit.client.auth import SelfSignedAuth
from toolkit.manager.client import ManagerClient

from ixmp4 import __version__, data
from ixmp4.conf.platforms import (
    ManagerPlatforms,
    PlatformConnectionInfo,
    TomlPlatforms,
)
from ixmp4.conf.settings import ServerSettings

if TYPE_CHECKING:
    from ixmp4.transport import DirectTransport

from .v1 import V1HttpApi

logger = logging.getLogger(__name__)

cors_config = CORSConfig(
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class Ixmp4Server:
    asgi_app: Litestar
    settings: ServerSettings

    v1: V1HttpApi

    def __init__(
        self,
        settings: ServerSettings,
        debug: bool = False,
        override_transport: "Callable[..., Awaitable[DirectTransport]] | None" = None,
        **kwargs: Any,
    ) -> None:
        self.settings = settings

        bearer_scheme = SecurityScheme(
            "http", scheme="bearer", bearer_format="Bearer <token>"
        )
        openapi_config = OpenAPIConfig(
            title="IXMP4",
            version=__version__,
            render_plugins=[ScalarRenderPlugin()],
            components=Components(security_schemes={"default": bearer_scheme}),
        )

        if settings.secret_hs256 is None:
            logger.warning(
                "Starting server with no token secret and disabling authentication. "
            )

        self.v1 = V1HttpApi(
            settings,
            override_transport=override_transport,
        )
        self.asgi_app = Litestar(
            debug=debug,
            cors_config=cors_config,
            route_handlers=[self.v1.router],
            openapi_config=openapi_config,
            on_startup=[self.v1.on_startup],
        )

    def simulate_startup(self) -> None:
        self.v1.on_startup(self.asgi_app)
