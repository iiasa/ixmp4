"""

Run the web api with:

.. code:: bash

   ixmp4 server start [--host 127.0.0.1] [--port 8000]

This will start ixmp4’s asgi server. Check
``http://127.0.0.1:8000/v1/<platform>/docs/``.

"""

import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any, Sequence

from starlette.applications import Starlette
from toolkit.manager.client import ManagerClient

if TYPE_CHECKING:
    from ixmp4.services import Service
    from ixmp4.transport import DirectTransport

from . import v1

logger = logging.getLogger(__name__)


class Ixmp4Server(Starlette):
    def __init__(
        self,
        secret_hs256: str | None,
        toml_file: Path | None = None,
        manager_client: ManagerClient | None = None,
        override_transport: "DirectTransport | None" = None,
        service_classes: Sequence[type["Service"]] | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)

        if secret_hs256 is None:
            logger.warning(
                "Starting server with no token secret and disabling authentication. "
            )
        self.mount(
            "/v1",
            v1.V1Application(
                secret_hs256,
                toml_file=toml_file,
                manager_client=manager_client,
                service_classes=service_classes,
                override_transport=override_transport,
            ),
        )
