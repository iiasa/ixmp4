"""

Run the web api with:

.. code:: bash

   ixmp4 server start [--host 127.0.0.1] [--port 8000]

This will start ixmp4’s asgi server. Check
``http://127.0.0.1:8000/v1/<platform>/docs/``.

"""

from typing import Any, Callable

from fastapi import FastAPI

from . import v1
from .deps import get_direct_toml_transport


def get_app(transport_dep: Callable[..., Any] = get_direct_toml_transport) -> FastAPI:
    app = FastAPI()

    app.mount("/v1/{platform}", v1.get_app(transport_dep))
    return app
