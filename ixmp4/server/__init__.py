"""

Run the web api with:

.. code:: bash

   ixmp4 server start [--host 127.0.0.1] [--port 8000]

This will start ixmp4’s asgi server. Check
``http://127.0.0.1:8000/v1/<platform>/docs/``.

"""

from fastapi import FastAPI

from .rest import v1

app = FastAPI()

app.mount("/v1/{platform}", v1)
