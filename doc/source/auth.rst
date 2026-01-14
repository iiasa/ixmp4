Authentication & Authorization
==============================

IXMP4 has built-in authentication and authorization facilities, 
which can restrict access for different users according to a 
custom permission system. 

Almost all operations on a platform require "view", "submit", 
"edit" or "manage" permissions if authorization is enabled. 
Additionally, when querying data, a given user may not be 
able to see the totality of data in the platform. 

Authenticating with IIASA Infrastructure
----------------------------------------

IIASA provides a number of "public", "gated" and "private" ixmp4 instances. 
To access "gated" instances and enable instance managers give you 
access to or permissions on their instances, you will need an account with
the |ece_management_service|.
    
.. |ece_management_service| raw:: html

   <a href="https://manager.ece.iiasa.ac.at/" target="_blank">ECE Management Service</a>

Once active, your account can be used to log in via 
the ``login`` `console command <cli>`

.. code:: bash

    ixmp4 login <username>

You will be prompted to enter your password.

.. warning::

    Your username and password will be saved locally in plain-text for future use!

To list the instances you have access to:

.. code:: bash

    ixmp4 platforms list

From a Python environment, you can then connect to any of these platforms
using the following code (provided you enjoy the necessary permissions):

.. code:: python

    import ixmp4
    platform = ixmp4.Platform("<platform-name>")


Local Server Auth
-----------------

By default, the ``ixmp4 server start`` command will start an
ixmp4 server without any authentication mechanisms or checks.

To enable authentication, supply the :class:`~ixmp4.server.Ixmp4Server` class
with a :class:`~ixmp4.conf.settings.ServerSettings` class 
that has a ``secret_hs256`` configuration variable.

.. code:: python

    from ixmp4.server import Ixmp4Server
    from ixmp4.conf.settings import ServerSettings

    server = Ixmp4Server(ServerSettings(secret_hs256="changeme"))

    # ... use server.asgi_app to start a server

Or set the ``IXMP4_SERVER__SECRET_HS256`` environment variable:

.. code:: bash

    IXMP4_SERVER__SECRET_HS256=changeme ixmp4 server start

A client connecting to a server started in this manner has to be
configured using the :attr:`~ixmp4.conf.settings.ClientSettings.secret_hs256` 
configuration variable to enable unrestricted use:

.. code:: python

    import ixmp4
    from ixmp4.conf.settings import ClientSettings
    from ixmp4.transport import HttpxTransport
    from ixmp4.backend import Backend

    transport = HttpxTransport.from_url(
        "http://localhost:9000/v1/test/",
        ClientSettings(secret_hs256="changeme"),
    )
    platform = ixmp4.Platform(_backend=Backend(transport))

This will give anyone with knowledge of the secret superuser access 
to the local server instance. 
Anyone connecting without a secret will receive an "unauthorized" response.
