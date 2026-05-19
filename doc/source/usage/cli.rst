Command-line interface
======================

.. automodule:: ixmp4.cli
   :show-inheritance:


Platforms
---------

.. typer:: ixmp4.cli.app:platforms
   :prog: ixmp4 platforms
   :width: 70
   :preferred: text

Create a local sqlite platform:

.. code:: bash

   ixmp4 platforms add test [--dsn sqlite://my/database/file.db] 

If ``--dsn`` is not supplied you will be prompted to create an sqlite database at a standardized location.

Register a postgresql database:

.. code:: bash

   ixmp4 platforms add postgres --dsn postgresql+psycopg://user:pw@host/db


List all available platforms from the local toml file and the configured manager API:

.. code:: bash

   ixmp4 platforms list

Remove platforms from your local toml file:

.. code:: bash

   ixmp4 platforms delete test
   ixmp4 platforms delete postgres

.. _cli-iiasa:

IIASA Infrastructure
--------------------

By default only public IIASA platforms will be available to all users.
If you want to log in using your ECE Manger account, use the ``ixmp4 login <username>`` command.

.. typer:: ixmp4.cli.app:login
   :prog: ixmp4 login
   :width: 70
   :preferred: text

To delete locally saved credentials and return to anonymous use: 

.. typer:: ixmp4.cli.app:logout
   :prog: ixmp4 logout
   :width: 70
   :preferred: text

Alembic CLI
-----------

.. typer:: ixmp4.cli.app:alembic
   :prog: ixmp4 alembic
   :width: 70
   :preferred: text

IXMP4 Server
------------

.. typer:: ixmp4.cli.app:server
   :prog: ixmp4 server
   :width: 70
   :preferred: text
