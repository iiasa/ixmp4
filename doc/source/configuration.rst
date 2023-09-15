Database setup and configuration
================================

The **ixmp4** package provides an interface to local and server-based database instances.

After installing **ixmp4**, you have to configure access to any database instances.

Connections to a server-based database
--------------------------------------

To connect to an **ixmp4 database** hosted by IIASA, you have to provide your username
and password for the IIASA ECE Manager (https://manager.ece.iiasa.ac.at).

In a console, run the following:

.. code-block::

    ixmp4 login <username>

You will be prompted to enter your password.

.. warning::

    Your username and password will be saved locally in plain-text for future use!

From a Python environment, you can then access any **ixmp4 database** hosted by IIASA
using the following code:

.. code-block:: python

    import ixmp4
    mp = ixmp4.Platform("<database-name>")

Creating and accessing a local database instance
------------------------------------------------

To initialize a new database locally, run the following in a console:

.. code-block::

    ixmp4 platforms add <database-name>

By default, an SQLite database will be created. If you want to add an existing database or a database of a different type, use the `--dsn` argument:

.. code-block::
    
    ixmp4 platforms add <database-name> --dsn postgresql+psycopg://user:pw@host/db    

From a Python environment, you can then access this **ixmp4 database** using the
following code:

.. code-block:: python

    import ixmp4
    mp = ixmp4.Platform("<database-name>")

Available instances
-------------------

For a list of available **ixmp4 database** instances, run the following in a console:

.. code-block::

    ixmp4 platforms list
