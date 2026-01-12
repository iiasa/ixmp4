Developer Documentation
=======================

.. toctree::
   :maxdepth: 1

   ixmp4.core/modules
   ixmp4.data/modules
   ixmp4.server/modules
   ixmp4.cli
   ixmp4.db
   ixmp4.conf
   tests


Package/Folder Structure
------------------------

.. code:: bash

   .
   ├── ixmp4
   │   ├── cli                 # cli
   │   ├── conf                # configuration module, loads settings etc.
   │   ├── core                # contains the facade layer for the core python API
   │   ├── data                # data layer classes used by the APIs
   │   ├── db                  # database management
   │   └── server              # web application server
   ├── run                     # runtime artifacts
   └── tests                   # tests

Architecture
------------

ixmp4 provides a Python API, a REST API and a compatibility layer for Postgres and SQLite Databases.
The Python API can interact with databases directly or use the REST API of a compatible ixmp4 server instance.

::

                            Web or SQL
            Platform         Backend                 SQL Backend
      │  ┌────────────┐   ┌───────────┐    ┌─    │  ┌───────────┐  ─┐      │  ┌─┐
    P │  │            │   │ Service   │    │     │  │ Service   │   │    S │  │ │
    y │  │ ┌────────┐ │   │ ┌───────┐ │    │   R │  │ ┌───────┐ │   │    Q │  │D│
    t │  │ │Facade  │ │   │ │       │ │  ┌─┘   E │  │ │       │ │   └─┐  L │  │a│
    h │  │ │Object  │ │   │ │Model  │ │  │     S │  │ │Model  │ │     │  A │  │t│
    o │  │ ├────────┤ │   │ ├───────┤ │  │     T │  │ ├───────┤ │     │  l │  │a│
    n │  │ │Facade  │ │   │ │Repo.  │ │  │       │  │ │Repo.  │ │     │  c │  │b│
      │  │ │        │ │   │ │       │ │  │     A │  │ │       │ │     │  h │  │a│
    A │  │ └────────┘ │   │ ├───────┤ │  └─┐   P │  │ ├───────┤ │   ┌─┘  e │  │s│
    P │  │            │   │ │(Auth.)│ │    │   I │  │ │Auth.  │ │   │    m │  │e│
    I │  │            │   │ └───────┘ │    │     │  │ └───────┘ │   │    y │  │ │
      │  └────────────┘   └───────────┘    └─    │  └───────────┘  ─┘      │  └─┘


The :mod:`ixmp4.data` module organizes each datatype into a few files for consistency:

- **db.py**: sqlalchemy database models and other database definitions
- **dto.py**: a data transfer class for item serialization
- **exceptions.py**: exceptions specific to the datatype (NotFound, NotUnique, etc.)
- **filter.py**: filter definitions for use in repositories
- **repositories.py**: repository classes responsible for interacting with the database
- **service.py**: service class as the main interface for the datatype which combines all of the above

The service classes are instantiated together via a :class:`ixmp4.backend.Backend` object 
which can be used by other code to perform operations in the database or on a remote ixmp4 
http server. This construct and its classes can be referred to as the ":doc:`data layer <ixmp4.data/modules>`".

For a user-friendly python API an additional "facade layer" is added in the :doc:`ixmp4.core <ixmp4.core/modules>`
module on top of the data layer.
