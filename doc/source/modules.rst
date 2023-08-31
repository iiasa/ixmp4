Developer Documentation
=======================

.. toctree::
   :maxdepth: 1

   ixmp4.core/modules
   ixmp4.data/modules
   ixmp4.server/modules
   ixmp4.cli
   ixmp4.db
   ixmp4.db.utils
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
   │   ├── data
   │   │   ├── abstract        # ABCs for data source models and repositories
   │   │   ├── api             # data source implementation for the web api
   │   │   ├── backend         # data source backends
   │   │   └── db              # data source implementation for databases (sqlalchemy)
   │   ├── db                  # database management
   │   ├── server              # web application server
   │       └── rest            # REST endpoints
   ├── run                     # runtime artifacts
   └── tests                   # tests

Architecture
------------

ixmp4 provides a Python API, a REST API and a compatibility layer for Postgres and SQLite Databases.
The Python API can interact with databases directly or use the REST API of a compatible ixmp4 server instance.

::

   -> calls -> 
                            Web or SQL
            Platform         Backend                    Server       SQL Backend
      │  ┌────────────┐   ┌───────────┐    ┌─    │   ┌──────────┐   ┌───────────┐  ─┐      │  ┌─┐
    P │  │            │   │           │    │     │   │          │   │           │   │    S │  │ │
    y │  │ ┌────────┐ │   │ ┌───────┐ │    │   R │   │ ┌──────┐ │   │ ┌───────┐ │   │    Q │  │D│
    t │  │ │        │ │   │ │       │ │  ┌─┘   E │   │ │Endp. │ │   │ │       │ │   └─┐  L │  │a│
    h │  │ │Facade  │ │   │ │Model  │ │  │     S │   │ └──────┘ │   │ │Model  │ │     │  A │  │t│
    o │  │ └────────┘ │   │ ├───────┤ │  │     T │   │          │   │ ├───────┤ │     │  l │  │a│
    n │  │            │   │ ├───────┤ │  │       │   │ ┌──────┐ │   │ ├───────┤ │     │  c │  │b│
      │  │    ...     │   │ │       │ │  │     A │   │ │Endp. │ │   │ │       │ │     │  h │  │a│
    A │  │            │   │ │Repo.  │ │  └─┐   P │   │ └──────┘ │   │ │Repo.  │ │   ┌─┘  e │  │s│
    P │  │            │   │ └───────┘ │    │   I │   │          │   │ └───────┘ │   │    m │  │e│
    I │  │            │   │    ...    │    │     │   │   ...    │   │    ...    │   │    y │  │ │
      │  └────────────┘   └───────────┘    └─    │   └──────────┘   └───────────┘  ─┘      │  └─┘

           ixmp4.core        ixmp4.data                ixmp4.server      ixmp4.data

Note that a REST SDK in another programming language would have to implement only the
components before the bracketed part of the diagram (``ixmp4.data.api`` + optionally a facade layer).

Overall both the “facade” layer and the “data source” layer are split
into “models” (representing a row in a database or a json object) and
“repositories” (representing a database table or a collection of REST
endpoints) which manage these models.
