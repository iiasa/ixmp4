Structure and Architecture
==========================

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

ixmp4 provides a Python API, a HTTP API and a compatibility layer for Postgres and SQLite Databases.
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

The :doc:`ixmp4.data <data/modules>` module exposes a common interface for direct and remote usage via the 
:class:`~ixmp4.data.services.base.Service` classes. 
See the :doc:`/development/services` section for more information.


For a user-friendly python API an additional "facade layer" centered around the 
:doc:`Platform <core/platform>` class is added in the ``ixmp4.core``
module on top of the data layer.

