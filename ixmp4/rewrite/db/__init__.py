"""
This module is responsible for everything database related.
Interfacing, migrating and more is all done here.

It uses `sqlalchemy <https://www.sqlalchemy.org/>`__ and
`alembic <https://alembic.sqlalchemy.org/en/latest/>`__ for database
management.

Migrations
----------

There is a development database at ``run/db.sqlite`` which is used for
generating migrations, nothing else. It can be manipulated with alembic
directly using these commands:

.. code:: bash

   # run all migrations until the current state is reached
   alembic upgrade head

   # run one migration forward
   alembic upgrade +1

   # run one migration backward
   alembic downgrade -1

   # autogenerate new migration (please choose a descriptive change message)
   alembic revision -m "<message>" --autogenerate

You will have to run all migrations before being able to create new ones
in the development database. Be sure to run ``ruff`` on newly created
migrations before committing them!

TODO
"""
