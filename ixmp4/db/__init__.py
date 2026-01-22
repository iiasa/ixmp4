"""

Database operations are executed using `sqlalchemy <https://www.sqlalchemy.org/>`__
as a programmatic database API and
`alembic <https://alembic.sqlalchemy.org/en/latest/>`__ for database
migration management.

Migrations
----------

Migrations are run automatically when creating a new sqlite database:

.. code:: bash

   $ ixmp4 platforms add test

   No DSN supplied, assuming you want to add a local sqlite database...
   No file at the standard filesystem location for name 'test' exists. \
   Do you want to create a new database? [y/N]: y

   Creating the database and running migrations...

   [INFO] 16:51:40 - alembic.runtime.migration: Context impl SQLiteImpl.
   [INFO] 16:51:40 - alembic.runtime.migration: Will assume non-transactional DDL.
   [INFO] 16:51:40 - alembic.runtime.migration: Running upgrade  -> c71efc396d2b, \
   Initial Migration
   ...

If the ixmp4 data model changes and the database models are updated,
a new migration has to be created to update the database table definitions.

To generate a new migration automatically, you will need an existing up-to-date database
to compare and a descriptive message for the migration (think commit-message):

.. code:: bash

   $ ixmp4 alembic -p test autogenerate "My migration message."

   [INFO] 16:54:51 - alembic.runtime.migration: Context impl SQLiteImpl.
   [INFO] 16:54:51 - alembic.runtime.migration: Will assume non-transactional DDL.
   Generating /home/wolschlager/Code/ixmp4/ixmp4/db/migrations/versions/\
fbe0529cafe7_test.py ...  done
   Running post write hook 'ruff' ...
   1 file reformatted
   done

Afterwards you can update existing databases with the upgrade command:

.. code:: bash

   $ ixmp4 alembic -p test upgrade

   [INFO] 17:02:06 - alembic.runtime.migration: Context impl SQLiteImpl.
   [INFO] 17:02:06 - alembic.runtime.migration: Will assume non-transactional DDL.
   [INFO] 17:02:06 - alembic.runtime.migration: Running upgrade 8b0797ebf42f -> \
5069a5e8c20c, test

   Upgraded database 'sqlite:////home/wolschlager/Code/ixmp4/run/storage/databases/\
test.sqlite3' to revision 'head'.

Refer to the :doc:`cli documentation <cli>` for all alembic cli commands.

"""
