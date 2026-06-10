Version Tables and Triggers
===========================

On PostgreSQL backends, ixmp4 records version history for selected tables.
Each versioned table has a sibling version table that stores row history.

Version Table Columns
---------------------

Any table with versioning enabled has a sibling table with additional columns
used to represent the history of each row.

* **transaction_id**:
    The transaction in which the row was created, updated, or deleted.
* **end_transaction_id**:
    The transaction at which a newer row superseded this row.
* **operation_type**:
    The :class:`~ixmp4.data.versions.model.Operation` that produced the row
    state.

.. autoclass:: ixmp4.data.versions.model.Operation
    :members:
    :undoc-members:

Triggers
--------

Versioned base tables and their version tables are connected by database
triggers that record changes made by SQL statements.

.. autoclass:: ixmp4.data.versions.PostgresVersionTriggers
    :members:
