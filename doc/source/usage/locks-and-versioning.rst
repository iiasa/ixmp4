Locks & Versioning
==================

To ensure changes to platform data are properly recorded,
ixmp4 has a data versioning mechanism in place on PostgreSQL
databases.
This mechanism records all rows changed by statements executed
on marked tables and associates them with transaction records.
In order to coordinate between many concurrent clients, a locking
mechanism is implemented around the :class:`~ixmp4.core.run.Run` class.
This locking mechanism is also enabled on SQLite databases.

Locking Runs
------------

To acquire the lock on a :class:`~ixmp4.core.run.Run`, 
the :meth:`~ixmp4.core.run.Run.transact` context manager may be used.

.. automethod:: ixmp4.core.run.Run.transact
    :no-index:
    
Internally, ixmp4 will save the last transaction id in the database as
the ``lock_transaction`` column for the run's row.
Any subsequent attempt to lock the run will result in an exception as 
the column will be read and only updated if it currently contains ``NULL`` / ``None``.
No additional checks for lock ownership are implemented in the data layer.

Any well-behaved ixmp4 client must remember that it has acquired this 
run's lock, for example with the :attr:`~ixmp4.core.run.Run.owns_lock` property.
Lock ownership is bound to a single run, not the entire platform, so only one of
these instances may acquire a lock. 

.. code:: python

    run1 = platform.runs.get("Model", "Scenario")
    run2 = platform.runs.get("Model", "Scenario")
    
    with run1.transact("Add xyz data"):
        run2.iamc.add(...)
        # ^ `RunIsLocked`
    
    with run1.transact("Add xyz data"):
        with run2.transact("Second locking attempt.")
            # ^ `RunIsLocked`
            pass

    with run1.transact("Add xyz data"):
        run1.iamc.add(...)
        # ^ works!

.. note::
        
    All other operations that potentially result in a 
    :class:`~ixmp4.data.run.exceptions.RunIsLocked` exception do so solely
    because of **client-side checks**, making the locking mechanism essentially
    a **method of coordination** for clients that rely on versioning facilities
    and thus **not a reliable security mechanism** in any manner.

Checkpoints
-----------

Checkpoints are used to label important transactions in the transaction history
as to provide "anchors" to revert to. They are created automatically at the exit 
point of the :meth:`~ixmp4.core.run.Run.transact` context manager and can be manually 
created at any other point. 

.. code:: python

    with run.transact("Add data"):
        run.iamc.add(...)
        run.checkpoints.create("Add IAMC Data")
        #> Checkpoint "Add IAMC Data" created
        run.meta["key"] = "value"
    #> Checkpoint "Add data" created

If an exception occurs within a :meth:`~ixmp4.core.run.Run.transact` block,
data in the run will be rolled back to the latest checkpoint on platforms 
that support versioning. 

Checkpoint Views
----------------

Checkpoint views provide read-only access to run data at a specific checkpoint.
Access them through ``run.checkpoints[checkpoint_id]``.

.. autoclass:: ixmp4.core.checkpoint.Checkpoint
    :members:

For IAMC data, a checkpoint-specific view is exposed by
``Checkpoint.iamc``:

.. autoclass:: ixmp4.core.iamc.checkpoint.CheckpointIamcData
    :members:

For optimization data, checkpoint-specific views are exposed by
``Checkpoint.optimization``:

.. autoclass:: ixmp4.core.optimization.checkpoint.CheckpointScalarView
    :members:

.. autoclass:: ixmp4.core.optimization.checkpoint.CheckpointTableView
    :members:

.. autoclass:: ixmp4.core.optimization.checkpoint.CheckpointParameterView
    :members:

.. autoclass:: ixmp4.core.optimization.checkpoint.CheckpointEquationView
    :members:

.. autoclass:: ixmp4.core.optimization.checkpoint.CheckpointVariableView
    :members:

.. autoclass:: ixmp4.core.optimization.checkpoint.CheckpointIndexSetView
    :members:
