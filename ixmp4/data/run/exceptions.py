from ixmp4.base_exceptions import (
    BadRequest,
    ConstraintViolated,
    NotFound,
    NotUnique,
    registry,
)


@registry.register()
class RunNotFound(NotFound):
    message = "Run not found."


@registry.register()
class RunNotUnique(NotUnique):
    message = "Run is not unique."


@registry.register()
class RunDeletionPrevented(ConstraintViolated):
    pass


@registry.register()
class NoDefaultRunVersion(BadRequest):
    message = "No default version available for this run."


@registry.register()
class RunIsLocked(BadRequest):
    message = (
        "This run is already locked by another transaction. "
        "Use the `timeout` parameter on `run.transact()` (or `run.lock()`) "
        "to wait for the lock or forcibly unlock it with `run.unlock(force=True)`."
    )


@registry.register()
class RunLockRequired(BadRequest):
    message = (
        "This operation requires an active run lock. "
        "Use ``with run.transact('description'):`` to acquire one."
    )
    http_error_name = "run_lock_required"
