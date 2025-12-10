from ixmp4.base_exceptions import (
    BadRequest,
    ConstraintViolated,
    NotFound,
    NotUnique,
    registry,
)


@registry.register()
class RunNotFound(NotFound):
    pass


@registry.register()
class RunNotUnique(NotUnique):
    pass


@registry.register()
class RunDeletionPrevented(ConstraintViolated):
    pass


@registry.register()
class NoDefaultRunVersion(BadRequest):
    message = "No default version available for this run."


@registry.register()
class RunIsLocked(BadRequest):
    message = "This run is already locked."


@registry.register()
class RunLockRequired(BadRequest):
    http_error_name = "run_lock_required"
