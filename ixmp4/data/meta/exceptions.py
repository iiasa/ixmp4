from ixmp4.base_exceptions import (
    BadRequest,
    DeletionPrevented,
    NotFound,
    NotUnique,
    registry,
)


@registry.register()
class RunMetaEntryNotFound(NotFound):
    pass


@registry.register()
class RunMetaEntryNotUnique(NotUnique):
    pass


@registry.register()
class RunMetaEntryDeletionPrevented(DeletionPrevented):
    message = "Cannot delete metadata entry: it has dependencies."


@registry.register()
class InvalidRunMeta(BadRequest):
    message = "Invalid run meta entry."
