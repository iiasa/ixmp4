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
    pass


@registry.register()
class InvalidRunMeta(BadRequest):
    message = "Invalid run meta entry."
