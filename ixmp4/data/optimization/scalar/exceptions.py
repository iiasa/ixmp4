from ixmp4.base_exceptions import (
    DeletionPrevented,
    NotFound,
    NotUnique,
    registry,
)


@registry.register()
class ScalarNotFound(NotFound):
    message = "Scalar not found."


@registry.register()
class ScalarNotUnique(NotUnique):
    message = "Scalar is not unique."


@registry.register()
class ScalarDeletionPrevented(DeletionPrevented):
    message = "Cannot delete scalar: it has dependencies."
