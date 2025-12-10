from ixmp4.base_exceptions import (
    DeletionPrevented,
    NotFound,
    NotUnique,
    registry,
)


@registry.register()
class ScalarNotFound(NotFound):
    pass


@registry.register()
class ScalarNotUnique(NotUnique):
    pass


@registry.register()
class ScalarDeletionPrevented(DeletionPrevented):
    pass
