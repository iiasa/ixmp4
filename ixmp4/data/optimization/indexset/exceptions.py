from ixmp4.base_exceptions import (
    DeletionPrevented,
    NotFound,
    NotUnique,
    OptimizationDataValidationError,
    registry,
)


@registry.register()
class IndexSetNotFound(NotFound):
    pass


@registry.register()
class IndexSetNotUnique(NotUnique):
    pass


@registry.register()
class IndexSetDeletionPrevented(DeletionPrevented):
    pass


@registry.register()
class IndexSetDataInvalid(OptimizationDataValidationError):
    pass


@registry.register()
class IndexSetDataNotFound(NotFound, IndexSetDataInvalid):
    pass


@registry.register()
class IndexSetDataNotUnique(NotUnique, IndexSetDataInvalid):
    pass
