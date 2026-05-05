from ixmp4.base_exceptions import (
    DeletionPrevented,
    NotFound,
    NotUnique,
    OptimizationDataValidationError,
    registry,
)


@registry.register()
class IndexSetNotFound(NotFound):
    message = "Index set not found."


@registry.register()
class IndexSetNotUnique(NotUnique):
    message = "Index set is not unique."


@registry.register()
class IndexSetDeletionPrevented(DeletionPrevented):
    message = "Cannot delete index set: it has dependencies."


@registry.register()
class IndexSetDataInvalid(OptimizationDataValidationError):
    pass


@registry.register()
class IndexSetDataNotFound(NotFound, IndexSetDataInvalid):
    pass


@registry.register()
class IndexSetDataNotUnique(NotUnique, IndexSetDataInvalid):
    pass
