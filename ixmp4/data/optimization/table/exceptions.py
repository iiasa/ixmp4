from ixmp4.base_exceptions import (
    DeletionPrevented,
    NotFound,
    NotUnique,
    OptimizationDataValidationError,
    registry,
)


@registry.register()
class TableNotFound(NotFound):
    pass


@registry.register()
class TableNotUnique(NotUnique):
    pass


@registry.register()
class TableDeletionPrevented(DeletionPrevented):
    message = "Cannot delete table: it has dependencies."


@registry.register()
class TableDataInvalid(OptimizationDataValidationError):
    pass
