from ixmp4.base_exceptions import (
    DeletionPrevented,
    NotFound,
    NotUnique,
    OptimizationDataValidationError,
    registry,
)


@registry.register()
class TableNotFound(NotFound):
    message = "Table not found."


@registry.register()
class TableNotUnique(NotUnique):
    message = "Table is not unique."


@registry.register()
class TableDeletionPrevented(DeletionPrevented):
    message = "Cannot delete table: it has dependencies."


@registry.register()
class TableDataInvalid(OptimizationDataValidationError):
    message = "Invalid data for Table."
