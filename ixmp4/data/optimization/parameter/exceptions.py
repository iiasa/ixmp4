from ixmp4.base_exceptions import (
    DeletionPrevented,
    NotFound,
    NotUnique,
    OptimizationDataValidationError,
    registry,
)


@registry.register()
class ParameterNotFound(NotFound):
    message = "Parameter not found."


@registry.register()
class ParameterNotUnique(NotUnique):
    message = "Parameter is not unique."


@registry.register()
class ParameterDeletionPrevented(DeletionPrevented):
    message = "Cannot delete parameter: it has dependencies."


@registry.register()
class ParameterDataInvalid(OptimizationDataValidationError):
    message = "Invalid data for Parameter."
