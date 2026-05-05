from ixmp4.base_exceptions import (
    DeletionPrevented,
    NotFound,
    NotUnique,
    OptimizationDataValidationError,
    registry,
)


@registry.register()
class ParameterNotFound(NotFound):
    pass


@registry.register()
class ParameterNotUnique(NotUnique):
    pass


@registry.register()
class ParameterDeletionPrevented(DeletionPrevented):
    message = "Cannot delete parameter: it has dependencies."


@registry.register()
class ParameterDataInvalid(OptimizationDataValidationError):
    pass
