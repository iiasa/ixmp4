from ixmp4.base_exceptions import (
    DeletionPrevented,
    NotFound,
    NotUnique,
    OptimizationDataValidationError,
    registry,
)


@registry.register()
class EquationNotFound(NotFound):
    message = "Equation not found."


@registry.register()
class EquationNotUnique(NotUnique):
    message = "Equation is not unique."


@registry.register()
class EquationDeletionPrevented(DeletionPrevented):
    message = "Cannot delete equation: it has dependencies."


@registry.register()
class EquationDataInvalid(OptimizationDataValidationError):
    message = "Invalid data for Equation."
