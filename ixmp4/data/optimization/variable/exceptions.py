from ixmp4.base_exceptions import (
    DeletionPrevented,
    NotFound,
    NotUnique,
    OptimizationDataValidationError,
    registry,
)


@registry.register(name="OptimizationVariableNotFound")
class VariableNotFound(NotFound):
    message = "Variable not found."


@registry.register(name="OptimizationVariableNotUnique")
class VariableNotUnique(NotUnique):
    message = "Variable is not unique."


@registry.register(name="OptimizationVariableDeletionPrevented")
class VariableDeletionPrevented(DeletionPrevented):
    pass


@registry.register(name="OptimizationVariableDataInvalid")
class VariableDataInvalid(OptimizationDataValidationError):
    message = "Invalid data for Variable."
