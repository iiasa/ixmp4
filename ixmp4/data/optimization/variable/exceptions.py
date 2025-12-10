from ixmp4.base_exceptions import (
    DeletionPrevented,
    NotFound,
    NotUnique,
    OptimizationDataValidationError,
    registry,
)


@registry.register(name="OptimizationVariableNotFound")
class VariableNotFound(NotFound):
    pass


@registry.register(name="OptimizationVariableNotUnique")
class VariableNotUnique(NotUnique):
    pass


@registry.register(name="OptimizationVariableDeletionPrevented")
class VariableDeletionPrevented(DeletionPrevented):
    pass


@registry.register(name="OptimizationVariableDataInvalid")
class VariableDataInvalid(OptimizationDataValidationError):
    pass
