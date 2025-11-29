from ixmp4.base_exceptions import (
    DeletionPrevented,
    NotFound,
    NotUnique,
    OptimizationDataValidationError,
    registry,
)


@registry.register()
class EquationNotFound(NotFound):
    pass


@registry.register()
class EquationNotUnique(NotUnique):
    pass


@registry.register()
class EquationDeletionPrevented(DeletionPrevented):
    pass


@registry.register()
class EquationDataInvalid(OptimizationDataValidationError):
    pass
