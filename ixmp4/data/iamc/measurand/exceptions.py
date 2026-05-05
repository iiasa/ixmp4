from ixmp4.base_exceptions import DeletionPrevented, NotFound, NotUnique, registry


@registry.register()
class MeasurandNotFound(NotFound):
    message = "Measurand not found."


@registry.register()
class MeasurandNotUnique(NotUnique):
    message = "Measurand is not unique."


@registry.register()
class MeasurandDeletionPrevented(DeletionPrevented):
    message = "Cannot delete measurand: it has dependencies."
