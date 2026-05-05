from ixmp4.base_exceptions import DeletionPrevented, NotFound, NotUnique, registry


@registry.register()
class DataPointNotFound(NotFound):
    message = "Datapoint not found."


@registry.register()
class DataPointNotUnique(NotUnique):
    message = "Datapoint is not unique."


@registry.register()
class DataPointDeletionPrevented(DeletionPrevented):
    message = "Cannot delete datapoint: it has dependencies."
