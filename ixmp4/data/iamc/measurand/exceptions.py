from ixmp4.base_exceptions import DeletionPrevented, NotFound, NotUnique, registry


@registry.register()
class MeasurandNotFound(NotFound):
    pass


@registry.register()
class MeasurandNotUnique(NotUnique):
    pass


@registry.register()
class MeasurandDeletionPrevented(DeletionPrevented):
    pass
