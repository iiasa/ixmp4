from ixmp4.base_exceptions import DeletionPrevented, NotFound, NotUnique, registry


@registry.register()
class DataPointNotFound(NotFound):
    pass


@registry.register()
class DataPointNotUnique(NotUnique):
    pass


@registry.register()
class DataPointDeletionPrevented(DeletionPrevented):
    pass
