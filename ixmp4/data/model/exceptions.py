from ixmp4.base_exceptions import DeletionPrevented, NotFound, NotUnique, registry


@registry.register()
class ModelNotFound(NotFound):
    pass


@registry.register()
class ModelNotUnique(NotUnique):
    pass


@registry.register()
class ModelDeletionPrevented(DeletionPrevented):
    pass
