from ixmp4.base_exceptions import DeletionPrevented, NotFound, NotUnique, registry


@registry.register()
class CheckpointNotFound(NotFound):
    pass


@registry.register()
class CheckpointNotUnique(NotUnique):
    pass


@registry.register()
class CheckpointDeletionPrevented(DeletionPrevented):
    pass
