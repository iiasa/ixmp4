from ixmp4.base_exceptions import DeletionPrevented, NotFound, NotUnique, registry


@registry.register()
class CheckpointNotFound(NotFound):
    message = "Checkpoint not found."


@registry.register()
class CheckpointNotUnique(NotUnique):
    message = "Checkpoint is not unique."


@registry.register()
class CheckpointDeletionPrevented(DeletionPrevented):
    message = "Cannot delete checkpoint: it is referenced."
