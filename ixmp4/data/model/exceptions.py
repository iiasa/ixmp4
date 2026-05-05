from ixmp4.base_exceptions import DeletionPrevented, NotFound, NotUnique, registry


@registry.register()
class ModelNotFound(NotFound):
    message = "Model not found."


@registry.register()
class ModelNotUnique(NotUnique):
    message = "Model is not unique."


@registry.register()
class ModelDeletionPrevented(DeletionPrevented):
    message = "Cannot delete model: it is referenced by runs."
