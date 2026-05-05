from ixmp4.base_exceptions import DeletionPrevented, NotFound, NotUnique, registry


@registry.register()
class UnitNotFound(NotFound):
    pass


@registry.register()
class UnitNotUnique(NotUnique):
    pass


@registry.register()
class UnitDeletionPrevented(DeletionPrevented):
    message = "Cannot delete unit: it is used in parameters, variables, or scalars."
