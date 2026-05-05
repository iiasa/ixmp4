from ixmp4.base_exceptions import DeletionPrevented, NotFound, NotUnique, registry


@registry.register()
class UnitNotFound(NotFound):
    message = "Unit not found."


@registry.register()
class UnitNotUnique(NotUnique):
    message = "Unit is not unique."


@registry.register()
class UnitDeletionPrevented(DeletionPrevented):
    message = "Cannot delete unit: it is used in parameters, variables, or scalars."
