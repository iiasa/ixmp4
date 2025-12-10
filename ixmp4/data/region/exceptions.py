from ixmp4.base_exceptions import DeletionPrevented, NotFound, NotUnique, registry


@registry.register()
class RegionNotFound(NotFound):
    pass


@registry.register()
class RegionNotUnique(NotUnique):
    pass


@registry.register()
class RegionDeletionPrevented(DeletionPrevented):
    pass
