from ixmp4.base_exceptions import DeletionPrevented, NotFound, NotUnique, registry


@registry.register()
class RegionNotFound(NotFound):
    message = "Region not found."


@registry.register()
class RegionNotUnique(NotUnique):
    message = "Region is not unique."


@registry.register()
class RegionDeletionPrevented(DeletionPrevented):
    message = "Cannot delete region: it is used in timeseries data."
