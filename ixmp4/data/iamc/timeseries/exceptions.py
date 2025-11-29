from ixmp4.base_exceptions import DeletionPrevented, NotFound, NotUnique, registry


@registry.register()
class TimeSeriesNotFound(NotFound):
    pass


@registry.register()
class TimeSeriesNotUnique(NotUnique):
    pass


@registry.register()
class TimeSeriesDeletionPrevented(DeletionPrevented):
    pass
