from ixmp4.base_exceptions import DeletionPrevented, NotFound, NotUnique, registry


@registry.register()
class TimeSeriesNotFound(NotFound):
    message = "Timeseries not found."


@registry.register()
class TimeSeriesNotUnique(NotUnique):
    message = "Timeseries is not unique."


@registry.register()
class TimeSeriesDeletionPrevented(DeletionPrevented):
    message = "Cannot delete timeseries: it has dependencies."
