from ixmp4.data.iamc.datapoint.exceptions import (
    DataPointDeletionPrevented,
    DataPointNotFound,
    DataPointNotUnique,
)
from ixmp4.data.iamc.datapoint.filter import FacadeDataPointFilter
from ixmp4.data.iamc.datapoint.type import Type as Type


class DataPoint:
    Type = Type
    Filter = FacadeDataPointFilter

    NotFound = DataPointNotFound
    NotUnique = DataPointNotUnique
    DeletionPrevented = DataPointDeletionPrevented
