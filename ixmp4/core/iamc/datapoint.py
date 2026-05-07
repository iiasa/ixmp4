from ixmp4.data.iamc.datapoint.exceptions import (
    DataPointDeletionPrevented,
    DataPointNotFound,
    DataPointNotUnique,
)
from ixmp4.data.iamc.datapoint.type import Type as Type


class DataPoint:
    Type = Type

    NotFound = DataPointNotFound
    NotUnique = DataPointNotUnique
    DeletionPrevented = DataPointDeletionPrevented
