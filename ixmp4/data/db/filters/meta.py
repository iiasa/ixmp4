from typing import ClassVar

from ixmp4.db import filters

from .. import RunMetaEntry


class RunMetaEntryFilter(filters.BaseFilter, metaclass=filters.FilterMeta):
    sqla_model: ClassVar[type] = RunMetaEntry

    id: filters.Id
    type: filters.String
    run__id: filters.Integer = filters.Field(None, alias="run_id")

    key: filters.String

    value_int: filters.Integer
    value_str: filters.String
    value_float: filters.Float
    value_bool: filters.Boolean

    def join(self, exc, **kwargs):
        return exc
