from typing import ClassVar

from ixmp4.db import Session, filters, sql

from .. import RunMetaEntry


class RunMetaEntryFilter(filters.BaseFilter, metaclass=filters.FilterMeta):
    sqla_model: ClassVar[type] = RunMetaEntry

    id: filters.Id
    dtype: filters.String
    run__id: filters.Integer | None = filters.Field(None, alias="run_id")

    key: filters.String

    value_int: filters.Integer
    value_str: filters.String
    value_float: filters.Float
    value_bool: filters.Boolean

    # NOTE specific type hint here is based on usage; adapt accordingly
    def join(
        self, exc: sql.Select[tuple[RunMetaEntry]], session: Session | None = None
    ) -> sql.Select[tuple[RunMetaEntry]]:
        return exc
