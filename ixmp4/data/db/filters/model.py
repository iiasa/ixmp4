from typing import ClassVar

from ixmp4.db import Session, filters, sql, utils

from .. import Model, Run


class ModelFilter(filters.BaseFilter, metaclass=filters.FilterMeta):
    id: filters.Id | None = filters.Field(None)
    name: filters.String | None = filters.Field(None)

    sqla_model: ClassVar[type] = Model

    def join(self, exc: sql.Select, session: Session | None = None) -> sql.Select:
        if not utils.is_joined(exc, Model):
            exc = exc.join(Model, Run.model)
        return exc
