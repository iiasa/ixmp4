from typing import ClassVar

from ixmp4.db import Session, filters, utils

from .. import Model, Run
from ..base import SelectType


class ModelFilter(filters.BaseFilter, metaclass=filters.FilterMeta):
    id: filters.Id | None = filters.Field(None)
    name: filters.String | None = filters.Field(None)

    sqla_model: ClassVar[type] = Model

    # TODO using general form here as this seems to be callable on non-Model tables
    def join(self, exc: SelectType, session: Session | None = None) -> SelectType:
        if not utils.is_joined(exc, Model):
            exc = exc.join(Model, Run.model)
        return exc
