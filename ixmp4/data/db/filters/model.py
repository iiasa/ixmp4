from typing import ClassVar

from ixmp4.db import filters

from .. import Model, Run


class ModelFilter(filters.BaseFilter, metaclass=filters.FilterMeta):
    id: filters.Id | None = filters.Field(None)
    name: filters.String | None = filters.Field(None)

    sqla_model: ClassVar = Model

    def join(self, exc, **kwargs):
        return exc.join(Model, Run.model)
