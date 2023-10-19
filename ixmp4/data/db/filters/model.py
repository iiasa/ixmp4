from typing import ClassVar

from ixmp4.db import filters

from .. import Model, Run


class ModelFilter(filters.BaseFilter, metaclass=filters.FilterMeta):
    id: filters.Id
    name: filters.String

    sqla_model: ClassVar[type] = Model

    def join(self, exc, **kwargs):
        return exc.join(Model, Run.model)
