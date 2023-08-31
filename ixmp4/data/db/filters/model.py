from ixmp4.db import filters

from .. import Model, Run


class ModelFilter(filters.BaseFilter, metaclass=filters.FilterMeta):
    id: filters.Id
    name: filters.String

    class Config:
        sqla_model = Model

    def join(self, exc, **kwargs):
        return exc.join(Model, Run.model)
