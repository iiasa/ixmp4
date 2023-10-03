from typing_extensions import Annotated

from ixmp4.db import filters

from .. import Model, Run


class ModelFilter(filters.BaseFilter, metaclass=filters.FilterMeta):
    id: Annotated[filters.Id | None, filters.Field(None)]
    name: Annotated[filters.String | None, filters.Field(None)]

    _sqla_model = Model

    def join(self, exc, **kwargs):
        return exc.join(Model, Run.model)
