from typing import Any, Sequence

import sqlalchemy as sa
from toolkit.db.filter import Filter
from toolkit.db.repositories import ItemRepository as BaseItemRepository
from toolkit.db.repositories import PandasRepository as BasePandasRepository
from toolkit.db.target import ModelTarget

from ixmp4.data.model.db import Model, ModelVersion
from ixmp4.data.model.exceptions import (
    ModelNotFound,
    ModelNotUnique,
)
from ixmp4.data.model.filter import IamcModelFilter
from ixmp4.data.model.repositories import ModelAuthRepository
from ixmp4.data.run.db import Run


class IamcModelTarget(ModelTarget[Model | ModelVersion]):
    def select_statement(
        self, columns: Sequence[str] | None = None
    ) -> sa.Select[tuple[Any, ...]]:
        return (
            super()
            .select_statement(columns=columns)
            .where(Model.runs.any(Run.timeseries.any()))
        )


class ItemRepository(ModelAuthRepository, BaseItemRepository[Model]):
    NotFound = ModelNotFound
    NotUnique = ModelNotUnique
    target = IamcModelTarget(Model)
    filter = Filter(IamcModelFilter, Model)


class PandasRepository(ModelAuthRepository, BasePandasRepository):
    NotFound = ModelNotFound
    NotUnique = ModelNotUnique
    target = IamcModelTarget(Model)
    filter = Filter(IamcModelFilter, Model)
