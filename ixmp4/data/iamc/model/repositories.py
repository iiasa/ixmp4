from typing import Any, Sequence

import sqlalchemy as sa
from toolkit import db

from ixmp4.data.model.db import Model
from ixmp4.data.model.exceptions import (
    ModelNotFound,
    ModelNotUnique,
)
from ixmp4.data.model.filter import IamcModelFilter
from ixmp4.data.model.repositories import ModelAuthRepository
from ixmp4.data.run.db import Run


class IamcModelTarget(db.r.ModelTarget[Model]):
    def select_statement(
        self, columns: Sequence[str] | None = None
    ) -> sa.Select[tuple[Any, ...]]:
        return (
            super()
            .select_statement(columns=columns)
            .where(Model.runs.any(Run.timeseries.any()))
        )


class ItemRepository(ModelAuthRepository, db.r.ItemRepository[Model]):
    NotFound = ModelNotFound
    NotUnique = ModelNotUnique
    target = IamcModelTarget(Model)
    filter = db.r.Filter(IamcModelFilter, Model)


class PandasRepository(ModelAuthRepository, db.r.PandasRepository):
    NotFound = ModelNotFound
    NotUnique = ModelNotUnique
    target = IamcModelTarget(Model)
    filter = db.r.Filter(IamcModelFilter, Model)
