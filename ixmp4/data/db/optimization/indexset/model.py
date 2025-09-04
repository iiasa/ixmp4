from typing import ClassVar

import numpy as np

from ixmp4 import db
from ixmp4.core.exceptions import OptimizationDataValidationError
from ixmp4.data import types
from ixmp4.data.abstract import optimization as abstract
from ixmp4.data.db import versions

from .. import base


class IndexSet(base.RunLinkedBaseModel):
    __tablename__ = "optimization_indexset"

    NotFound: ClassVar = abstract.IndexSet.NotFound
    NotUnique: ClassVar = abstract.IndexSet.NotUnique
    DataInvalid: ClassVar = OptimizationDataValidationError
    DeletionPrevented: ClassVar = abstract.IndexSet.DeletionPrevented

    _data_type: types.OptimizationDataType

    _data: types.Mapped[list["IndexSetData"]] = db.relationship(
        back_populates="indexset",
        order_by="IndexSetData.id",
        cascade="all, delete",
        passive_deletes=True,
    )

    @property
    def data(self) -> list[float] | list[int] | list[str]:
        return (
            []
            if self._data_type is None
            else np.array([d.value for d in self._data], dtype=self._data_type).tolist()
        )

    @data.setter
    def data(self, value: list[float] | list[int] | list[str]) -> None:
        return None

    # run__id: types.RunId

    updateable_columns = ["_data_type"]

    __table_args__ = (db.UniqueConstraint("name", "run__id"),)


class IndexSetData(base.RootBaseModel):
    __tablename__ = "optimization_indexsetdata"

    indexset: types.Mapped["IndexSet"] = db.relationship(back_populates="_data")
    indexset__id: types.IndexSet__Id
    value: types.String = db.Column(db.String, nullable=False)

    __table_args__ = (db.UniqueConstraint("indexset__id", "value"),)


class IndexSetVersion(versions.RunLinkedVersionModel):
    __tablename__ = "optimization_indexset_version"

    name: types.String = db.Column(db.String(255), nullable=False)
    run__id: db.MappedColumn[int] = db.Column(db.Integer, nullable=False, index=True)

    _data_type: types.OptimizationDataType

    created_at: types.DateTime = db.Column(nullable=True)
    created_by: types.Username


class IndexSetDataVersion(versions.DefaultVersionModel):
    __tablename__ = "optimization_indexsetdata_version"

    indexset__id: db.MappedColumn[int] = db.Column(
        db.Integer, nullable=False, index=True
    )
    value: types.String = db.Column(db.String, nullable=False)


version_triggers = versions.PostgresVersionTriggers(
    IndexSet.__table__, IndexSetVersion.__table__
)
data_version_triggers = versions.PostgresVersionTriggers(
    IndexSetData.__table__, IndexSetDataVersion.__table__
)
