from typing import ClassVar, Literal, cast

import pandas as pd

from ixmp4 import db
from ixmp4.core.exceptions import OptimizationDataValidationError
from ixmp4.data import types
from ixmp4.data.abstract import optimization as abstract

from .. import IndexSet, base


class TableIndexsetAssociation(base.RootBaseModel):
    table_prefix = "optimization_"

    table_id: types.TableId
    table: types.Mapped["Table"] = db.relationship(
        back_populates="_table_indexset_associations"
    )
    indexset_id: types.IndexSetId
    indexset: types.Mapped[IndexSet] = db.relationship()

    column_name: types.String = db.Column(db.String(255), nullable=True)


class Table(base.BaseModel):
    # NOTE: These might be mixin-able, but would require some abstraction
    NotFound: ClassVar = abstract.Table.NotFound
    NotUnique: ClassVar = abstract.Table.NotUnique
    DataInvalid: ClassVar = OptimizationDataValidationError
    DeletionPrevented: ClassVar = abstract.Table.DeletionPrevented

    run__id: types.RunId

    __table_args__ = (db.UniqueConstraint("name", "run__id"),)

    _table_indexset_associations: types.Mapped[list[TableIndexsetAssociation]] = (
        db.relationship(back_populates="table", cascade="all, delete-orphan")
    )

    _indexsets: db.AssociationProxy[list[IndexSet]] = db.association_proxy(
        "_table_indexset_associations", "indexset"
    )
    _column_names: db.AssociationProxy[list[str | None]] = db.association_proxy(
        "_table_indexset_associations", "column_name"
    )

    @property
    def indexsets(self) -> list[str]:
        return [indexset.name for indexset in self._indexsets]

    @property
    def column_names(self) -> list[str] | None:
        return cast(list[str], self._column_names) if any(self._column_names) else None

    _data: types.Mapped[list["TableData"]] = db.relationship(
        back_populates="table", order_by="TableData.id"
    )

    @property
    def data(self) -> dict[str, list[float] | list[int] | list[str]]:
        if self._data == []:
            return {}
        else:
            renames: dict[str, str] = {}
            type_map: dict[str, str] = {}
            if self.column_names:
                for i in range(len(self.column_names)):
                    renames[f"Column {i}"] = self.column_names[i]
                    # would only be None if indexset had no data
                    type_map[self.column_names[i]] = cast(
                        Literal["float", "int", "str"], self._indexsets[i]._data_type
                    )
            else:
                for i in range(len(self.indexsets)):
                    renames[f"Column {i}"] = self.indexsets[i]
                    type_map[self.indexsets[i]] = cast(
                        Literal["float", "int", "str"], self._indexsets[i]._data_type
                    )
            return cast(
                dict[str, list[float] | list[int] | list[str]],
                pd.DataFrame.from_records(
                    [
                        {
                            "Column 0": td.value_0,
                            "Column 1": td.value_1,
                            "Column 2": td.value_2,
                            "Column 3": td.value_3,
                            "Column 4": td.value_4,
                            "Column 5": td.value_5,
                            "Column 6": td.value_6,
                            "Column 7": td.value_7,
                            "Column 8": td.value_8,
                            "Column 9": td.value_9,
                            "Column 10": td.value_10,
                            "Column 11": td.value_11,
                            "Column 12": td.value_12,
                            "Column 13": td.value_13,
                            "Column 14": td.value_14,
                        }
                        for td in self._data
                    ]
                )
                .dropna(axis="columns")
                .rename(renames, axis="columns")
                .astype(type_map)
                .to_dict(orient="list"),
            )

    @data.setter
    def data(
        self, value: dict[str, list[float] | list[int] | list[str]] | pd.DataFrame
    ) -> None:
        return None


class TableData(base.RootBaseModel):
    table_prefix = "optimization_"

    table: types.Mapped["Table"] = db.relationship(back_populates="_data")
    table__id: types.TableId

    value_0: types.String = db.Column(db.String, nullable=False)
    value_1: types.String = db.Column(db.String, nullable=True)
    value_2: types.String = db.Column(db.String, nullable=True)
    value_3: types.String = db.Column(db.String, nullable=True)
    value_4: types.String = db.Column(db.String, nullable=True)
    value_5: types.String = db.Column(db.String, nullable=True)
    value_6: types.String = db.Column(db.String, nullable=True)
    value_7: types.String = db.Column(db.String, nullable=True)
    value_8: types.String = db.Column(db.String, nullable=True)
    value_9: types.String = db.Column(db.String, nullable=True)
    value_10: types.String = db.Column(db.String, nullable=True)
    value_11: types.String = db.Column(db.String, nullable=True)
    value_12: types.String = db.Column(db.String, nullable=True)
    value_13: types.String = db.Column(db.String, nullable=True)
    value_14: types.String = db.Column(db.String, nullable=True)

    __table_args__ = (
        db.UniqueConstraint(
            "table__id",
            "value_0",
            "value_1",
            "value_2",
            "value_3",
            "value_4",
            "value_5",
            "value_6",
            "value_7",
            "value_8",
            "value_9",
            "value_10",
            "value_11",
            "value_12",
            "value_13",
            "value_14",
        ),
    )
