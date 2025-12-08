import abc
import logging
from typing import ClassVar, Collection, Generic, TypeVar

import pandas as pd
import sqlalchemy as sa
from toolkit import db

from ixmp4.base_exceptions import (
    OptimizationDataValidationError,
    OptimizationItemUsageError,
)
from ixmp4.data.optimization.indexset.db import IndexSet

from .db import IndexedModel, IndexsetAssociationModel

logger = logging.getLogger(__name__)
AssocT = TypeVar("AssocT", bound=IndexsetAssociationModel)


class IndexedRepository(
    abc.ABC, db.r.ItemRepository[IndexedModel[AssocT]], Generic[AssocT]
):
    executor: db.r.SessionExecutor
    target: db.r.ModelTarget[IndexedModel[AssocT]]
    association_target: db.r.ModelTarget[AssocT]
    idxset_target = db.r.ModelTarget(IndexSet)
    DataInvalid: ClassVar[type[OptimizationDataValidationError]]
    extra_data_columns: Collection[str] = {}
    "Extra and required columns for the item's data property."

    def add_data(self, id: int, data: pd.DataFrame) -> None:
        indexed_item = self.get_by_pk({"id": id})
        index_list = indexed_item.column_names or indexed_item.indexset_names
        existing_data = pd.DataFrame(indexed_item.data)

        if index_list:
            data = data.set_index(index_list)
            if not existing_data.empty:
                existing_data.set_index(index_list, inplace=True)

        data = data.combine_first(existing_data)
        if index_list:
            data = data.reset_index()

        self.validate_data(
            indexed_item, data, indexed_item.indexsets, indexed_item.column_names
        )
        self.update_by_pk({"id": id, "data": data.to_dict(orient="list")})

    def remove_data(self, id: int, data: pd.DataFrame) -> None:
        indexed_item = self.get_by_pk({"id": id})
        index_list = indexed_item.column_names or indexed_item.indexset_names
        if not index_list:
            logger.warning(
                f"Trying to remove {data.to_dict(orient='list')} from "
                f"`{indexed_item.__class__.__name__}` '{indexed_item.name}', "
                "but that is not indexed; not removing anything!"
            )
            return  # can't remove specific data from unindexed variable

        existing_data = pd.DataFrame(indexed_item.data)
        if not existing_data.empty:
            existing_data.set_index(index_list, inplace=True)

        # This is the only kind of validation we do for removal data
        try:
            data = data.set_index(index_list)
        except KeyError as e:
            logger.error(
                f"Data to be removed from {str(indexed_item)} "
                f"must include {index_list} as keys/columns, "
                f"but {data.columns.to_list()} were provided."
            )
            raise OptimizationItemUsageError(
                "The data to be removed must specify one or more complete indices "
                "to remove associated levels and marginals!"
            ) from e

        remaining_data = existing_data[~existing_data.index.isin(data.index)]
        if not remaining_data.index.empty:
            remaining_data.reset_index(inplace=True)

        if remaining_data.empty:
            self.update_by_pk({"id": id, "data": {}})
        else:
            self.update_by_pk({"id": id, "data": remaining_data.to_dict(orient="list")})

    def get_linked_ids(self, id: int) -> list[int]:
        exc = sa.select(self.association_target.model_class.get_item_id_column()).where(
            self.association_target.model_class.indexset__id == id
        )

        with self.executor.select(exc) as result:
            return list(result.scalars().all())

    def get_indexset(self, id: int) -> IndexSet:
        exc = self.idxset_target.select_statement()
        exc = exc.where(IndexSet.id == id)

        with self.executor.select(exc) as result, self.expect_one_result():
            return self.idxset_target.get_single_item(result)

    def remove_invalid_linked_data(
        self,
        indexset: IndexSet,
        data: list[str],
    ) -> None:
        # get items
        exc = self.target.select_statement().where(
            self.target.model_class.indexsets.any(
                self.idxset_target.model_class.id == indexset.id
            )
        )
        with self.executor.select(exc) as result:
            items: list[IndexedModel[AssocT]] = list(result.scalars().all())

        for item in items:
            # Convert existing data for manipulation
            df = pd.DataFrame(item.data)

            if df.empty:
                continue  # nothing to do

            # Identify column/dimension names to target; item and name must be linked
            columns = self.get_constrained_data_columns(item, indexset.name)

            # Prepare data template to exclude
            invalid_data = {columns[i]: data for i in range(len(columns))}

            # Prepare stored data to remove
            # NOTE if any linked column is not of type str, this may be incorrect
            remove_data = df[df[columns].isin(invalid_data).any(axis=1)]

            self.remove_data(item.id, remove_data)

    def get_constrained_data_columns(
        self, item: IndexedModel[AssocT], name: str
    ) -> list[str]:
        if not item.column_names:
            # The item's indexset_names must be a unique list of names
            return [name]
        else:
            # If we have column_names, we must also have indexsets
            assert item.indexset_names

            # Handle possible duplicate values
            return [
                item.column_names[i]
                for i in range(len(item.column_names))
                if item.indexset_names[i] == name
            ]

    def validate_data(
        self,
        item: IndexedModel[AssocT],
        data: pd.DataFrame,
        indexsets: list["IndexSet"],
        column_names: list[str] | None = None,
    ) -> None:
        # Can't validate ("values","units") or ("levels", "marginals") when they are present
        number_columns = len(data.columns) - len(self.extra_data_columns)
        columns = (
            column_names if column_names else [indexset.name for indexset in indexsets]
        )

        # TODO for all of the following, we might want to create unique exceptions
        # Could me make both more specific by specifiying missing/extra columns?
        if number_columns < len(indexsets):
            raise self.DataInvalid(
                f"While handling {str(item)}: \n"
                f"Data is missing for some columns! \n Data: {data} \n "
                f"Columns: {columns}"
            )
        elif number_columns > len(indexsets):
            raise self.DataInvalid(
                f"While handling {str(item)}: \n"
                f"Trying to add data to unknown columns! \n Data: {data} \n "
                f"Columns: {columns}"
            )

        # We could make this more specific maybe by pointing to the missing values
        if data.isna().any(axis=None):
            raise self.DataInvalid(
                f"While handling {str(item)}: \n"
                "The data is missing values, please make sure it "
                "does not contain None or NaN, either!"
            )

        limited_to_indexsets = {
            columns[i]: indexsets[i].data for i in range(len(indexsets))
        }

        # We can make this more specific e.g. highlighting all duplicate rows via
        # pd.DataFrame.duplicated(keep="False")
        if data[limited_to_indexsets.keys()].value_counts().max() > 1:
            raise self.DataInvalid(
                f"While handling {str(item)}: \nThe data contains duplicate rows!"
            )

        # Can we make this more specific? Iterating over columns; if any is False,
        # return its name or something?
        if (
            not data[limited_to_indexsets.keys()]
            .isin(limited_to_indexsets)
            .all(axis=None)
        ):
            raise self.DataInvalid(
                f"While handling {str(item)}: \n"
                "The data contains values that are not allowed as per the IndexSets "
                "it is constrained to!"
            )

    def delete_associations(self, id: int) -> None:
        raise NotImplementedError
