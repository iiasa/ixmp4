from typing import Any, ClassVar, TypedDict

import pandas as pd

# TODO Import this from typing when dropping Python 3.11
from typing_extensions import Unpack

from ixmp4.core.exceptions import IxmpError
from ixmp4.data import types
from ixmp4.data.abstract.annotations import HasIdFilter, HasNameFilter, HasRunIdFilter
from ixmp4.data.db.versions.repository import VersionRepository
from ixmp4.db.filters import BaseFilter

from .. import mixins
from ..base import BaseModel as RootBaseModel
from ..base import (
    BulkDeleter,
    BulkUpserter,
    Creator,
    Deleter,
    Enumerator,
    Lister,
    ModelType,
    Retriever,
    Selecter,
    Tabulator,
)


class BaseModel(RootBaseModel, mixins.HasCreationInfo):
    # NOTE: only subclasses storing data actually define this!
    DataInvalid: ClassVar[type[IxmpError]]

    __abstract__ = True

    name: types.Name


class EnumerateKwargs(HasIdFilter, HasNameFilter, HasRunIdFilter, total=False):
    _filter: BaseFilter


class RevertKwargs(TypedDict, total=False):
    run__id: int


class Reverter(BulkDeleter[ModelType], BulkUpserter[ModelType]):
    versions: VersionRepository[Any]

    # NOTE These are stored as a baseline
    _columns_to_drop_for_update = {
        "transaction_id",
        "end_transaction_id",
        "operation_type",
    }
    _columns_to_drop_for_insert = _columns_to_drop_for_update | {"id"}

    def revert(
        self,
        transaction__id: int,
        correct_versions: pd.DataFrame | None = None,
        **kwargs: Unpack[RevertKwargs],
    ) -> None:
        current_versions = self.tabulate(**kwargs)
        if correct_versions is None:
            correct_versions = self.versions.tabulate(
                transaction__id=transaction__id, **kwargs
            )

        # TODO Add a check to exit early if current == correct

        versions_to_delete = current_versions[
            ~current_versions["id"].isin(correct_versions["id"])
        ]

        if not versions_to_delete.empty:
            self.bulk_delete(versions_to_delete)

        # NOTE The following manual data split could be avoided if `tabulate_existing`
        # returned the correct values
        # This would allow us to simply `bulk_upsert` the data, though that queries the
        # DB again (for `tabulate_existing`, the data of which we already have here)

        versions_to_insert = correct_versions[
            ~correct_versions["id"].isin(current_versions["id"])
        ].drop(columns=self._columns_to_drop_for_insert)

        if not versions_to_insert.empty:
            with self.backend.event_handler.pause():
                self.bulk_insert(versions_to_insert)

        # Limit updates to those differing from their existing versions
        ids_to_update = set(
            self.versions.tabulate(
                transaction__id=transaction__id, valid="after_transaction", **kwargs
            )["id"]
        )
        correct_versions = correct_versions[correct_versions["id"].isin(ids_to_update)]

        versions_to_update = correct_versions[
            correct_versions["id"].isin(current_versions["id"])
        ].drop(columns=self._columns_to_drop_for_update)

        if not versions_to_update.empty:
            self.bulk_update(versions_to_update)

        self.session.commit()
