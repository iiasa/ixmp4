from enum import Enum
from typing import ClassVar, Protocol

import pandas as pd
from pydantic import StrictBool, StrictFloat, StrictInt, StrictStr

from ixmp4.data import types

from . import base

# as long as all of these are `Strict` the order does not matter
StrictMetaValue = StrictBool | StrictInt | StrictFloat | StrictStr
MetaValue = bool | int | float | str


class RunMetaEntry(base.BaseModel, Protocol):
    """Run meta entry model."""

    class Type(str, Enum):
        INT = "INT"
        STR = "STR"
        FLOAT = "FLOAT"
        BOOL = "BOOL"

        @classmethod
        def from_pytype(cls, type_):
            return RunMetaEntry._type_map[type_]

    run__id: types.Integer
    "Foreign unique integer id of a run."
    key: types.String
    "Key for the entry. Unique for each `run__id`."
    type: types.String
    "Datatype of the entry's value."
    value: types.Integer | types.Float | types.Integer | types.Boolean
    "Value of the entry."

    _type_map: ClassVar[dict] = {
        int: Type.INT,
        str: Type.STR,
        float: Type.FLOAT,
        bool: Type.BOOL,
    }

    def __str__(self) -> str:
        return f"<RunMetaEntry {self.id} run__id={self.run__id} \
            key={self.key} value={self.value}"


class RunMetaEntryRepository(
    base.Creator,
    base.Retriever,
    base.Deleter,
    base.Enumerator,
    base.BulkUpserter,
    base.BulkDeleter,
    Protocol,
):
    def create(
        self,
        run__id: int,
        key: str,
        value: MetaValue,
    ) -> RunMetaEntry:
        """Creates a meta indicator entry for a run.

        Parameters
        ----------
        run__id : str
            The unique integer id of a run.
        key : str
            The key of the entry.
        value : str, int, bool or float
            The value of the entry.

        Raises
        ------
        :class:`ixmp4.core.exceptions.RunMetaEntryNotUnique`:
            If the entry with `run__id` and `key` is not unique.

        Returns
        -------
        :class:`ixmp4.data.base.RunMetaEntry`:
            The created entry.
        """
        ...

    def get(self, run__id: int, key: str) -> RunMetaEntry:
        """Retrieves a run's meta indicator entry.

        Parameters
        ----------
        run__id : str
            The unique integer id of a run.
        key : str
            The key of the entry.

        Raises
        ------
        :class:`ixmp4.data.abstract.RunMetaEntry.NotFound`:
            If the entry with `run__id` and `key` does not exist.

        Returns
        -------
        :class:`ixmp4.data.base.Run`:
            The retrieved entry.
        """
        ...

    def delete(self, id: int) -> None:
        """Deletes a run's meta indicator entry.
        Warning: No recovery of deleted data shall be possible via ixmp
        after the execution of this function.

        Parameters
        ----------
        id : int
            The unique integer ids of entries to delete.

        Raises
        ------
        :class:`ixmp4.data.abstract.RunMetaEntry.NotFound`:
            If the entry with `id` does not exist.
        """
        ...

    def list(
        self,
        **kwargs,
    ) -> list[RunMetaEntry]:
        r"""Lists run's meta indicator entries by specified criteria.

        Parameters
        ----------
        \*\*kwargs: any
            Filter parameters as specified in
            `ixmp4.data.db.meta.filter.RunMetaEntryFilter`.

        Returns
        -------
        Iterable[:class:`ixmp4.data.abstract.RunMetaEntry`]:
            List of run meta indicator entries.
        """
        ...

    def tabulate(
        self,
        join_run_index: bool = False,
        **kwargs,
    ) -> pd.DataFrame:
        r"""Tabulates run's meta indicator entries by specified criteria.

        Parameters
        ----------
        \*\*kwargs: any
            Filter parameters as specified in
            `ixmp4.data.db.meta.filter.RunMetaEntryFilter`.

        Returns
        -------
        :class:`pandas.DataFrame`:
            A data frame with the columns:
                - id
                - run__id
                - key
                - type
                - value
        """
        ...

    def bulk_upsert(self, df: pd.DataFrame) -> None:
        """Upserts a dataframe of run meta indicator entries.

        Parameters
        ----------
        df : :class:`pandas.DataFrame`
            A data frame with the columns:
                - run__id
                - key
                - value
                - type
        """
        ...

    def bulk_delete(self, df: pd.DataFrame) -> None:
        """Deletes run meta indicator entries as specified per dataframe.
        Warning: No recovery of deleted data shall be possible via ixmp
        after the execution of this function.

        Parameters
        ----------
        df : :class:`pandas.DataFrame`
            A data frame with the columns:
                - run__id
                - key

        """
        ...
