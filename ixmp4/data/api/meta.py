from collections.abc import Iterable

# TODO Use `type` instead of TypeAlias when dropping Python 3.11
from typing import ClassVar, TypeAlias, cast

import pandas as pd

# TODO Import this from typing when dropping Python 3.11
from typing_extensions import Unpack

from ixmp4.data import abstract

from . import base


class RunMetaEntry(base.BaseModel):
    NotFound: ClassVar = abstract.RunMetaEntry.NotFound
    NotUnique: ClassVar = abstract.RunMetaEntry.NotUnique
    DeletionPrevented: ClassVar = abstract.RunMetaEntry.DeletionPrevented

    id: int
    run__id: int
    key: str
    dtype: str
    value: abstract.StrictMetaValue
    value_int: int | None
    value_str: str | None
    value_float: float | None
    value_bool: bool | None


# TODO This is tantalizingly close to the run JsonType, but not quite there.
JsonType: TypeAlias = dict[
    str,
    bool
    | float
    | Iterable[float]
    | abstract.annotations.DefaultFilterAlias
    | dict[
        str,
        bool
        | abstract.annotations.IntFilterAlias
        | dict[str, abstract.annotations.DefaultFilterAlias],
    ]
    | None,
]


class RunMetaEntryRepository(
    base.Creator[RunMetaEntry],
    base.Retriever[RunMetaEntry],
    base.Deleter[RunMetaEntry],
    base.Enumerator[RunMetaEntry],
    base.BulkUpserter[RunMetaEntry],
    base.BulkDeleter[RunMetaEntry],
    base.VersionManager[RunMetaEntry],
    abstract.RunMetaEntryRepository,
):
    model_class = RunMetaEntry
    prefix = "meta/"

    def create(self, run__id: int, key: str, value: abstract.MetaValue) -> RunMetaEntry:
        return super().create(run__id=run__id, key=key, value=value)

    def get(self, run__id: int, key: str) -> RunMetaEntry:
        return super().get(run_id=run__id, key=key)

    def delete(self, id: int) -> None:
        super().delete(id)

    def enumerate(
        self, **kwargs: Unpack[abstract.meta.EnumerateKwargs]
    ) -> list[RunMetaEntry] | pd.DataFrame:
        return super().enumerate(**kwargs)

    def list(
        self,
        join_run_index: bool | None = None,
        **kwargs: Unpack[abstract.meta.EnumerateKwargs],
    ) -> list[RunMetaEntry]:
        # base functions require dict, but TypedDict just inherits from Mapping
        json = cast(JsonType, kwargs)
        return super()._list(json=json, params={"join_run_index": join_run_index})

    def tabulate(
        self,
        join_run_index: bool | None = None,
        **kwargs: Unpack[abstract.meta.EnumerateKwargs],
    ) -> pd.DataFrame:
        json = cast(JsonType, kwargs)
        return super()._tabulate(json=json, params={"join_run_index": join_run_index})

    def bulk_upsert(self, df: pd.DataFrame) -> None:
        super().bulk_upsert(df)

    def bulk_delete(self, df: pd.DataFrame) -> None:
        super().bulk_delete(df)
