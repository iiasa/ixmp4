from typing import ClassVar, Iterable

import pandas as pd

from ixmp4.data import abstract

from . import base


class RunMetaEntry(base.BaseModel):
    NotFound: ClassVar = abstract.RunMetaEntry.NotFound
    NotUnique: ClassVar = abstract.RunMetaEntry.NotUnique
    DeletionPrevented: ClassVar = abstract.RunMetaEntry.DeletionPrevented

    id: int
    run__id: int
    key: str
    type: str
    value: abstract.StrictMetaValue


class RunMetaEntryRepository(
    base.Creator[RunMetaEntry],
    base.Retriever[RunMetaEntry],
    base.Deleter[RunMetaEntry],
    base.Enumerator[RunMetaEntry],
    base.BulkUpserter[RunMetaEntry],
    base.BulkDeleter[RunMetaEntry],
    abstract.RunMetaEntryRepository,
):
    model_class = RunMetaEntry
    prefix = "meta/"

    def create(
        self,
        run__id: int,
        key: str,
        value: abstract.MetaValue,
    ) -> RunMetaEntry:
        return super().create(run__id=run__id, key=key, value=value)

    def get(self, run__id: int, key: str) -> RunMetaEntry:
        return super().get(run__ids=[run__id], keys=[key])

    def delete(self, id: int) -> None:
        super().delete(id)

    def list(self, *args, **kwargs) -> Iterable[RunMetaEntry]:
        return super().list(*args, **kwargs)

    def tabulate(self, *args, **kwargs) -> pd.DataFrame:
        return super().tabulate(*args, **kwargs)

    def enumerate(self, *args, **kwargs) -> Iterable[RunMetaEntry] | pd.DataFrame:
        return super().enumerate(*args, **kwargs)

    def bulk_upsert(self, df: pd.DataFrame) -> None:
        super().bulk_upsert(df)

    def bulk_delete(self, df: pd.DataFrame) -> None:
        super().bulk_delete(df)
