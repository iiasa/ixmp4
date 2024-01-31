from typing import ClassVar

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
        return super().get(run_id=run__id, key=key)

    def delete(self, id: int) -> None:
        super().delete(id)

    def enumerate(self, **kwargs) -> list[RunMetaEntry] | pd.DataFrame:
        return super().enumerate(**kwargs)

    def list(self, join_run_index: bool | None = None, **kwargs) -> list[RunMetaEntry]:
        return super()._list(json=kwargs, params={"join_run_index": join_run_index})

    def tabulate(self, join_run_index: bool | None = None, **kwargs) -> pd.DataFrame:
        return super()._tabulate(json=kwargs, params={"join_run_index": join_run_index})

    def bulk_upsert(self, df: pd.DataFrame) -> None:
        super().bulk_upsert(df)

    def bulk_delete(self, df: pd.DataFrame) -> None:
        super().bulk_delete(df)
