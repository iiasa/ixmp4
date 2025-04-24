from typing import Any, ClassVar, cast

import pandas as pd

# TODO Import this from typing when dropping support for Python 3.11
from typing_extensions import Unpack

from ixmp4.data import abstract

from . import base


class Checkpoint(base.BaseModel):
    NotFound: ClassVar = abstract.Unit.NotFound
    NotUnique: ClassVar = abstract.Unit.NotUnique
    DeletionPrevented: ClassVar = abstract.Unit.DeletionPrevented

    id: int
    run__id: int
    transaction__id: int
    message: str


class CheckpointRepository(
    base.Creator[Checkpoint],
    base.Deleter[Checkpoint],
    base.Enumerator[Checkpoint],
    abstract.CheckpointRepository,
):
    model_class = Checkpoint
    prefix = "checkpoints/"

    def create(self, run__id: int, message: str) -> Checkpoint:
        return super().create(run__id=run__id, message=message)

    def delete(self, id: int) -> None:
        super().delete(id)

    def get_by_id(self, id: int) -> Checkpoint:
        res = self._get_by_id(id)
        return Checkpoint(**res)

    def enumerate(
        self, **kwargs: Unpack[abstract.checkpoint.EnumerateKwargs]
    ) -> list[Checkpoint] | pd.DataFrame:
        return super().enumerate(**kwargs)

    def list(
        self, **kwargs: Unpack[abstract.checkpoint.EnumerateKwargs]
    ) -> list[Checkpoint]:
        json = cast(dict[str, Any], kwargs)
        return super()._list(json=json)

    def tabulate(
        self, **kwargs: Unpack[abstract.checkpoint.EnumerateKwargs]
    ) -> pd.DataFrame:
        json = cast(dict[str, Any], kwargs)
        return super()._tabulate(json=json)
