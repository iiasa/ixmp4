from collections.abc import Iterable
from typing import TYPE_CHECKING, Generic, Protocol, TypeVar

if TYPE_CHECKING:
    from . import EnumerateKwargs

import pandas as pd

# TODO Import this from typing when dropping Python 3.11
from typing_extensions import TypedDict, Unpack

from ixmp4.data.abstract.unit import Unit

from .. import base
from ..docs import DocsRepository

BackendModelType = TypeVar("BackendModelType", bound=base.BaseModel, covariant=True)


class CreateKwargs(TypedDict, total=False):
    value: float
    unit: str | Unit | None
    constrained_to_indexsets: str | list[str] | None
    column_names: list[str] | None


class BackendBaseRepository(
    Generic[BackendModelType],
    base.Creator,
    base.Deleter,
    base.Retriever,
    base.Enumerator,
    Protocol,
):
    docs: DocsRepository

    def create(
        self, run_id: int, name: str, **kwargs: Unpack["CreateKwargs"]
    ) -> BackendModelType: ...

    def delete(self, id: int) -> None: ...

    def get(self, run_id: int, name: str) -> BackendModelType: ...

    def list(
        self, **kwargs: Unpack["EnumerateKwargs"]
    ) -> Iterable[BackendModelType]: ...

    def tabulate(self, **kwargs: Unpack["EnumerateKwargs"]) -> pd.DataFrame: ...
