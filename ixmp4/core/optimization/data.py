from functools import partial

from ixmp4.data.abstract import Run

from ..base import BaseFacade
from .indexset import IndexSet as IndexSetModel
from .indexset import IndexSetRepository


class OptimizationData(BaseFacade):
    """An optimization data instance, which provides access to optimization data such as
    IndexSet, Table, Variable, etc."""

    IndexSet: partial[IndexSetModel]

    indexsets: IndexSetRepository

    def __init__(self, *args, run: Run, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.IndexSet = partial(IndexSetModel, _backend=self.backend, _run=run)
        self.indexsets = IndexSetRepository(_backend=self.backend, _run=run)
