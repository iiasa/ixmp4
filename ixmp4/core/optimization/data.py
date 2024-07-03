from ixmp4.data.abstract import Run

from ..base import BaseFacade
from .equation import EquationRepository
from .indexset import IndexSetRepository
from .scalar import ScalarRepository
from .table import TableRepository


class OptimizationData(BaseFacade):
    """An optimization data instance, which provides access to optimization data such as
    IndexSet, Table, Variable, etc."""

    equations: EquationRepository
    indexsets: IndexSetRepository
    scalars: ScalarRepository
    tables: TableRepository

    def __init__(self, *args, run: Run, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.equations = EquationRepository(_backend=self.backend, _run=run)
        self.indexsets = IndexSetRepository(_backend=self.backend, _run=run)
        self.scalars = ScalarRepository(_backend=self.backend, _run=run)
        self.tables = TableRepository(_backend=self.backend, _run=run)
