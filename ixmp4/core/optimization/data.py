from ixmp4.data.abstract import Run

from ..base import BaseFacade
from .equation import EquationRepository
from .indexset import IndexSetRepository
from .parameter import ParameterRepository
from .scalar import ScalarRepository
from .table import TableRepository
from .variable import VariableRepository


class OptimizationData(BaseFacade):
    """An optimization data instance, which provides access to optimization data such as
    IndexSet, Table, Variable, etc."""

    equations: EquationRepository
    indexsets: IndexSetRepository
    parameters: ParameterRepository
    scalars: ScalarRepository
    tables: TableRepository
    variables: VariableRepository

    def __init__(self, *args, run: Run, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.equations = EquationRepository(_backend=self.backend, _run=run)
        self.indexsets = IndexSetRepository(_backend=self.backend, _run=run)
        self.parameters = ParameterRepository(_backend=self.backend, _run=run)
        self.scalars = ScalarRepository(_backend=self.backend, _run=run)
        self.tables = TableRepository(_backend=self.backend, _run=run)
        self.variables = VariableRepository(_backend=self.backend, _run=run)
