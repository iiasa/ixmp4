from ixmp4.data.abstract import Run
from ixmp4.data.backend import Backend

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

    def __init__(self, run: Run, **kwargs: Backend) -> None:
        super().__init__(**kwargs)
        self.equations = EquationRepository(_backend=self.backend, _run=run)
        self.indexsets = IndexSetRepository(_backend=self.backend, _run=run)
        self.parameters = ParameterRepository(_backend=self.backend, _run=run)
        self.scalars = ScalarRepository(_backend=self.backend, _run=run)
        self.tables = TableRepository(_backend=self.backend, _run=run)
        self.variables = VariableRepository(_backend=self.backend, _run=run)

    def remove_solution(self) -> None:
        for equation in self.equations.list():
            equation.remove_data()
        for variable in self.variables.list():
            variable.remove_data()

    def has_solution(self) -> bool:
        """Check whether this Run contains a solution."""
        for variable in self.variables.list():
            if variable.levels:
                return True
        for equation in self.equations.list():
            if equation.levels:
                return True

        # If neither variables nor equations contain data, it doesn't
        return False
