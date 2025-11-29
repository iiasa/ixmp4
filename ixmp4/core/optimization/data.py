from typing import TYPE_CHECKING

from ixmp4.backend import Backend

from ..base import BaseBackendFacade
from .equation import EquationServiceFacade
from .indexset import IndexSetServiceFacade
from .parameter import ParameterServiceFacade
from .scalar import ScalarServiceFacade
from .table import TableServiceFacade
from .variable import VariableServiceFacade

if TYPE_CHECKING:
    from ixmp4.core.run import Run


class RunOptimizationData(BaseBackendFacade):
    """An optimization data instance, which provides access to optimization data such as
    IndexSet, Table, Variable, etc."""

    equations: EquationServiceFacade
    indexsets: IndexSetServiceFacade
    parameters: ParameterServiceFacade
    scalars: ScalarServiceFacade
    tables: TableServiceFacade
    variables: VariableServiceFacade

    def __init__(self, backend: Backend, run: "Run") -> None:
        super().__init__(backend)
        self.equations = EquationServiceFacade(backend.optimization.equations, run)
        self.indexsets = IndexSetServiceFacade(backend.optimization.indexsets, run)
        self.parameters = ParameterServiceFacade(backend.optimization.parameters, run)
        self.scalars = ScalarServiceFacade(
            backend.optimization.scalars, backend.units, run
        )
        self.tables = TableServiceFacade(backend.optimization.tables, run)
        self.variables = VariableServiceFacade(backend.optimization.variables, run)

    # TODO Improve performance by writing dedicated queries
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
