from typing import TYPE_CHECKING

from ixmp4.data.backend import Backend

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
    IndexSet, Table, Variable, etc.
    """

    equations: EquationServiceFacade
    """Facade instance to manage :class:`~ixmp4.core.optimization.equation.Equation`
    instances for a run."""

    indexsets: IndexSetServiceFacade
    """Facade instance to manage :class:`~ixmp4.core.optimization.indexset.IndexSet`
    instances for a run."""

    parameters: ParameterServiceFacade
    """Facade instance to manage :class:`~ixmp4.core.optimization.parameter.Parameter`
    instances for a run."""

    scalars: ScalarServiceFacade
    """Facade instance to manage :class:`~ixmp4.core.optimization.scalar.Scalar`
    instances for a run."""

    tables: TableServiceFacade
    """Facade instance to manage :class:`~ixmp4.core.optimization.table.Table`
    instances for a run."""

    variables: VariableServiceFacade
    """Facade instance to manage :class:`~ixmp4.core.optimization.variable.Variable`
    instances for a run."""

    def __init__(self, backend: Backend, run: "Run") -> None:
        super().__init__(backend)
        self.equations = EquationServiceFacade(backend, run)
        self.indexsets = IndexSetServiceFacade(backend, run)
        self.parameters = ParameterServiceFacade(backend, run)
        self.scalars = ScalarServiceFacade(backend, run)
        self.tables = TableServiceFacade(backend, run)
        self.variables = VariableServiceFacade(backend, run)

    # TODO Improve performance by writing dedicated queries
    def remove_solution(self) -> None:
        """Remove solution data from all equations and variables on this run.

        .. code:: python

            run.optimization.remove_solution()
            #> None (solution data removed)

        """
        for equation in self.equations.list():
            equation.remove_data()
        for variable in self.variables.list():
            variable.remove_data()

    def has_solution(self) -> bool:
        """Check whether this Run contains a solution.

        .. code:: python

            run.optimization.has_solution()
            #> True

        """
        for variable in self.variables.list():
            if variable.levels:
                return True
        for equation in self.equations.list():
            if equation.levels:
                return True

        # If neither variables nor equations contain data, it doesn't
        return False
