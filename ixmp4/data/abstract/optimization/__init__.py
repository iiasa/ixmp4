from collections.abc import Iterable

from ..annotations import HasIdFilter, HasNameFilter, HasRunIdFilter
from .base import BackendBaseRepository
from .equation import Equation, EquationRepository
from .indexset import IndexSet, IndexSetRepository
from .parameter import Parameter, ParameterRepository
from .scalar import Scalar, ScalarRepository
from .table import Table, TableRepository
from .variable import Variable, VariableRepository


class EnumerateKwargs(HasIdFilter, HasNameFilter, HasRunIdFilter, total=False): ...
