# flake8: noqa
from .iamc.variable import Variable as Variable
from .model import Model as Model
from .optimization.equation import Equation as Equation
from .optimization.indexset import IndexSet as IndexSet
from .optimization.scalar import Scalar as Scalar
from .optimization.table import Table as Table
from .optimization.parameter import Parameter as Parameter

# TODO Is this really the name we want to use?
from .optimization.variable import Variable as OptimizationVariable
from .platform import Platform as Platform
from .region import Region as Region
from .run import Run as Run
from .scenario import Scenario as Scenario
from .unit import Unit as Unit
