# flake8: noqa
import importlib.metadata

from ixmp4.core import Model as Model
from ixmp4.core import Platform as Platform
from ixmp4.core import Region as Region
from ixmp4.core import Run as Run
from ixmp4.core import Scenario as Scenario
from ixmp4.core import Unit as Unit
from ixmp4.core import Variable as Variable
from ixmp4.core.exceptions import InconsistentIamcType as InconsistentIamcType
from ixmp4.core.exceptions import IxmpError as IxmpError
from ixmp4.core.exceptions import NotFound as NotFound
from ixmp4.core.exceptions import NotUnique as NotUnique
from ixmp4.data.abstract import DataPoint as DataPoint

__version__ = importlib.metadata.version("ixmp4")
