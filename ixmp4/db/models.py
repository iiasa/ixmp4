"""All database models collected in one module.
TODO: Think about explicit imports of version, docs and association models."""

from sqlalchemy import MetaData

from ixmp4.data.base.db import BaseModel as BaseModel
from ixmp4.data.checkpoint.db import Checkpoint as Checkpoint
from ixmp4.data.iamc.datapoint.db import DataPoint as DataPoint
from ixmp4.data.iamc.measurand.db import Measurand as Measurand
from ixmp4.data.iamc.timeseries.db import TimeSeries as TimeSeries
from ixmp4.data.iamc.variable.db import Variable as IamcVariable
from ixmp4.data.meta.db import RunMetaEntry as RunMetaEntry
from ixmp4.data.model.db import Model as Model
from ixmp4.data.optimization.equation.db import Equation as Equation
from ixmp4.data.optimization.indexset.db import IndexSet as IndexSet
from ixmp4.data.optimization.parameter.db import Parameter as Parameter
from ixmp4.data.optimization.scalar.db import Scalar as Scalar
from ixmp4.data.optimization.table.db import Table as Table
from ixmp4.data.optimization.variable.db import Variable as OptVariable
from ixmp4.data.region.db import Region as Region
from ixmp4.data.run.db import Run as Run
from ixmp4.data.scenario.db import Scenario as Scenario
from ixmp4.data.unit.db import Unit as Unit

all_basic_models = [
    Checkpoint,
    DataPoint,
    Measurand,
    TimeSeries,
    IamcVariable,
    RunMetaEntry,
    Model,
    Equation,
    IndexSet,
    Parameter,
    Scalar,
    Table,
    OptVariable,
    Region,
    Run,
    Model,
    Scenario,
    Unit,
]


def get_metadata() -> MetaData:
    return BaseModel.metadata
