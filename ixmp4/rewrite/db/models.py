"""All database models collected in one module TODO"""

from sqlalchemy import MetaData

from ixmp4.rewrite.data.base.db import BaseModel as BaseModel
from ixmp4.rewrite.data.checkpoint.db import Checkpoint as Checkpoint
from ixmp4.rewrite.data.iamc.datapoint.db import DataPoint as DataPoint
from ixmp4.rewrite.data.iamc.measurand.db import Measurand as Measurand
from ixmp4.rewrite.data.iamc.timeseries.db import TimeSeries as TimeSeries
from ixmp4.rewrite.data.iamc.variable.db import Variable as Variable
from ixmp4.rewrite.data.meta.db import RunMetaEntry as RunMetaEntry
from ixmp4.rewrite.data.model.db import Model as Model
from ixmp4.rewrite.data.region.db import Region as Region
from ixmp4.rewrite.data.run.db import Run as Run
from ixmp4.rewrite.data.scenario.db import Scenario as Scenario
from ixmp4.rewrite.data.unit.db import Unit as Unit


def get_metadata() -> MetaData:
    return BaseModel.metadata
