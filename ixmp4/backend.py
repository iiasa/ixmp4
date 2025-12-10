import logging

from ixmp4 import data
from ixmp4.transport import (
    Transport,
)

logger = logging.getLogger(__name__)


class IamcSubobject(object):
    datapoints: data.iamc.DataPointService
    timeseries: data.iamc.TimeSeriesService
    variables: data.iamc.VariableService


class OptimizationSubobject(object):
    equations: data.optimization.EquationService
    indexsets: data.optimization.IndexSetService
    parameters: data.optimization.ParameterService
    scalars: data.optimization.ScalarService
    tables: data.optimization.TableService
    variables: data.optimization.VariableService


class Backend(object):
    transport: Transport

    iamc: IamcSubobject
    optimization: OptimizationSubobject

    # info: PlatformInfo
    meta: data.RunMetaEntryService
    models: data.ModelService
    regions: data.RegionService
    runs: data.RunService
    scenarios: data.ScenarioService
    units: data.UnitService
    checkpoints: data.CheckpointService

    def __init__(self, transport: Transport) -> None:
        logger.info(f"Creating backend class with transport: {transport}")
        self.transport = transport
        self.optimization = OptimizationSubobject()
        self.iamc = IamcSubobject()

        self.meta = data.RunMetaEntryService(transport)
        self.models = data.ModelService(transport)
        self.regions = data.RegionService(transport)
        self.runs = data.RunService(transport)
        self.scenarios = data.ScenarioService(transport)
        self.units = data.UnitService(transport)
        self.checkpoints = data.CheckpointService(transport)
        self.iamc.datapoints = data.iamc.DataPointService(transport)
        self.iamc.timeseries = data.iamc.TimeSeriesService(transport)
        self.iamc.variables = data.iamc.VariableService(transport)
        self.optimization.equations = data.optimization.EquationService(transport)
        self.optimization.indexsets = data.optimization.IndexSetService(transport)
        self.optimization.parameters = data.optimization.ParameterService(transport)
        self.optimization.scalars = data.optimization.ScalarService(transport)
        self.optimization.tables = data.optimization.TableService(transport)
        self.optimization.variables = data.optimization.VariableService(transport)
