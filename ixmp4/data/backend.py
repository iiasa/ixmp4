import logging

from ixmp4.data.checkpoint.service import CheckpointService
from ixmp4.data.iamc.datapoint.service import DataPointService as IamcDataPointService
from ixmp4.data.iamc.timeseries.service import (
    TimeSeriesService as IamcTimeSeriesService,
)
from ixmp4.data.iamc.variable.service import VariableService as IamcVariableService
from ixmp4.data.meta.service import RunMetaEntryService
from ixmp4.data.model.service import ModelService
from ixmp4.data.optimization.equation.service import (
    EquationService as OptEquationService,
)
from ixmp4.data.optimization.indexset.service import (
    IndexSetService as OptIndexSetService,
)
from ixmp4.data.optimization.parameter.service import (
    ParameterService as OptParameterService,
)
from ixmp4.data.optimization.scalar.service import ScalarService as OptScalarService
from ixmp4.data.optimization.table.service import TableService as OptTableService
from ixmp4.data.optimization.variable.service import (
    VariableService as OptVariableService,
)
from ixmp4.data.region.service import RegionService
from ixmp4.data.run.service import RunService
from ixmp4.data.scenario.service import ScenarioService
from ixmp4.data.unit.service import UnitService
from ixmp4.transport import (
    Transport,
)

logger = logging.getLogger(__name__)


class IamcSubobject(object):
    datapoints: IamcDataPointService
    timeseries: IamcTimeSeriesService
    variables: IamcVariableService


class OptimizationSubobject(object):
    equations: OptEquationService
    indexsets: OptIndexSetService
    parameters: OptParameterService
    scalars: OptScalarService
    tables: OptTableService
    variables: OptVariableService


class Backend(object):
    transport: Transport

    iamc: IamcSubobject
    optimization: OptimizationSubobject

    meta: RunMetaEntryService
    models: ModelService
    regions: RegionService
    runs: RunService
    scenarios: ScenarioService
    units: UnitService
    checkpoints: CheckpointService

    def __init__(self, transport: Transport) -> None:
        logger.info(f"Creating backend class with transport: {transport}")
        self.transport = transport
        self.optimization = OptimizationSubobject()
        self.iamc = IamcSubobject()

        self.meta = RunMetaEntryService(transport)
        self.models = ModelService(transport)
        self.regions = RegionService(transport)
        self.runs = RunService(transport)
        self.scenarios = ScenarioService(transport)
        self.units = UnitService(transport)
        self.checkpoints = CheckpointService(transport)
        self.iamc.datapoints = IamcDataPointService(transport)
        self.iamc.timeseries = IamcTimeSeriesService(transport)
        self.iamc.variables = IamcVariableService(transport)
        self.optimization.equations = OptEquationService(transport)
        self.optimization.indexsets = OptIndexSetService(transport)
        self.optimization.parameters = OptParameterService(transport)
        self.optimization.scalars = OptScalarService(transport)
        self.optimization.tables = OptTableService(transport)
        self.optimization.variables = OptVariableService(transport)
