import logging

from ixmp4.rewrite import data, services

logger = logging.getLogger(__name__)


class IamcSubobject(object):
    datapoints: data.DataPointService
    timeseries: data.TimeSeriesService
    # variables: VariableRepository


class OptimizationSubobject(object):
    # equations: EquationRepository
    # indexsets: IndexSetRepository
    # parameters: ParameterRepository
    # scalars: ScalarRepository
    # tables: TableRepository
    # variables: OptimizationVariableRepository
    pass


class Backend(object):
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

    def __init__(self, transport: services.Transport) -> None:
        logger.info(f"Creating backend class with transport: {transport}")
        self.optimization = OptimizationSubobject()
        self.iamc = IamcSubobject()

        self.meta = data.RunMetaEntryService(transport)
        self.models = data.ModelService(transport)
        self.regions = data.RegionService(transport)
        self.runs = data.RunService(transport)
        self.scenarios = data.ScenarioService(transport)
        self.units = data.UnitService(transport)
        self.checkpoints = data.CheckpointService(transport)
        self.iamc.datapoints = data.DataPointService(transport)
        self.iamc.timeseries = data.TimeSeriesService(transport)
