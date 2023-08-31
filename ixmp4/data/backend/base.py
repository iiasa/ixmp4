from ixmp4.conf.base import PlatformInfo
from ixmp4.data.abstract import (
    DataPointRepository,
    IndexSetRepository,
    ModelRepository,
    RegionRepository,
    RunMetaEntryRepository,
    RunRepository,
    ScenarioRepository,
    TimeSeriesRepository,
    UnitRepository,
    VariableRepository,
)


class IamcSubobject(object):
    datapoints: DataPointRepository
    timeseries: TimeSeriesRepository
    variables: VariableRepository


class OptimizationSubobject(object):
    indexsets: IndexSetRepository


class Backend(object):
    iamc: IamcSubobject
    info: PlatformInfo
    meta: RunMetaEntryRepository
    models: ModelRepository
    optimization: OptimizationSubobject
    regions: RegionRepository
    runs: RunRepository
    scenarios: ScenarioRepository
    units: UnitRepository

    def __init__(self, info: PlatformInfo) -> None:
        self.info = info
        self.iamc = IamcSubobject()
        self.optimization = OptimizationSubobject()
