from ixmp4.conf import PlatformInfo
from ixmp4.data.abstract import (
    DataPointRepository,
    RunRepository,
    RunMetaEntryRepository,
    TimeSeriesRepository,
    UnitRepository,
    RegionRepository,
    ScenarioRepository,
    ModelRepository,
    VariableRepository,
)


class IamcSubobject(object):
    datapoints: DataPointRepository
    timeseries: TimeSeriesRepository
    variables: VariableRepository


class Backend(object):
    info: PlatformInfo
    runs: RunRepository
    meta: RunMetaEntryRepository
    regions: RegionRepository
    units: UnitRepository
    scenarios: ScenarioRepository
    models: ModelRepository
    iamc: IamcSubobject

    def __init__(self, info: PlatformInfo) -> None:
        self.info = info
        self.iamc = IamcSubobject()
