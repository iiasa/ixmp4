from ixmp4.conf.base import PlatformInfo
from ixmp4.data.abstract import (
    CheckpointRepository,
    DataPointRepository,
    EquationRepository,
    IndexSetRepository,
    ModelRepository,
    OptimizationVariableRepository,
    ParameterRepository,
    RegionRepository,
    RunMetaEntryRepository,
    RunRepository,
    ScalarRepository,
    ScenarioRepository,
    TableRepository,
    TimeSeries,
    TimeSeriesRepository,
    UnitRepository,
    VariableRepository,
)


class IamcSubobject(object):
    datapoints: DataPointRepository
    timeseries: TimeSeriesRepository[TimeSeries]
    variables: VariableRepository


class OptimizationSubobject(object):
    equations: EquationRepository
    indexsets: IndexSetRepository
    parameters: ParameterRepository
    scalars: ScalarRepository
    tables: TableRepository
    variables: OptimizationVariableRepository


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
    checkpoints: CheckpointRepository

    def __init__(self, info: PlatformInfo) -> None:
        self.info = info
        self.iamc = IamcSubobject()
        self.optimization = OptimizationSubobject()

    def close(self) -> None:
        """Closes the connection to the database."""
        ...

    def __str__(self) -> str:
        return f"<Backend {self.info}>"
