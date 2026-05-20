import logging

from ixmp4.data.checkpoint.service import CheckpointService
from ixmp4.data.iamc.datapoint.service import DataPointService as IamcDataPointService
from ixmp4.data.iamc.model.service import IamcModelService
from ixmp4.data.iamc.region.service import IamcRegionService
from ixmp4.data.iamc.scenario.service import IamcScenarioService
from ixmp4.data.iamc.timeseries.service import (
    TimeSeriesService as IamcTimeSeriesService,
)
from ixmp4.data.iamc.unit.service import IamcUnitService
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
from ixmp4.transport import Transport

logger = logging.getLogger(__name__)


class IamcSubobject(object):
    """Namespace grouping all IAMC-related data services on a
    :class:`Backend`."""

    datapoints: IamcDataPointService
    timeseries: IamcTimeSeriesService
    variables: IamcVariableService
    regions: IamcRegionService
    units: IamcUnitService
    models: IamcModelService
    scenarios: IamcScenarioService


class OptimizationSubobject(object):
    """Namespace grouping all optimization-related data services on a
    :class:`Backend`."""

    equations: OptEquationService
    indexsets: OptIndexSetService
    parameters: OptParameterService
    scalars: OptScalarService
    tables: OptTableService
    variables: OptVariableService


class Backend(object):
    """Central data-layer object that aggregates all service instances.

    A ``Backend`` is built around a single :class:`~ixmp4.transport.Transport`
    and exposes every data service as an attribute.  IAMC-related services
    are grouped under :attr:`iamc` and optimisation-related services under
    :attr:`optimization`.
    """

    transport: Transport
    """The transport used by all services attached to this backend."""

    iamc: IamcSubobject
    """Namespace for IAMC data services (datapoints, timeseries, variables, ...)."""

    optimization: OptimizationSubobject
    """Namespace for optimisation services (equations, indexsets, parameters, ...)."""

    meta: RunMetaEntryService
    models: ModelService
    regions: RegionService
    runs: RunService
    scenarios: ScenarioService
    units: UnitService
    checkpoints: CheckpointService

    def __init__(self, transport: Transport) -> None:
        """Initialise a Backend and instantiate all service classes.

        Parameters
        ----------
        transport : Transport
            The transport used by every service.
        """
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
