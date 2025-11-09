import logging

from ixmp4.rewrite import data
from ixmp4.rewrite.conf import settings
from ixmp4.rewrite.conf.platforms import PlatformConnectionInfo
from ixmp4.rewrite.transport import DirectTransport, HttpxTransport, Transport

logger = logging.getLogger(__name__)


class IamcSubobject(object):
    datapoints: data.iamc.DataPointService
    timeseries: data.iamc.TimeSeriesService
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

    @classmethod
    def from_connection_info(cls, ci: PlatformConnectionInfo) -> "Backend":
        transport = cls.get_transport(ci)
        return cls(transport)

    @classmethod
    def get_transport(cls, ci: PlatformConnectionInfo) -> Transport:
        if ci.dsn.startswith("http"):
            auth = settings.get_client_auth()
            return HttpxTransport.from_url(ci.dsn, auth)
        else:
            try:
                auth_context = settings.get_local_auth_context()
                return DirectTransport.from_dsn(ci.dsn, auth_context)
            except Exception as e:
                logger.debug("Intiating transport failed with exception: " + str(e))
                if ci.url is not None:
                    logger.debug("Retrying with http transport.")
                    auth = settings.get_client_auth()
                    return HttpxTransport.from_url(ci.url, auth)
