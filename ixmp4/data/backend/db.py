import logging
from collections.abc import Generator
from contextlib import contextmanager
from functools import lru_cache

from sqlalchemy.engine import Engine, create_engine
from sqlalchemy.orm import configure_mappers
from sqlalchemy.orm.session import sessionmaker
from sqlalchemy.pool import NullPool, StaticPool

# TODO Import this from typing when dropping support for Python 3.11
from typing_extensions import Unpack

from ixmp4.conf.base import PlatformInfo
from ixmp4.conf.manager import ManagerConfig, ManagerPlatformInfo
from ixmp4.conf.user import User
from ixmp4.core.exceptions import ProgrammingError
from ixmp4.data.db import (
    BaseModel,
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
    TimeSeriesRepository,
    UnitRepository,
    VariableRepository,
)
from ixmp4.data.db.events import SqlaEventHandler

from ..auth.context import AuthorizationContext
from .base import (
    Backend,
)
from .base import IamcSubobject as BaseIamcSubobject
from .base import OptimizationSubobject as BaseOptimizationSubobject

logger = logging.getLogger(__name__)


@lru_cache()
def cached_create_engine(dsn: str) -> Engine:
    return create_engine(dsn, poolclass=NullPool, max_identifier_length=63)


class IamcSubobject(BaseIamcSubobject):
    datapoints: DataPointRepository
    timeseries: TimeSeriesRepository
    variables: VariableRepository


class OptimizationSubobject(BaseOptimizationSubobject):
    equations: EquationRepository
    indexsets: IndexSetRepository
    parameters: ParameterRepository
    scalars: ScalarRepository
    tables: TableRepository
    variables: OptimizationVariableRepository


class SqlAlchemyBackend(Backend):
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

    Session = sessionmaker(autocommit=False, autoflush=False, future=True)
    auth_context: AuthorizationContext | None = None
    event_handler: SqlaEventHandler

    def __init__(self, info: PlatformInfo) -> None:
        super().__init__(info)
        logger.info(f"Creating database engine for platform '{info.name}'.")
        configure_mappers()

        dsn = self.check_dsn(info.dsn)
        self.make_engine(dsn)
        self.make_repositories()
        self.event_handler = SqlaEventHandler(self)

    def check_dsn(self, dsn: str) -> str:
        if dsn.startswith("postgresql://"):
            logger.debug(
                "Replacing the platform dsn prefix to use the new `psycopg` driver."
            )
            dsn = dsn.replace("postgresql://", "postgresql+psycopg://")
        return dsn

    def make_engine(self, dsn: str) -> None:
        self.engine = cached_create_engine(dsn)
        self.session = self.Session(bind=self.engine)

    def make_repositories(self) -> None:
        self.iamc.datapoints = DataPointRepository(self)
        self.iamc.timeseries = TimeSeriesRepository(self)
        self.iamc.variables = VariableRepository(self)
        self.meta = RunMetaEntryRepository(self)
        self.models = ModelRepository(self)
        self.optimization.equations = EquationRepository(self)
        self.optimization.indexsets = IndexSetRepository(self)
        self.optimization.parameters = ParameterRepository(self)
        self.optimization.scalars = ScalarRepository(self)
        self.optimization.tables = TableRepository(self)
        self.optimization.variables = OptimizationVariableRepository(self)
        self.regions = RegionRepository(self)
        self.runs = RunRepository(self)
        self.scenarios = ScenarioRepository(self)
        self.units = UnitRepository(self)
        self.checkpoints = CheckpointRepository(self)

    @contextmanager
    def auth(
        self,
        user: User,
        manager: ManagerConfig,
        info: ManagerPlatformInfo,
        overlap_ok: bool = False,
    ) -> Generator[AuthorizationContext, None, None]:
        if self.auth_context is not None and not overlap_ok:
            raise ProgrammingError("Overlapping auth context.")

        self.auth_context = AuthorizationContext(user, manager, info)
        yield self.auth_context
        self.auth_context = None

    def _create_all(self) -> None:
        BaseModel.metadata.create_all(bind=self.engine)

    def _drop_all(self) -> None:
        BaseModel.metadata.drop_all(bind=self.engine, checkfirst=True)

    def setup(self) -> None:
        self._create_all()

    def teardown(self) -> None:
        self.session.rollback()
        self._drop_all()

    def close(self) -> None:
        self.session.close()
        self.engine.dispose()


class SqliteTestBackend(SqlAlchemyBackend):
    def __init__(self, *args: Unpack[tuple[PlatformInfo]]) -> None:
        super().__init__(*args)

    def make_engine(self, dsn: str) -> None:
        self.engine = create_engine(
            dsn,
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
            max_identifier_length=63,
        )
        self.session = self.Session(bind=self.engine)


class PostgresTestBackend(SqlAlchemyBackend):
    def __init__(self, *args: Unpack[tuple[PlatformInfo]]) -> None:
        super().__init__(*args)

    def make_engine(self, dsn: str) -> None:
        self.engine = create_engine(
            dsn,
            poolclass=NullPool,
            max_identifier_length=63,
        )
        self.session = self.Session(bind=self.engine)
