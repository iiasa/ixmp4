import logging
from contextlib import contextmanager
from functools import lru_cache
from typing import Generator

from sqlalchemy.engine import Engine, create_engine
from sqlalchemy.orm.session import sessionmaker
from sqlalchemy.pool import StaticPool

from ixmp4.conf.base import PlatformInfo
from ixmp4.conf.manager import ManagerConfig, ManagerPlatformInfo
from ixmp4.conf.user import User
from ixmp4.core.exceptions import ProgrammingError
from ixmp4.data.db import (
    BaseModel,
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

from ..auth.context import AuthorizationContext
from .base import Backend

logger = logging.getLogger(__name__)


@lru_cache()
def cached_create_engine(dsn: str) -> Engine:
    logger.info(f"Creating database engine for {dsn}")
    return create_engine(dsn)


class SqlAlchemyBackend(Backend):
    runs: RunRepository
    Session = sessionmaker(autocommit=False, autoflush=False, future=True)
    auth_context: AuthorizationContext | None = None

    def __init__(self, info: PlatformInfo) -> None:
        super().__init__(info)
        self.make_engine(info.dsn)
        self.make_repositories()

    def make_engine(self, dsn: str):
        if dsn.startswith("postgresql://"):
            logger.debug(
                "Replacing the platform dsn prefix to use the new `psycopg` driver."
            )
            dsn = dsn.replace("postgresql://", "postgresql+psycopg://")
        self.engine = cached_create_engine(dsn)
        self.session = self.Session(bind=self.engine)

    def make_repositories(self):
        self.iamc.datapoints = DataPointRepository(self)
        self.iamc.timeseries = TimeSeriesRepository(self)
        self.iamc.variables = VariableRepository(self)
        self.meta = RunMetaEntryRepository(self)
        self.models = ModelRepository(self)
        self.optimization.indexsets = IndexSetRepository(self)
        self.regions = RegionRepository(self)
        self.runs = RunRepository(self)
        self.scenarios = ScenarioRepository(self)
        self.units = UnitRepository(self)

    def close(self):
        self.session.close()

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

    def _create_all(self):
        BaseModel.metadata.create_all(bind=self.engine)

    def _drop_all(self):
        BaseModel.metadata.drop_all(bind=self.engine, checkfirst=True)

    def reset(self):
        self.session.commit()
        self._drop_all()
        self._create_all()


class SqliteTestBackend(SqlAlchemyBackend):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(
            PlatformInfo(name="sqlite-test", dsn="sqlite:///:memory:"),
            *args,
            **kwargs,
        )
        self.reset()

    def make_engine(self, dsn: str):
        self.engine = create_engine(
            dsn,
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
        self.session = self.Session(bind=self.engine)


class PostgresTestBackend(SqlAlchemyBackend):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(
            PlatformInfo(
                name="postgres-test",
                dsn="postgresql://postgres:postgres@localhost/test",
            ),
            *args,
            **kwargs,
        )
        self.reset()
