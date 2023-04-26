from typing import Generator
from contextlib import contextmanager

from sqlalchemy.engine import create_engine
from sqlalchemy.orm.session import sessionmaker
from sqlalchemy.pool import StaticPool

from ixmp4.core.exceptions import ProgrammingError
from ixmp4.conf import PlatformInfo
from ixmp4.conf.user import User
from ixmp4.conf.manager import ManagerConfig, ManagerPlatformInfo
from ixmp4.data.db import (
    BaseModel,
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

from ..auth.context import AuthorizationContext
from .base import Backend


class SqlAlchemyBackend(Backend):
    runs: RunRepository
    Session = sessionmaker(autocommit=False, autoflush=False, future=True)
    auth_context: AuthorizationContext | None = None

    def __init__(self, info: PlatformInfo) -> None:
        super().__init__(info)
        self.make_engine(info.dsn)
        self.make_repositories()

    def make_repositories(self):
        self.runs = RunRepository(self)
        self.meta = RunMetaEntryRepository(self)
        self.iamc.datapoints = DataPointRepository(self)
        self.iamc.timeseries = TimeSeriesRepository(self)
        self.iamc.variables = VariableRepository(self)
        self.regions = RegionRepository(self)
        self.scenarios = ScenarioRepository(self)
        self.models = ModelRepository(self)
        self.units = UnitRepository(self)

    def make_engine(self, dsn: str):
        self.engine = create_engine(dsn)
        self.session = self.Session(bind=self.engine)

    def close(self):
        self.session.close()
        self.engine.dispose()

    @contextmanager
    def auth(
        self, user: User, manager: ManagerConfig, info: ManagerPlatformInfo
    ) -> Generator[AuthorizationContext, None, None]:
        if self.auth_context is not None:
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


class OracleTestBackend(SqlAlchemyBackend):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(
            PlatformInfo(
                name="oracle-test",
                dsn="oracle+oracledb://ixmp4_test:ixmp4_test@localhost:1521?service_name=XEPDB1",
            ),
            *args,
            **kwargs,
        )
        self.reset()
