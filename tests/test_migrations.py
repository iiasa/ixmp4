from typing import Any, Generator

import pytest
import pytest_alembic
from pytest_alembic import MigrationContext
from pytest_alembic.config import Config
from pytest_alembic.tests import (
    test_model_definitions_match_ddl as model_definitions_match_ddl,
)
from pytest_alembic.tests import (
    test_single_head_revision as single_head_revision,
)
from pytest_alembic.tests import (
    test_up_down_consistency as up_down_consistency,
)
from pytest_alembic.tests import (
    test_upgrade as upgrade,
)
from sqlalchemy import text
from sqlalchemy.exc import OperationalError, ProgrammingError

from .conftest import (
    Backends,
    PostgresTestBackend,
    SqliteTestBackend,
    _GeneratorContextManager,
)


@pytest.fixture(scope="session")
def backends(request: pytest.FixtureRequest) -> Backends:
    postgres_dsn = request.config.option.postgres_dsn
    backends = Backends(postgres_dsn)
    return backends


@pytest.fixture()
def alembic(
    request: pytest.FixtureRequest, backends: Backends, alembic_config: dict[str, Any]
) -> Generator[MigrationContext, Any, None]:
    bctx: (
        _GeneratorContextManager[PostgresTestBackend, None, None]
        | _GeneratorContextManager[SqliteTestBackend, None, None]
    )
    if request.param == "postgres":
        backends_config = request.config.option.backend.split(",")
        if "postgres" not in backends_config:
            pytest.skip("Postgres backend not requested, skipping migration tests.")
        bctx = backends.postgresql()
    elif request.param == "sqlite":
        bctx = backends.sqlite()

    config = Config.from_raw_config(alembic_config)
    with bctx as backend:
        backend.teardown()
        with backend.engine.connect() as conn:
            try:
                conn.execute(text("DROP TABLE alembic_version;"))
                conn.commit()
            except (ProgrammingError, OperationalError):
                conn.rollback()

        with pytest_alembic.runner(config=config, engine=backend.engine) as runner:
            yield runner


@pytest.mark.parametrize("alembic", ["sqlite", "postgres"], indirect=True)
def test_model_definitions_match_ddl(alembic: MigrationContext) -> None:
    """Test that model definitions match the DDL."""
    model_definitions_match_ddl(alembic)


@pytest.mark.parametrize("alembic", ["sqlite", "postgres"], indirect=True)
def test_single_head_revision(alembic: MigrationContext) -> None:
    """Assert that there only exists one head revision."""
    single_head_revision(alembic)


@pytest.mark.parametrize("alembic", ["sqlite", "postgres"], indirect=True)
def test_up_down_consistency(alembic: MigrationContext) -> None:
    """Test that the up and down migrations are consistent."""
    up_down_consistency(alembic)


@pytest.mark.parametrize("alembic", ["sqlite", "postgres"], indirect=True)
def test_upgrade(alembic: MigrationContext) -> None:
    """Test that the upgrade migrations work."""
    upgrade(alembic)
    pass
