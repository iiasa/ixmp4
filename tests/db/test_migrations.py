from typing import Any, Generator

import pytest
import pytest_alembic
import sqlalchemy as sa
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
from sqlalchemy.exc import OperationalError, ProgrammingError

import ixmp4
from ixmp4.transport import DirectTransport
from tests import backends
from tests.fixtures import get_migration_data


@pytest.fixture(scope="session")
def at_revision_c71efc396d2b() -> list[dict[str, Any]]:
    return get_migration_data("c71efc396d2b")


@pytest.fixture
def alembic_config(at_revision_c71efc396d2b: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "script_location": "ixmp4/db/migrations",
        "at_revision_data": {"c71efc396d2b": at_revision_c71efc396d2b},
        "sqlalchemy.url": "fake-test-dsn-which-is-unused",
    }


transport = backends.get_transport_fixture(
    backends=["sqlite", "postgres"], create_tables=False, scope="function"
)


@pytest.fixture()
def alembic(
    alembic_config: dict[str, Any],
    transport: DirectTransport,
) -> Generator[MigrationContext, Any, None]:
    config = Config.from_raw_config(alembic_config)
    assert transport.session.bind is not None
    with transport.session.bind.engine.connect() as conn:
        try:
            conn.execute(sa.text("DROP TABLE alembic_version;"))
            conn.commit()
        except (ProgrammingError, OperationalError):
            conn.rollback()

    with pytest_alembic.runner(
        config=config, engine=transport.session.bind.engine
    ) as runner:
        yield runner


def test_model_definitions_match_ddl(alembic: MigrationContext) -> None:
    """Test that model definitions match the DDL."""
    model_definitions_match_ddl(alembic)


def test_single_head_revision(alembic: MigrationContext) -> None:
    """Assert that there only exists one head revision."""
    single_head_revision(alembic)


def test_up_down_consistency(alembic: MigrationContext) -> None:
    """Test that the up and down migrations are consistent."""
    up_down_consistency(alembic)


def test_upgrade(alembic: MigrationContext, transport: DirectTransport) -> None:
    """Test that the upgrade migrations work."""
    upgrade(alembic)

    # some simple checks to assert all the
    # data is still here
    mp = ixmp4.Platform(transport)

    assert len(mp.regions.list()) == 180
    assert len(mp.units.list()) == 100
    assert len(mp.runs.list(default_only=False)) == 30

    run_meta = mp.meta.tabulate(run={"default_only": False})
    assert len(run_meta) == 100

    iamc_data = mp.iamc.tabulate(run={"default_only": False})
    assert len(iamc_data) == 9436
    assert not iamc_data["value"].isnull().any()
