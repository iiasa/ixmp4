"""Integration tests for DirectTransport.check_alembic_version()."""

from pathlib import Path
from unittest import mock

import pytest
import sqlalchemy as sa
from sqlalchemy import orm

from ixmp4.base_exceptions import ImproperlyConfigured
from ixmp4.db import get_alembic_controller
from ixmp4.transport import DirectTransport


@pytest.fixture
def engine(tmp_path: Path) -> sa.Engine:
    """File-based SQLite engine backed by a per-test temp directory."""
    return sa.create_engine(
        f"sqlite:///{tmp_path / 'test.sqlite3'}",
        poolclass=sa.StaticPool,
        connect_args={"check_same_thread": False},
    )


@pytest.fixture
def transport(engine: sa.Engine) -> DirectTransport:
    """DirectTransport wrapping *engine* with all start-up checks disabled."""
    session = orm.sessionmaker(autocommit=False, autoflush=False)(bind=engine)
    return DirectTransport(session, ping_database=False, check_alembic_version=False)


@pytest.fixture
def version_table(engine: sa.Engine) -> None:
    """Create the bare ``alembic_version`` table without running any migration."""
    with engine.connect() as conn:
        conn.execute(
            sa.text(
                "CREATE TABLE alembic_version "
                "(version_num VARCHAR(32) NOT NULL, PRIMARY KEY (version_num))"
            )
        )
        conn.commit()


@pytest.fixture(scope="module")
def head_revision() -> str:
    """Current head revision hash from the real migration scripts."""
    ctrl = get_alembic_controller("sqlite:///:memory:")
    head = ctrl.get_head_revision()
    assert isinstance(head, str), "Expected a single head revision"
    return head


@pytest.fixture(scope="module")
def oldest_revision() -> str:
    """Oldest known revision hash from the real migration scripts."""
    ctrl = get_alembic_controller("sqlite:///:memory:")
    # walk_revisions yields newest-first; last entry is the oldest
    revisions = [s.revision for s in ctrl.list_revisions()]
    return revisions[-1]


def _insert_revision(engine: sa.Engine, revision: str) -> None:
    """Insert *revision* into an existing ``alembic_version`` table."""
    with engine.connect() as conn:
        conn.execute(
            sa.text("INSERT INTO alembic_version VALUES (:rev)"),
            {"rev": revision},
        )
        conn.commit()


class TestCheckAlembicVersion:
    def test_missing_alembic_version_table(self, transport: DirectTransport) -> None:
        """Raises when the ``alembic_version`` table does not exist at all."""
        with pytest.raises(
            ImproperlyConfigured,
            match="'alembic_version' does not exist",
        ):
            transport.check_alembic_version()

    def test_head_revision_is_none(
        self, transport: DirectTransport, engine: sa.Engine, version_table: None
    ) -> None:
        """Raises when ``get_head_revision`` returns ``None``."""
        _insert_revision(engine, "abc123abc123")

        mock_ctrl = mock.Mock()
        mock_ctrl.get_database_revision.return_value = "abc123abc123"
        mock_ctrl.get_head_revision.return_value = None

        with mock.patch(
            "ixmp4.transport.get_alembic_controller", return_value=mock_ctrl
        ):
            with pytest.raises(
                ImproperlyConfigured,
                match="Could not determine the expected alembic revision",
            ):
                transport.check_alembic_version()

    def test_head_revision_multiple_heads(
        self, transport: DirectTransport, engine: sa.Engine, version_table: None
    ) -> None:
        """Raises when ``get_head_revision`` returns a tuple with more than one
        entry."""
        _insert_revision(engine, "abc123abc123")

        mock_ctrl = mock.Mock()
        mock_ctrl.get_database_revision.return_value = "abc123abc123"
        mock_ctrl.get_head_revision.return_value = ("head_a1b2c3d4", "head_e5f6a7b8")

        with mock.patch(
            "ixmp4.transport.get_alembic_controller", return_value=mock_ctrl
        ):
            with pytest.raises(
                ImproperlyConfigured,
                match="multiple heads were found",
            ):
                transport.check_alembic_version()

    def test_empty_alembic_version_table(
        self, transport: DirectTransport, version_table: None
    ) -> None:
        """Raises when the ``alembic_version`` table exists but contains no rows."""
        with pytest.raises(
            ImproperlyConfigured,
            match="no alembic revision entry was found",
        ):
            transport.check_alembic_version()

    def test_outdated_known_revision(
        self,
        transport: DirectTransport,
        engine: sa.Engine,
        version_table: None,
        oldest_revision: str,
    ) -> None:
        """Raises with an 'older revision' message when the DB lags behind head."""
        _insert_revision(engine, oldest_revision)

        with pytest.raises(
            ImproperlyConfigured,
            match="found older revision",
        ):
            transport.check_alembic_version()

    def test_unknown_revision(
        self, transport: DirectTransport, engine: sa.Engine, version_table: None
    ) -> None:
        """Raises a generic mismatch message for a completely unknown revision.

        This covers the case where the database was created by a *newer* ixmp4
        version and the current installation does not know that revision.
        """
        _insert_revision(engine, "deadbeefcafe1234")

        with pytest.raises(
            ImproperlyConfigured,
            match="Upgrade your ixmp4 installation",
        ):
            transport.check_alembic_version()

    def test_valid_head_revision_passes(
        self,
        transport: DirectTransport,
        engine: sa.Engine,
        version_table: None,
        head_revision: str,
    ) -> None:
        """No exception is raised when the DB revision matches head."""
        _insert_revision(engine, head_revision)

        transport.check_alembic_version()  # must not raise
