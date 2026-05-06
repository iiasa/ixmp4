from collections.abc import Iterator
from datetime import datetime
from types import SimpleNamespace
from unittest import mock

import pytest
import sqlalchemy as sa
from sqlalchemy import orm
from toolkit.db.executor import SessionExecutor
from toolkit.db.target import ModelTarget

import ixmp4.data.versions.reverter as reverter_module
from ixmp4.core.exceptions import ProgrammingError
from ixmp4.data.base.db import BaseModel
from ixmp4.data.meta.db import RunMetaEntry
from ixmp4.data.run.db import Run
from ixmp4.data.versions.model import Operation
from ixmp4.data.versions.reverter import Reverter, ReverterRepository
from ixmp4.data.versions.transaction import Transaction


class ReverterTestBase(orm.DeclarativeBase):
    pass


class ReverterItem(ReverterTestBase):
    __tablename__ = "unit_reverter_item"

    id: orm.Mapped[int] = orm.mapped_column(sa.Integer(), primary_key=True)
    name: orm.Mapped[str] = orm.mapped_column(sa.String(255), nullable=False)
    value: orm.Mapped[int] = orm.mapped_column(sa.Integer(), nullable=False)
    mismatch: orm.Mapped[int | None] = orm.mapped_column(sa.Integer(), nullable=True)


class ReverterItemVersion(ReverterTestBase):
    __tablename__ = "unit_reverter_item_version"

    id: orm.Mapped[int] = orm.mapped_column(sa.Integer(), primary_key=True)
    transaction_id: orm.Mapped[int] = orm.mapped_column(
        sa.BigInteger(), primary_key=True, nullable=False, index=True
    )
    operation_type: orm.Mapped[int] = orm.mapped_column(
        sa.SmallInteger(), nullable=False, index=True
    )
    end_transaction_id: orm.Mapped[int | None] = orm.mapped_column(
        sa.BigInteger(), nullable=True, index=True
    )
    name: orm.Mapped[str] = orm.mapped_column(sa.String(255), nullable=False)
    value: orm.Mapped[int] = orm.mapped_column(sa.Integer(), nullable=False)
    mismatch: orm.Mapped[str | None] = orm.mapped_column(sa.String(255), nullable=True)


class DemoReverterRepository(ReverterRepository):
    target = ModelTarget(ReverterItem)
    version_target = ModelTarget(ReverterItemVersion)


@pytest.fixture()
def demo_session() -> Iterator[orm.Session]:
    engine = sa.create_engine("sqlite:///:memory:")
    BaseModel.metadata.create_all(bind=engine, tables=[Transaction.__table__])
    ReverterTestBase.metadata.create_all(bind=engine)
    session = orm.Session(engine)

    try:
        yield session
    finally:
        session.close()
        ReverterTestBase.metadata.drop_all(bind=engine)
        BaseModel.metadata.drop_all(bind=engine, tables=[Transaction.__table__])
        engine.dispose()


def seed_demo_state(session: orm.Session) -> None:
    session.add_all(
        [
            Transaction(id=1, issued_at=datetime(2024, 1, 1, 0, 0, 0)),
            Transaction(id=2, issued_at=datetime(2024, 1, 1, 0, 1, 0)),
            Transaction(id=3, issued_at=datetime(2024, 1, 1, 0, 2, 0)),
            ReverterItem(id=1, name="alpha-updated", value=11, mismatch=111),
            ReverterItem(id=3, name="new-item", value=30, mismatch=333),
            ReverterItemVersion(
                id=1,
                name="alpha",
                value=10,
                mismatch="unused-alpha",
                transaction_id=1,
                operation_type=Operation.INSERT.value,
                end_transaction_id=2,
            ),
            ReverterItemVersion(
                id=1,
                name="alpha-updated",
                value=11,
                mismatch="unused-alpha-updated",
                transaction_id=2,
                operation_type=Operation.UPDATE.value,
                end_transaction_id=None,
            ),
            ReverterItemVersion(
                id=2,
                name="deleted-item",
                value=20,
                mismatch="unused-deleted",
                transaction_id=1,
                operation_type=Operation.INSERT.value,
                end_transaction_id=2,
            ),
            ReverterItemVersion(
                id=2,
                name="deleted-item",
                value=20,
                mismatch="unused-deleted",
                transaction_id=2,
                operation_type=Operation.DELETE.value,
                end_transaction_id=None,
            ),
            ReverterItemVersion(
                id=3,
                name="new-item",
                value=30,
                mismatch="unused-new",
                transaction_id=3,
                operation_type=Operation.INSERT.value,
                end_transaction_id=None,
            ),
        ]
    )
    session.commit()


def test_reverter_repository_requires_version_target(demo_session: orm.Session) -> None:
    class MissingVersionRepository(ReverterRepository):
        target = ModelTarget(ReverterItem)

    with pytest.raises(ProgrammingError, match="requires a `version_target`"):
        MissingVersionRepository(SessionExecutor(demo_session))


def test_reverter_repository_tracks_versioned_columns_and_valid_rows(
    demo_session: orm.Session,
) -> None:
    seed_demo_state(demo_session)
    repository = DemoReverterRepository(SessionExecutor(demo_session))

    assert set(repository.versioned_columns.keys()) == {"id", "name", "value"}

    all_ids = demo_session.execute(
        repository.select_versions().with_only_columns(ReverterItemVersion.id)
    ).scalars()
    assert sorted(all_ids) == [1, 1, 2, 2, 3]

    base_query = repository.select_versions().with_only_columns(ReverterItemVersion.id)
    origin_ids = demo_session.execute(
        repository.where_valid_at_tx(base_query, 3)
    ).scalars()
    compare_ids = demo_session.execute(
        repository.where_valid_at_tx(base_query, 1)
    ).scalars()

    assert sorted(origin_ids) == [1, 3]
    assert sorted(compare_ids) == [1, 2]

    with pytest.raises(ProgrammingError, match="must be bigger"):
        repository.check_tx_ids(1, 2)


def test_reverter_repository_tabulates_revert_operations(
    demo_session: orm.Session,
) -> None:
    seed_demo_state(demo_session)
    repository = DemoReverterRepository(SessionExecutor(demo_session))

    operations = repository.tabulate_revert_ops(3, 1).sort_values("id").set_index("id")

    assert operations[repository.revert_op_label].to_dict() == {
        1: Operation.UPDATE.value,
        2: Operation.INSERT.value,
        3: Operation.DELETE.value,
    }
    assert operations["name"].to_dict() == {
        1: "alpha",
        2: "deleted-item",
        3: "new-item",
    }
    assert operations["value"].to_dict() == {1: 10, 2: 20, 3: 30}


def test_reverter_repository_reverts_destructive_and_constructive_changes(
    demo_session: orm.Session,
) -> None:
    seed_demo_state(demo_session)
    repository = DemoReverterRepository(SessionExecutor(demo_session))

    deleted = repository.revert_destructive(3, 1)
    assert deleted == 1

    after_delete = demo_session.execute(
        sa.select(ReverterItem).order_by(ReverterItem.id)
    ).scalars()
    assert [(item.id, item.name, item.value) for item in after_delete] == [
        (1, "alpha-updated", 11)
    ]

    repository.revert_constructive(3, 1)
    reverted = demo_session.execute(
        sa.select(ReverterItem).order_by(ReverterItem.id)
    ).scalars()
    assert [(item.id, item.name, item.value, item.mismatch) for item in reverted] == [
        (1, "alpha", 10, 111),
        (2, "deleted-item", 20, None),
    ]


def test_reverter_repository_revert_uses_latest_transaction(
    demo_session: orm.Session,
) -> None:
    seed_demo_state(demo_session)
    repository = DemoReverterRepository(SessionExecutor(demo_session))

    repository.revert(1)

    reverted = demo_session.execute(
        sa.select(ReverterItem).order_by(ReverterItem.id)
    ).scalars()
    assert [(item.id, item.name, item.value, item.mismatch) for item in reverted] == [
        (1, "alpha", 10, 111),
        (2, "deleted-item", 20, None),
    ]


def test_reverter_orders_repositories_and_commits_per_phase(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    log: list[tuple[str, str, int, int, tuple[object, ...], dict[str, object]]] = []

    class MockTransactions:
        def __init__(self, executor: object):
            self.executor = executor

        def latest(self) -> SimpleNamespace:
            return SimpleNamespace(id=9)

    monkeypatch.setattr(reverter_module, "TransactionRepository", MockTransactions)

    class ParentRepository:
        target = SimpleNamespace(table=Run.__table__)

        def __init__(self, executor: object):
            self.executor = executor

        def revert_destructive(
            self, origin_tx_id: int, tx_id: int, *args: object, **kwargs: object
        ) -> None:
            log.append(("destructive", "parent", origin_tx_id, tx_id, args, kwargs))

        def revert_constructive(
            self, origin_tx_id: int, tx_id: int, *args: object, **kwargs: object
        ) -> None:
            log.append(("constructive", "parent", origin_tx_id, tx_id, args, kwargs))

    class ChildRepository:
        target = SimpleNamespace(table=RunMetaEntry.__table__)

        def __init__(self, executor: object):
            self.executor = executor

        def revert_destructive(
            self, origin_tx_id: int, tx_id: int, *args: object, **kwargs: object
        ) -> None:
            log.append(("destructive", "child", origin_tx_id, tx_id, args, kwargs))

        def revert_constructive(
            self, origin_tx_id: int, tx_id: int, *args: object, **kwargs: object
        ) -> None:
            log.append(("constructive", "child", origin_tx_id, tx_id, args, kwargs))

    executor = SimpleNamespace(session=SimpleNamespace(commit=mock.Mock()))
    reverter = Reverter([ChildRepository, ParentRepository])

    assert reverter.repo_classes == [ParentRepository, ChildRepository]

    reverter(executor, 4, "scope", force=True)

    assert log == [
        ("destructive", "child", 9, 4, ("scope",), {"force": True}),
        ("destructive", "parent", 9, 4, ("scope",), {"force": True}),
        ("constructive", "parent", 9, 4, ("scope",), {"force": True}),
        ("constructive", "child", 9, 4, ("scope",), {"force": True}),
    ]
    assert executor.session.commit.call_count == 4
