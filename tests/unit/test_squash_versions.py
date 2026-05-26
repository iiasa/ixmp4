"""Unit tests for ixmp4.data.versions.squash step functions.

Each step function is exercised in isolation using an in-memory SQLite
database.
"""

from collections.abc import Iterator
from datetime import datetime, timezone

import pytest
import sqlalchemy as sa

from ixmp4.data.versions.model import Operation
from ixmp4.data.versions.squash import (
    _delete_immediately_invalidated,
    _delete_superseded,
    _delete_transient,
    _delete_unused_transactions,
    _promote_orphaned_updates_to_inserts,
    _remap_end_transaction_id,
    _remap_transaction_id,
    squash_version_records,
)

_NOW = datetime(2024, 1, 1, tzinfo=timezone.utc)

_VRow = tuple[int, int, int | None, int]  # id, tx, end_tx, op


class SquashTest:
    @staticmethod
    def create_tables(meta: sa.MetaData) -> tuple[sa.Table, sa.Table, sa.Table]:
        tx_table = sa.Table(
            "transaction",
            meta,
            sa.Column("id", sa.Integer, primary_key=True),
            sa.Column("issued_at", sa.DateTime, nullable=True),
        )
        cp_table = sa.Table(
            "checkpoint",
            meta,
            sa.Column("id", sa.Integer, primary_key=True),
            sa.Column("run__id", sa.Integer, nullable=False),
            sa.Column("transaction__id", sa.Integer, nullable=True),
            sa.Column("message", sa.String(1023), nullable=False),
        )
        vt = sa.Table(
            "test_version",
            meta,
            sa.Column("id", sa.Integer),
            sa.Column("transaction_id", sa.Integer, nullable=False),
            sa.Column("end_transaction_id", sa.Integer, nullable=True),
            sa.Column("operation_type", sa.Integer, nullable=False),
        )
        return tx_table, cp_table, vt

    @pytest.fixture()
    def db_conn(self) -> Iterator[tuple[sa.engine.Connection, sa.Table]]:
        """Yield an (open SQLite connection, version_table) pair."""
        engine = sa.create_engine("sqlite:///:memory:")
        meta = sa.MetaData()
        _tx_tbl, _cp_tbl, version_table = self.create_tables(meta)
        meta.create_all(engine)

        with engine.connect() as conn:
            yield conn, version_table

        engine.dispose()

    @staticmethod
    def seed_versions(
        conn: sa.engine.Connection,
        *,
        transactions: list[int],
        checkpoints: list[int],
        version_rows: list[_VRow],
        version_table: sa.Table,
    ) -> None:
        meta = version_table.metadata
        tx_table = meta.tables["transaction"]
        cp_table = meta.tables["checkpoint"]

        for tx_id in transactions:
            conn.execute(tx_table.insert().values(id=tx_id, issued_at=_NOW))
        for idx, tx_id in enumerate(checkpoints, start=1):
            conn.execute(
                cp_table.insert().values(
                    id=idx,
                    run__id=idx,
                    transaction__id=tx_id if tx_id != 0 else None,
                    message="checkpoint",
                )
            )
        if version_rows:
            conn.execute(
                version_table.insert(),
                [
                    {
                        "id": id_,
                        "transaction_id": tx,
                        "end_transaction_id": end_tx,
                        "operation_type": int(op),
                    }
                    for id_, tx, end_tx, op in version_rows
                ],
            )
        conn.commit()

    @staticmethod
    def get_version_rows(
        conn: sa.engine.Connection, version_table: sa.Table
    ) -> list[_VRow]:
        """Return all version rows sorted by (id, transaction_id)."""
        result = conn.execute(
            sa.select(
                version_table.c.id,
                version_table.c.transaction_id,
                version_table.c.end_transaction_id,
                version_table.c.operation_type,
            ).order_by(version_table.c.id, version_table.c.transaction_id)
        )
        return [
            (r.id, r.transaction_id, r.end_transaction_id, r.operation_type)
            for r in result
        ]

    @staticmethod
    def get_all_tx_ids(conn: sa.engine.Connection, version_table: sa.Table) -> set[int]:
        """Return the set of ids currently in the transaction table."""
        tx_table = version_table.metadata.tables["transaction"]
        return {r.id for r in conn.execute(sa.select(tx_table.c.id))}


class TestDeleteSuperseded(SquashTest):
    def test_loser_in_same_interval_deleted(
        self, db_conn: tuple[sa.engine.Connection, sa.Table]
    ) -> None:
        conn, vt = db_conn
        self.seed_versions(
            conn,
            transactions=[1, 2],
            checkpoints=[2],
            version_rows=[
                (1, 1, 2, Operation.INSERT),
                (1, 2, None, Operation.UPDATE),
            ],
            version_table=vt,
        )
        _delete_superseded(conn, vt)
        assert self.get_version_rows(conn, vt) == [(1, 2, None, int(Operation.UPDATE))]

    def test_all_intermediate_losers_deleted(
        self, db_conn: tuple[sa.engine.Connection, sa.Table]
    ) -> None:
        conn, vt = db_conn
        self.seed_versions(
            conn,
            transactions=[1, 2, 3],
            checkpoints=[3],
            version_rows=[
                (1, 1, 2, Operation.INSERT),
                (1, 2, 3, Operation.UPDATE),
                (1, 3, None, Operation.UPDATE),
            ],
            version_table=vt,
        )
        _delete_superseded(conn, vt)
        assert self.get_version_rows(conn, vt) == [(1, 3, None, int(Operation.UPDATE))]

    def test_records_in_different_intervals_both_kept(
        self, db_conn: tuple[sa.engine.Connection, sa.Table]
    ) -> None:
        conn, vt = db_conn
        self.seed_versions(
            conn,
            transactions=[1, 2, 3, 4],
            checkpoints=[2, 4],
            version_rows=[
                (1, 1, 2, Operation.INSERT),
                (1, 3, None, Operation.UPDATE),
            ],
            version_table=vt,
        )
        _delete_superseded(conn, vt)
        assert len(self.get_version_rows(conn, vt)) == 2

    def test_record_beyond_last_checkpoint_untouched(
        self, db_conn: tuple[sa.engine.Connection, sa.Table]
    ) -> None:
        conn, vt = db_conn
        self.seed_versions(
            conn,
            transactions=[1, 2, 3],
            checkpoints=[1],
            version_rows=[
                (1, 2, None, Operation.INSERT),
                (1, 3, None, Operation.UPDATE),
            ],
            version_table=vt,
        )
        _delete_superseded(conn, vt)
        assert len(self.get_version_rows(conn, vt)) == 2

    def test_different_entity_ids_evaluated_independently(
        self, db_conn: tuple[sa.engine.Connection, sa.Table]
    ) -> None:
        conn, vt = db_conn
        self.seed_versions(
            conn,
            transactions=[1, 2],
            checkpoints=[2],
            version_rows=[
                (1, 1, 2, Operation.INSERT),
                (1, 2, None, Operation.UPDATE),
                (2, 1, None, Operation.INSERT),
            ],
            version_table=vt,
        )
        _delete_superseded(conn, vt)
        remaining = self.get_version_rows(conn, vt)
        assert (2, 1, None, int(Operation.INSERT)) in remaining
        assert len(remaining) == 2

    def test_checkpoint_boundary_record_kept(
        self, db_conn: tuple[sa.engine.Connection, sa.Table]
    ) -> None:
        conn, vt = db_conn
        self.seed_versions(
            conn,
            transactions=[3],
            checkpoints=[3],
            version_rows=[(1, 3, None, Operation.INSERT)],
            version_table=vt,
        )
        _delete_superseded(conn, vt)
        assert len(self.get_version_rows(conn, vt)) == 1


class TestDeleteTransient(SquashTest):
    def test_record_opened_and_closed_in_same_interval_removed(
        self, db_conn: tuple[sa.engine.Connection, sa.Table]
    ) -> None:
        conn, vt = db_conn
        self.seed_versions(
            conn,
            transactions=[1, 2, 3],
            checkpoints=[3],
            version_rows=[
                (1, 1, 2, Operation.INSERT),
                (1, 2, None, Operation.UPDATE),
            ],
            version_table=vt,
        )
        _delete_transient(conn, vt)
        assert self.get_version_rows(conn, vt) == [(1, 2, None, int(Operation.UPDATE))]

    def test_delete_marker_in_same_interval_kept(
        self, db_conn: tuple[sa.engine.Connection, sa.Table]
    ) -> None:
        conn, vt = db_conn
        self.seed_versions(
            conn,
            transactions=[1, 2, 3],
            checkpoints=[3],
            version_rows=[(1, 1, 2, Operation.DELETE)],
            version_table=vt,
        )
        _delete_transient(conn, vt)
        assert len(self.get_version_rows(conn, vt)) == 1

    def test_record_spanning_two_intervals_kept(
        self, db_conn: tuple[sa.engine.Connection, sa.Table]
    ) -> None:
        conn, vt = db_conn
        self.seed_versions(
            conn,
            transactions=[1, 2, 3, 4],
            checkpoints=[2, 4],
            version_rows=[(1, 1, 3, Operation.INSERT)],
            version_table=vt,
        )
        _delete_transient(conn, vt)
        assert len(self.get_version_rows(conn, vt)) == 1

    def test_open_record_not_deleted(
        self, db_conn: tuple[sa.engine.Connection, sa.Table]
    ) -> None:
        conn, vt = db_conn
        self.seed_versions(
            conn,
            transactions=[1, 3],
            checkpoints=[3],
            version_rows=[(1, 1, None, Operation.INSERT)],
            version_table=vt,
        )
        _delete_transient(conn, vt)
        assert len(self.get_version_rows(conn, vt)) == 1

    def test_record_without_covering_checkpoint_not_deleted(
        self, db_conn: tuple[sa.engine.Connection, sa.Table]
    ) -> None:
        conn, vt = db_conn
        self.seed_versions(
            conn,
            transactions=[1, 2],
            checkpoints=[],
            version_rows=[(1, 1, 2, Operation.INSERT)],
            version_table=vt,
        )
        _delete_transient(conn, vt)
        assert len(self.get_version_rows(conn, vt)) == 1


class TestRemapTransactionId(SquashTest):
    def test_transaction_id_remapped_to_covering_checkpoint(
        self, db_conn: tuple[sa.engine.Connection, sa.Table]
    ) -> None:
        conn, vt = db_conn
        self.seed_versions(
            conn,
            transactions=[1, 3],
            checkpoints=[3],
            version_rows=[(1, 1, None, Operation.INSERT)],
            version_table=vt,
        )
        _remap_transaction_id(conn, vt)
        assert self.get_version_rows(conn, vt) == [(1, 3, None, int(Operation.INSERT))]

    def test_already_at_checkpoint_unchanged(
        self, db_conn: tuple[sa.engine.Connection, sa.Table]
    ) -> None:
        conn, vt = db_conn
        self.seed_versions(
            conn,
            transactions=[3],
            checkpoints=[3],
            version_rows=[(1, 3, None, Operation.INSERT)],
            version_table=vt,
        )
        _remap_transaction_id(conn, vt)
        assert self.get_version_rows(conn, vt) == [(1, 3, None, int(Operation.INSERT))]

    def test_record_beyond_last_checkpoint_unchanged(
        self, db_conn: tuple[sa.engine.Connection, sa.Table]
    ) -> None:
        conn, vt = db_conn
        self.seed_versions(
            conn,
            transactions=[1, 2, 5],
            checkpoints=[2],
            version_rows=[(1, 5, None, Operation.INSERT)],
            version_table=vt,
        )
        _remap_transaction_id(conn, vt)
        assert self.get_version_rows(conn, vt) == [(1, 5, None, int(Operation.INSERT))]

    def test_multiple_records_remapped_to_their_own_checkpoint(
        self, db_conn: tuple[sa.engine.Connection, sa.Table]
    ) -> None:
        conn, vt = db_conn
        self.seed_versions(
            conn,
            transactions=[1, 2, 3, 4, 5],
            checkpoints=[3, 5],
            version_rows=[
                (1, 1, None, Operation.INSERT),
                (2, 4, None, Operation.INSERT),
            ],
            version_table=vt,
        )
        _remap_transaction_id(conn, vt)
        result = self.get_version_rows(conn, vt)
        assert (1, 3, None, int(Operation.INSERT)) in result
        assert (2, 5, None, int(Operation.INSERT)) in result


class TestRemapEndTransactionId(SquashTest):
    def test_end_transaction_id_remapped_to_covering_checkpoint(
        self, db_conn: tuple[sa.engine.Connection, sa.Table]
    ) -> None:
        conn, vt = db_conn
        self.seed_versions(
            conn,
            transactions=[1, 3],
            checkpoints=[3],
            version_rows=[(1, 1, 1, Operation.UPDATE)],
            version_table=vt,
        )
        _remap_end_transaction_id(conn, vt)
        assert self.get_version_rows(conn, vt) == [(1, 1, 3, int(Operation.UPDATE))]

    def test_null_end_transaction_id_unchanged(
        self, db_conn: tuple[sa.engine.Connection, sa.Table]
    ) -> None:
        conn, vt = db_conn
        self.seed_versions(
            conn,
            transactions=[1, 3],
            checkpoints=[3],
            version_rows=[(1, 1, None, Operation.INSERT)],
            version_table=vt,
        )
        _remap_end_transaction_id(conn, vt)
        assert self.get_version_rows(conn, vt) == [(1, 1, None, int(Operation.INSERT))]

    def test_end_tx_beyond_last_checkpoint_unchanged(
        self, db_conn: tuple[sa.engine.Connection, sa.Table]
    ) -> None:
        conn, vt = db_conn
        self.seed_versions(
            conn,
            transactions=[1, 2, 5],
            checkpoints=[2],
            version_rows=[(1, 1, 5, Operation.UPDATE)],
            version_table=vt,
        )
        _remap_end_transaction_id(conn, vt)
        assert self.get_version_rows(conn, vt) == [(1, 1, 5, int(Operation.UPDATE))]

    def test_end_tx_at_checkpoint_unchanged(
        self, db_conn: tuple[sa.engine.Connection, sa.Table]
    ) -> None:
        conn, vt = db_conn
        self.seed_versions(
            conn,
            transactions=[1, 3],
            checkpoints=[3],
            version_rows=[(1, 1, 3, Operation.UPDATE)],
            version_table=vt,
        )
        _remap_end_transaction_id(conn, vt)
        assert self.get_version_rows(conn, vt) == [(1, 1, 3, int(Operation.UPDATE))]


class TestPromoteOrphanedUpdatesToInserts(SquashTest):
    def test_update_without_predecessor_promoted_to_insert(
        self, db_conn: tuple[sa.engine.Connection, sa.Table]
    ) -> None:
        conn, vt = db_conn
        self.seed_versions(
            conn,
            transactions=[3],
            checkpoints=[3],
            version_rows=[(1, 3, None, Operation.UPDATE)],
            version_table=vt,
        )
        _promote_orphaned_updates_to_inserts(conn, vt)
        assert self.get_version_rows(conn, vt) == [(1, 3, None, int(Operation.INSERT))]

    def test_update_with_predecessor_unchanged(
        self, db_conn: tuple[sa.engine.Connection, sa.Table]
    ) -> None:
        conn, vt = db_conn
        self.seed_versions(
            conn,
            transactions=[1, 3],
            checkpoints=[3],
            version_rows=[
                (1, 1, 3, Operation.INSERT),
                (1, 3, None, Operation.UPDATE),
            ],
            version_table=vt,
        )
        _promote_orphaned_updates_to_inserts(conn, vt)
        rows = self.get_version_rows(conn, vt)
        assert (1, 1, 3, int(Operation.INSERT)) in rows
        assert (1, 3, None, int(Operation.UPDATE)) in rows

    def test_insert_unchanged(
        self, db_conn: tuple[sa.engine.Connection, sa.Table]
    ) -> None:
        conn, vt = db_conn
        self.seed_versions(
            conn,
            transactions=[3],
            checkpoints=[3],
            version_rows=[(1, 3, None, Operation.INSERT)],
            version_table=vt,
        )
        _promote_orphaned_updates_to_inserts(conn, vt)
        assert self.get_version_rows(conn, vt) == [(1, 3, None, int(Operation.INSERT))]

    def test_delete_unchanged(
        self, db_conn: tuple[sa.engine.Connection, sa.Table]
    ) -> None:
        conn, vt = db_conn
        self.seed_versions(
            conn,
            transactions=[3],
            checkpoints=[3],
            version_rows=[(1, 3, None, Operation.DELETE)],
            version_table=vt,
        )
        _promote_orphaned_updates_to_inserts(conn, vt)
        assert self.get_version_rows(conn, vt) == [(1, 3, None, int(Operation.DELETE))]

    def test_chain_of_updates_only_first_promoted(
        self, db_conn: tuple[sa.engine.Connection, sa.Table]
    ) -> None:
        conn, vt = db_conn
        self.seed_versions(
            conn,
            transactions=[3, 5],
            checkpoints=[3, 5],
            version_rows=[
                (1, 3, 5, Operation.UPDATE),
                (1, 5, None, Operation.UPDATE),
            ],
            version_table=vt,
        )
        _promote_orphaned_updates_to_inserts(conn, vt)
        rows = self.get_version_rows(conn, vt)
        assert (1, 3, 5, int(Operation.INSERT)) in rows
        assert (1, 5, None, int(Operation.UPDATE)) in rows

    def test_different_ids_evaluated_independently(
        self, db_conn: tuple[sa.engine.Connection, sa.Table]
    ) -> None:
        conn, vt = db_conn
        self.seed_versions(
            conn,
            transactions=[3, 5],
            checkpoints=[3, 5],
            version_rows=[
                (1, 3, None, Operation.UPDATE),  # no predecessor -> INSERT
                (2, 3, 5, Operation.INSERT),
                (2, 5, None, Operation.UPDATE),  # has predecessor -> UPDATE
            ],
            version_table=vt,
        )
        _promote_orphaned_updates_to_inserts(conn, vt)
        rows = self.get_version_rows(conn, vt)
        assert (1, 3, None, int(Operation.INSERT)) in rows
        assert (2, 3, 5, int(Operation.INSERT)) in rows
        assert (2, 5, None, int(Operation.UPDATE)) in rows


class TestDeleteImmediatelyInvalidated(SquashTest):
    def test_zero_interval_non_delete_removed(
        self, db_conn: tuple[sa.engine.Connection, sa.Table]
    ) -> None:
        conn, vt = db_conn
        self.seed_versions(
            conn,
            transactions=[3],
            checkpoints=[3],
            version_rows=[
                (1, 3, 3, Operation.UPDATE),
                (2, 3, 4, Operation.UPDATE),
                (3, 3, None, Operation.UPDATE),
            ],
            version_table=vt,
        )
        _delete_immediately_invalidated(conn, vt)
        result = self.get_version_rows(conn, vt)
        assert (1, 3, 3, int(Operation.UPDATE)) not in result
        assert len(result) == 2

    def test_delete_marker_zero_interval_kept(
        self, db_conn: tuple[sa.engine.Connection, sa.Table]
    ) -> None:
        conn, vt = db_conn
        self.seed_versions(
            conn,
            transactions=[3],
            checkpoints=[3],
            version_rows=[(1, 3, 3, Operation.DELETE)],
            version_table=vt,
        )
        _delete_immediately_invalidated(conn, vt)
        assert len(self.get_version_rows(conn, vt)) == 1

    def test_no_records_match_is_noop(
        self, db_conn: tuple[sa.engine.Connection, sa.Table]
    ) -> None:
        conn, vt = db_conn
        self.seed_versions(
            conn,
            transactions=[1, 2],
            checkpoints=[2],
            version_rows=[(1, 1, None, Operation.INSERT)],
            version_table=vt,
        )
        _delete_immediately_invalidated(conn, vt)
        assert len(self.get_version_rows(conn, vt)) == 1


class TestDeleteUnusedTransactions(SquashTest):
    def test_unreferenced_transactions_deleted(
        self, db_conn: tuple[sa.engine.Connection, sa.Table]
    ) -> None:
        conn, vt = db_conn
        self.seed_versions(
            conn,
            transactions=[1, 2, 3, 4, 5],
            checkpoints=[5],
            version_rows=[(1, 3, 4, Operation.UPDATE)],
            version_table=vt,
        )
        _delete_unused_transactions(conn, [vt])
        assert self.get_all_tx_ids(conn, vt) == {3, 4, 5}

    def test_checkpoint_only_pin_keeps_transaction(
        self, db_conn: tuple[sa.engine.Connection, sa.Table]
    ) -> None:
        conn, vt = db_conn
        self.seed_versions(
            conn,
            transactions=[1, 2],
            checkpoints=[2],
            version_rows=[],
            version_table=vt,
        )
        _delete_unused_transactions(conn, [vt])
        assert 2 in self.get_all_tx_ids(conn, vt)
        assert 1 not in self.get_all_tx_ids(conn, vt)

    def test_version_only_pin_keeps_transaction(
        self, db_conn: tuple[sa.engine.Connection, sa.Table]
    ) -> None:
        conn, vt = db_conn
        self.seed_versions(
            conn,
            transactions=[1, 2],
            checkpoints=[],
            version_rows=[(1, 2, None, Operation.INSERT)],
            version_table=vt,
        )
        _delete_unused_transactions(conn, [vt])
        assert 2 in self.get_all_tx_ids(conn, vt)
        assert 1 not in self.get_all_tx_ids(conn, vt)

    def test_end_transaction_id_also_pins(
        self, db_conn: tuple[sa.engine.Connection, sa.Table]
    ) -> None:
        conn, vt = db_conn
        self.seed_versions(
            conn,
            transactions=[1, 2, 3],
            checkpoints=[],
            version_rows=[(1, 2, 3, Operation.UPDATE)],
            version_table=vt,
        )
        _delete_unused_transactions(conn, [vt])
        assert self.get_all_tx_ids(conn, vt) == {2, 3}

    def test_empty_version_tables_list_is_noop(
        self, db_conn: tuple[sa.engine.Connection, sa.Table]
    ) -> None:
        conn, vt = db_conn
        self.seed_versions(
            conn, transactions=[1, 2], checkpoints=[], version_rows=[], version_table=vt
        )
        _delete_unused_transactions(conn, [])
        assert self.get_all_tx_ids(conn, vt) == {1, 2}

    def test_multiple_version_tables_all_considered(
        self, db_conn: tuple[sa.engine.Connection, sa.Table]
    ) -> None:
        conn, vt = db_conn

        meta = vt.metadata
        vt2 = sa.Table(
            "squash_unit_test_version_2",
            meta,
            sa.Column("id", sa.Integer),
            sa.Column("transaction_id", sa.Integer, nullable=False),
            sa.Column("end_transaction_id", sa.Integer, nullable=True),
            sa.Column("operation_type", sa.Integer, nullable=False),
        )
        meta.create_all(conn.engine, tables=[vt2], checkfirst=True)

        self.seed_versions(
            conn,
            transactions=[1, 2, 3, 4],
            checkpoints=[],
            version_rows=[(1, 2, None, Operation.INSERT)],
            version_table=vt,
        )
        conn.execute(
            vt2.insert().values(
                id=1,
                transaction_id=3,
                end_transaction_id=None,
                operation_type=int(Operation.INSERT),
            )
        )
        conn.commit()

        _delete_unused_transactions(conn, [vt, vt2])
        remaining = self.get_all_tx_ids(conn, vt)
        assert 2 in remaining
        assert 3 in remaining
        assert 1 not in remaining
        assert 4 not in remaining


class TestSquashVersionRecords(SquashTest):
    def test_sqlite_dialect_is_skipped(
        self, db_conn: tuple[sa.engine.Connection, sa.Table]
    ) -> None:
        conn, vt = db_conn
        self.seed_versions(
            conn,
            transactions=[1, 2],
            checkpoints=[2],
            version_rows=[(1, 1, None, Operation.INSERT)],
            version_table=vt,
        )
        squash_version_records(conn, version_tables=[vt])
        assert len(self.get_version_rows(conn, vt)) == 1

    def test_insert_then_update_in_same_interval_yields_single_insert(
        self,
        db_conn: tuple[sa.engine.Connection, sa.Table],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        conn, vt = db_conn
        monkeypatch.setattr(conn.dialect, "name", "postgresql")
        self.seed_versions(
            conn,
            transactions=[1, 2, 3],
            checkpoints=[3],
            version_rows=[
                (1, 1, 2, Operation.INSERT),
                (1, 2, None, Operation.UPDATE),
            ],
            version_table=vt,
        )
        squash_version_records(conn, version_tables=[vt])
        assert self.get_version_rows(conn, vt) == [(1, 3, None, int(Operation.INSERT))]

    def test_insert_update_across_two_checkpoints_produces_insert_then_update(
        self,
        db_conn: tuple[sa.engine.Connection, sa.Table],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        conn, vt = db_conn
        monkeypatch.setattr(conn.dialect, "name", "postgresql")
        self.seed_versions(
            conn,
            transactions=[1, 2, 3, 4, 5],
            checkpoints=[3, 5],
            version_rows=[
                (1, 1, 2, Operation.INSERT),
                (1, 2, 4, Operation.UPDATE),  # superseded within cp1 interval
                (1, 4, None, Operation.UPDATE),  # cp2 update
            ],
            version_table=vt,
        )
        squash_version_records(conn, version_tables=[vt])
        rows = self.get_version_rows(conn, vt)
        # Phase-1 record: squashed INSERT+UPDATE -> INSERT at cp1, closed at cp2
        assert (1, 3, 5, int(Operation.INSERT)) in rows
        # Phase-2 record: genuine UPDATE, stays UPDATE at cp2
        assert (1, 5, None, int(Operation.UPDATE)) in rows
        assert len(rows) == 2

    def test_no_checkpoint_transactions_skipped(
        self,
        db_conn: tuple[sa.engine.Connection, sa.Table],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        conn, vt = db_conn
        meta = vt.metadata

        # Only override the `name` attribute so SQLAlchemy's query compiler
        # still has access to all other dialect internals (e.g. compiler_linting).
        monkeypatch.setattr(conn.dialect, "name", "postgresql")

        conn.execute(
            meta.tables["checkpoint"]
            .insert()
            .values(id=1, run__id=1, transaction__id=None, message="no tx")
        )
        conn.execute(meta.tables["transaction"].insert().values(id=1, issued_at=_NOW))
        conn.execute(
            vt.insert().values(
                id=1,
                transaction_id=1,
                end_transaction_id=None,
                operation_type=int(Operation.INSERT),
            )
        )
        conn.commit()

        squash_version_records(conn, version_tables=[vt])
        assert len(self.get_version_rows(conn, vt)) == 1
