"""Squash version records so they only reference checkpoint transactions."""

import logging
from typing import Any

import sqlalchemy as sa

from ixmp4.data.checkpoint.db import Checkpoint

from .model import Operation
from .transaction import Transaction

logger = logging.getLogger(__name__)


def _get_version_tables() -> list[sa.Table]:
    """Return all version table names from the PostgresVersionTriggers registry."""
    # load all the models so _registry is populated
    import ixmp4.db.models  # noqa: F401
    from ixmp4.data.versions import PostgresVersionTriggers

    return [t.version_table for t in PostgresVersionTriggers._registry.values()]


def _covering_checkpoint_tx(
    outer: sa.TableClause,
    tx_col: sa.ColumnClause[int],
) -> sa.ScalarSelect[int]:
    """Correlated scalar subquery: the lowest checkpoint transaction__id >= tx_col."""
    return (
        sa.select(sa.func.min(Checkpoint.transaction__id))
        .where(Checkpoint.transaction__id.isnot(None))
        .where(Checkpoint.transaction__id >= tx_col)
        .correlate(outer)
        .scalar_subquery()
    )


def _delete_superseded(
    conn: sa.engine.Connection, v: sa.TableClause
) -> sa.CursorResult[Any]:
    """Delete version records superseded by another in the same checkpoint interval."""
    v2 = v.alias("v2")
    c2 = sa.table("checkpoint", sa.column("transaction__id")).alias("c2")

    covering_tx = _covering_checkpoint_tx(v, v.c.transaction_id)

    # True when no checkpoint falls between v.transaction_id and v2.transaction_id,
    # i.e. v and v2 belong to the same checkpoint interval.
    no_cp_between = (
        sa.select(sa.literal(1))
        .where(c2.c.transaction__id.isnot(None))
        .where(c2.c.transaction__id >= v.c.transaction_id)
        .where(c2.c.transaction__id < v2.c.transaction_id)
        .correlate(v, v2)
        .exists()
    )

    winner_exists = (
        sa.select(sa.literal(1))
        .where(v2.c.id == v.c.id)
        .where(v2.c.transaction_id > v.c.transaction_id)
        .where(v2.c.transaction_id <= covering_tx)
        .where(~no_cp_between)
        .correlate(v)
        .exists()
    )

    return conn.execute(
        sa.delete(v).where(covering_tx.isnot(None)).where(winner_exists)
    )


def _delete_transient(
    conn: sa.engine.Connection, v: sa.TableClause
) -> sa.CursorResult[Any]:
    """Delete records whose entire lifespan falls within a single checkpoint interval
    so the record is not accessible via any checkpoints."""
    covering_start = _covering_checkpoint_tx(v, v.c.transaction_id)
    covering_end = _covering_checkpoint_tx(v, v.c.end_transaction_id)

    return conn.execute(
        sa.delete(v)
        .where(v.c.end_transaction_id.isnot(None))
        .where(v.c.operation_type != int(Operation.DELETE))
        .where(covering_start.isnot(None))
        .where(covering_start == covering_end)
    )


def _remap_transaction_id(
    conn: sa.engine.Connection, v: sa.TableClause
) -> sa.CursorResult[Any]:
    """Remap transaction_id to the id of the covering checkpoint transaction."""
    covering_tx = _covering_checkpoint_tx(v, v.c.transaction_id)

    return conn.execute(
        sa.update(v)
        .values(transaction_id=covering_tx)
        .where(covering_tx.isnot(None))
        .where(covering_tx != v.c.transaction_id)
    )


def _remap_end_transaction_id(
    conn: sa.engine.Connection, v: sa.TableClause
) -> sa.CursorResult[Any]:
    """Remap end_transaction_id to the id of the covering checkpoint transaction."""
    covering_tx = _covering_checkpoint_tx(v, v.c.end_transaction_id)

    return conn.execute(
        sa.update(v)
        .values(end_transaction_id=covering_tx)
        .where(v.c.end_transaction_id.isnot(None))
        .where(covering_tx.isnot(None))
        .where(covering_tx != v.c.end_transaction_id)
    )


def _promote_orphaned_updates_to_inserts(
    conn: sa.engine.Connection, v: sa.TableClause
) -> sa.CursorResult[Any]:
    """Promote UPDATE records to INSERT when they have no preceding version record."""
    v2 = v.alias("v2")
    predecessor_exists = (
        sa.select(sa.literal(1))
        .where(v2.c.id == v.c.id)
        .where(v2.c.end_transaction_id == v.c.transaction_id)
        .correlate(v)
        .exists()
    )
    return conn.execute(
        sa.update(v)
        .values(operation_type=int(Operation.INSERT))
        .where(v.c.operation_type == int(Operation.UPDATE))
        .where(~predecessor_exists)
    )


def _delete_immediately_invalidated(
    conn: sa.engine.Connection, v: sa.TableClause
) -> sa.CursorResult[Any]:
    """Remove non-DELETE records where transaction_id == end_transaction_id."""
    return conn.execute(
        sa.delete(v)
        .where(v.c.end_transaction_id.isnot(None))
        .where(v.c.transaction_id == v.c.end_transaction_id)
        .where(v.c.operation_type != 2)
    )


def _squash_table(conn: sa.engine.Connection, vtable: sa.TableClause) -> None:
    """Squash a single version table in-place."""
    ds_res = _delete_superseded(conn, vtable)
    logger.info("   Deleted %s superseded records.", ds_res.rowcount)
    dt_res = _delete_transient(conn, vtable)
    logger.info("   Deleted %s transient records.", dt_res.rowcount)
    rmtx_res = _remap_transaction_id(conn, vtable)
    logger.info("   Remapped %s transaction_ids.", rmtx_res.rowcount)
    rmetx_res = _remap_end_transaction_id(conn, vtable)
    logger.info("   Remapped %s end_transaction_ids.", rmetx_res.rowcount)
    poui_res = _promote_orphaned_updates_to_inserts(conn, vtable)
    logger.info(
        "   Promoted %s orphaned UPDATE record(s) to INSERT.", poui_res.rowcount
    )
    dii_res = _delete_immediately_invalidated(conn, vtable)
    logger.info("   Deleted %s immediately invalidated records.", dii_res.rowcount)


def _delete_unused_transactions(
    conn: sa.engine.Connection, version_tables: list[sa.Table]
) -> None:
    """Delete transaction rows that are no longer referenced by any
    version table or checkpoint."""
    if not version_tables:
        return

    # Collect every transaction id still referenced by any version table.
    ref_selects: list[sa.Select[tuple[int]]] = []
    for t in version_tables:
        ref_selects.append(sa.select(t.c.transaction_id.label("id")))
        ref_selects.append(
            sa.select(t.c.end_transaction_id.label("id")).where(
                t.c.end_transaction_id.isnot(None)
            )
        )

    refs = sa.union_all(*ref_selects).subquery("_refs")
    referenced_ids = sa.select(refs.c.id).distinct().scalar_subquery()
    checkpoint_ids = (
        sa.select(Checkpoint.transaction__id)
        .where(Checkpoint.transaction__id.isnot(None))
        .scalar_subquery()
    )

    result = conn.execute(
        sa.delete(Transaction)
        .where(Transaction.id.notin_(referenced_ids))
        .where(Transaction.id.notin_(checkpoint_ids))
    )
    logger.info("Deleted %d unused transaction row(s).", result.rowcount)


def squash_version_records(
    conn: sa.engine.Connection,
    version_tables: list[sa.Table] | None = None,
    delete_unused_transactions: bool = True,
) -> None:
    """Squash all version records so they only reference checkpoint transactions.

    Parameters
    ----------
    conn :
        An active SQLAlchemy connection to a PostgreSQL database.
    version_tables :
        Override the list of version tables to process. Defaults to
        all version tables collected through the registry in the trigger class.
    delete_unused_transactions :
        Whether to delete all transactions not referenced by checkpoints or
        version tables. Default: ``True``
    """
    if conn.dialect.name != "postgresql":
        logger.info(
            "Skipping version-record squash for database dialect '%s'.",
            conn.dialect.name,
        )
        return

    tables = version_tables if version_tables is not None else _get_version_tables()

    count = conn.execute(
        sa.select(sa.func.count())
        .select_from(Checkpoint)
        .where(Checkpoint.transaction__id.isnot(None))
    ).scalar()
    if not count:
        logger.info("No checkpoint transactions found; nothing to squash.")
        return

    for vtable in tables:
        logger.info("Squashing version records in '%s'.", vtable.name)
        _squash_table(conn, vtable)

    if delete_unused_transactions:
        logger.info("Deleting unused transactions.")
        _delete_unused_transactions(conn, tables)
    logger.info("Version-record squash complete.")
