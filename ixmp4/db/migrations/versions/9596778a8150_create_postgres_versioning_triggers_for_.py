# type: ignore
"""Create postgres versioning triggers for optimization items

Revision ID: 9596778a8150
Revises: e189ffe6a65e
Create Date: 2025-09-22 18:57:11.623283

"""

import logging

import sqlalchemy as sa
from alembic import op

from ixmp4.data.db.versions import PostgresVersionTriggers

logger = logging.getLogger(__name__)

# Revision identifiers, used by Alembic.
revision = "9596778a8150"
down_revision = "e189ffe6a65e"
branch_labels = None
depends_on = None


transaction_table_name = "transaction"
tables = [
    ("opt_sca", "opt_sca_version"),
    ("opt_idx", "opt_idx_version"),
    ("opt_idx_data", "opt_idx_data_version"),
    ("opt_par", "opt_par_version"),
    ("opt_par_idx_association", "opt_par_idx_association_version"),
    ("opt_tab", "opt_tab_version"),
    ("opt_tab_idx_association", "opt_tab_idx_association_version"),
    ("opt_equ", "opt_equ_version"),
    ("opt_equ_idx_association", "opt_equ_idx_association_version"),
    ("opt_var", "opt_var_version"),
    ("opt_var_idx_association", "opt_var_idx_association_version"),
]


def make_versioning_triggers(
    table_name: str, version_table_name: str, transaction_table: sa.Table
):
    conn = op.get_bind()

    version_table = sa.Table(
        version_table_name,
        sa.MetaData(),
        autoload_with=conn,
    )

    data_table = sa.Table(
        table_name,
        sa.MetaData(),
        autoload_with=conn,
    )
    return PostgresVersionTriggers(data_table, version_table, transaction_table)


def upgrade():
    conn = op.get_bind()
    if conn is None:
        logging.warning("Cannot create version triggers without database connection.")
        return

    dialect_name = conn.dialect.name
    if dialect_name != "postgresql":
        logging.info(
            "Skipping creation of version triggers for "
            f"database dialect '{dialect_name}'."
        )
        return

    transaction_table = sa.Table(
        transaction_table_name,
        sa.MetaData(),
        autoload_with=conn,
    )
    for table, version_table in tables:
        triggers = make_versioning_triggers(table, version_table, transaction_table)
        triggers.create_entities(conn)


def downgrade():
    conn = op.get_bind()
    if conn is None:
        logging.warning("Cannot create version triggers without database connection.")
        return

    dialect_name = conn.dialect.name
    if dialect_name != "postgresql":
        logging.info(
            "Skipping dropping of version triggers for "
            f"database dialect '{dialect_name}'."
        )
        return

    transaction_table = sa.Table(
        transaction_table_name,
        sa.MetaData(),
        autoload_with=conn,
    )
    for table, version_table in tables:
        triggers = make_versioning_triggers(table, version_table, transaction_table)
        triggers.drop_entities(conn)
