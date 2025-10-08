# type: ignore
"""create postgres versioning triggers

Revision ID: fbda0eb90f37
Revises: ad25c5b542c4
Create Date: 2025-08-10 01:30:07.294959

"""

import logging

import sqlalchemy as sa
from alembic import op

from ixmp4.data.db.versions import PostgresVersionTriggers

logger = logging.getLogger(__name__)
# Revision identifiers, used by Alembic.
revision = "fbda0eb90f37"
down_revision = "ad25c5b542c4"
branch_labels = None
depends_on = None

transaction_table_name = "transaction"
tables = [
    ("iamc_datapoint_universal", "iamc_datapoint_universal_version"),
    ("iamc_measurand", "iamc_measurand_version"),
    ("iamc_timeseries", "iamc_timeseries_version"),
    ("iamc_variable", "iamc_variable_version"),
    ("model", "model_version"),
    ("region", "region_version"),
    ("run", "run_version"),
    ("runmetaentry", "runmetaentry_version"),
    ("scenario", "scenario_version"),
    ("unit", "unit_version"),
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
