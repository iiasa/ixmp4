# type: ignore
"""Create postgres versioning triggers for optimization items

Revision ID: b188382206bd
Revises: 278574794665
Create Date: 2025-09-04 12:19:15.663554

"""

import logging

import sqlalchemy as sa
from alembic import op

from ixmp4.data.db.versions import PostgresVersionTriggers

logger = logging.getLogger(__name__)

# Revision identifiers, used by Alembic.
revision = "b188382206bd"
down_revision = "278574794665"
branch_labels = None
depends_on = None


transaction_table_name = "transaction"
tables = [
    ("optimization_scalar", "optimization_scalar_version"),
    ("optimization_indexset", "optimization_indexset_version"),
    ("optimization_idnexsetdata", "optimization_idnexsetdata_version"),
    ("optimization_parameter", "optimization_parameter_version"),
    (
        "optimization_parameterindexsetassociation",
        "optimization_parameterindexsetassociation_version",
    ),
    ("optimization_table", "optimization_table_version"),
    (
        "optimization_tableindexsetassociation",
        "optimization_tableindexsetassociation_version",
    ),
    ("optimization_equation", "optimization_equation_version"),
    (
        "optimization_equationindexsetassociation",
        "optimization_equationindexsetassociation_version",
    ),
    ("optimization_optimizationvariable", "optimization_optimizationvariable_version"),
    (
        "optimization_variableindexsetassociation",
        "optimization_variableindexsetassociation_version",
    ),
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
