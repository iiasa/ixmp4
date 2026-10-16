# type: ignore
"""Add step-column index families on iamc_datapoint_universal

Revision ID: ad8eabea98b9
Revises: 87987b875e9b
Create Date: 2026-10-14 09:00:00.000000

"""

from alembic import op

# Revision identifiers, used by Alembic.
revision = "ad8eabea98b9"
down_revision = "87987b875e9b"
branch_labels = None
depends_on = None


def upgrade():
    concurrently_on_postgres = op.get_bind().dialect.name == "postgresql"
    # Composite step-column indexes: the (step, time_series__id) family helps
    # the datapoint<->timeseries join / count, and the (step, ..., id)
    # family enables sort-free paginated pages ordered by id.
    # CREATE INDEX CONCURRENTLY cannot run inside a transaction block, so on
    # PostgreSQL the statements must run in an autocommit block.
    with op.get_context().autocommit_block():
        with op.batch_alter_table("iamc_datapoint_universal", schema=None) as batch_op:
            batch_op.create_index(
                "ix_iamc_datapoint_universal_step_year_time_series__id",
                ["step_year", "time_series__id"],
                unique=False,
                postgresql_concurrently=concurrently_on_postgres,
            )
            batch_op.create_index(
                "ix_iamc_datapoint_universal_step_year_step_category_ts__id",
                ["step_year", "step_category", "time_series__id"],
                unique=False,
                postgresql_concurrently=concurrently_on_postgres,
            )
            batch_op.create_index(
                "ix_iamc_datapoint_universal_step_category_time_series__id",
                ["step_category", "time_series__id"],
                unique=False,
                postgresql_concurrently=concurrently_on_postgres,
            )
            batch_op.create_index(
                "ix_iamc_datapoint_universal_step_datetime_id",
                ["step_datetime", "id"],
                unique=False,
                postgresql_concurrently=concurrently_on_postgres,
            )
            batch_op.create_index(
                "ix_iamc_datapoint_universal_step_datetime_time_series__id",
                ["step_datetime", "time_series__id"],
                unique=False,
                postgresql_concurrently=concurrently_on_postgres,
            )
            batch_op.create_index(
                "ix_iamc_datapoint_universal_step_year_id",
                ["step_year", "id"],
                unique=False,
                postgresql_concurrently=concurrently_on_postgres,
            )
            batch_op.create_index(
                "ix_iamc_datapoint_universal_step_year_step_category_id",
                ["step_year", "step_category", "id"],
                unique=False,
                postgresql_concurrently=concurrently_on_postgres,
            )
            batch_op.create_index(
                "ix_iamc_datapoint_universal_step_category_id",
                ["step_category", "id"],
                unique=False,
                postgresql_concurrently=concurrently_on_postgres,
            )


def downgrade():
    concurrently_on_postgres = op.get_bind().dialect.name == "postgresql"
    # DROP INDEX CONCURRENTLY cannot run inside a transaction block, so on
    # PostgreSQL the statements must run in an autocommit block.
    with op.get_context().autocommit_block():
        with op.batch_alter_table("iamc_datapoint_universal", schema=None) as batch_op:
            batch_op.drop_index(
                "ix_iamc_datapoint_universal_step_category_id",
                postgresql_concurrently=concurrently_on_postgres,
            )
            batch_op.drop_index(
                "ix_iamc_datapoint_universal_step_year_step_category_id",
                postgresql_concurrently=concurrently_on_postgres,
            )
            batch_op.drop_index(
                "ix_iamc_datapoint_universal_step_year_id",
                postgresql_concurrently=concurrently_on_postgres,
            )
            batch_op.drop_index(
                "ix_iamc_datapoint_universal_step_datetime_time_series__id",
                postgresql_concurrently=concurrently_on_postgres,
            )
            batch_op.drop_index(
                "ix_iamc_datapoint_universal_step_datetime_id",
                postgresql_concurrently=concurrently_on_postgres,
            )
            batch_op.drop_index(
                "ix_iamc_datapoint_universal_step_category_time_series__id",
                postgresql_concurrently=concurrently_on_postgres,
            )
            batch_op.drop_index(
                "ix_iamc_datapoint_universal_step_year_step_category_ts__id",
                postgresql_concurrently=concurrently_on_postgres,
            )
            batch_op.drop_index(
                "ix_iamc_datapoint_universal_step_year_time_series__id",
                postgresql_concurrently=concurrently_on_postgres,
            )
