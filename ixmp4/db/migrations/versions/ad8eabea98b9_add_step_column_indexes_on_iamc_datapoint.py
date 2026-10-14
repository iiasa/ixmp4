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
    # Composite step-column indexes: the (step, time_series__id) family helps
    # the datapoint<->timeseries join / count, and the (step, ..., id)
    # family enables sort-free paginated pages ordered by id.
    with op.batch_alter_table("iamc_datapoint_universal", schema=None) as batch_op:
        batch_op.create_index(
            "ix_iamc_datapoint_universal_step_year_time_series__id",
            ["step_year", "time_series__id"],
            unique=False,
        )
        batch_op.create_index(
            "ix_iamc_datapoint_universal_step_year_step_category_ts__id",
            ["step_year", "step_category", "time_series__id"],
            unique=False,
        )
        batch_op.create_index(
            "ix_iamc_datapoint_universal_step_category_time_series__id",
            ["step_category", "time_series__id"],
            unique=False,
        )
        batch_op.create_index(
            "ix_iamc_datapoint_universal_step_datetime_id",
            ["step_datetime", "id"],
            unique=False,
        )
        batch_op.create_index(
            "ix_iamc_datapoint_universal_step_datetime_time_series__id",
            ["step_datetime", "time_series__id"],
            unique=False,
        )
        batch_op.create_index(
            "ix_iamc_datapoint_universal_step_year_id",
            ["step_year", "id"],
            unique=False,
        )
        batch_op.create_index(
            "ix_iamc_datapoint_universal_step_year_step_category_id",
            ["step_year", "step_category", "id"],
            unique=False,
        )
        batch_op.create_index(
            "ix_iamc_datapoint_universal_step_category_id",
            ["step_category", "id"],
            unique=False,
        )


def downgrade():
    with op.batch_alter_table("iamc_datapoint_universal", schema=None) as batch_op:
        batch_op.drop_index("ix_iamc_datapoint_universal_step_category_id")
        batch_op.drop_index("ix_iamc_datapoint_universal_step_year_step_category_id")
        batch_op.drop_index("ix_iamc_datapoint_universal_step_year_id")
        batch_op.drop_index("ix_iamc_datapoint_universal_step_datetime_time_series__id")
        batch_op.drop_index("ix_iamc_datapoint_universal_step_datetime_id")
        batch_op.drop_index("ix_iamc_datapoint_universal_step_category_time_series__id")
        batch_op.drop_index("ix_iamc_datapoint_universal_step_year_step_category_ts__id")
        batch_op.drop_index("ix_iamc_datapoint_universal_step_year_time_series__id")
