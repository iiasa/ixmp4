# type: ignore
"""alter version table column nullability

Revision ID: ad25c5b542c4
Revises: 84b534bbc858
"""

import sqlalchemy as sa
from alembic import op

revision = "ad25c5b542c4"
down_revision = "84b534bbc858"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("checkpoint", schema=None) as batch_op:
        batch_op.alter_column(
            "transaction__id", existing_type=sa.INTEGER(), nullable=True
        )

    with op.batch_alter_table(
        "iamc_datapoint_universal_version", schema=None
    ) as batch_op:
        batch_op.alter_column(
            "time_series__id", existing_type=sa.INTEGER(), nullable=False
        )
        batch_op.alter_column(
            "type", existing_type=sa.VARCHAR(length=255), nullable=False
        )

    with op.batch_alter_table("iamc_measurand_version", schema=None) as batch_op:
        batch_op.alter_column(
            "variable__id", existing_type=sa.INTEGER(), nullable=False
        )
        batch_op.alter_column("unit__id", existing_type=sa.INTEGER(), nullable=False)

    with op.batch_alter_table("iamc_timeseries_version", schema=None) as batch_op:
        batch_op.alter_column("region__id", existing_type=sa.INTEGER(), nullable=False)
        batch_op.alter_column(
            "measurand__id", existing_type=sa.INTEGER(), nullable=False
        )
        batch_op.alter_column("run__id", existing_type=sa.INTEGER(), nullable=False)

    with op.batch_alter_table("iamc_variable_version", schema=None) as batch_op:
        batch_op.alter_column(
            "name", existing_type=sa.VARCHAR(length=255), nullable=False
        )

    with op.batch_alter_table("model_version", schema=None) as batch_op:
        batch_op.alter_column(
            "name", existing_type=sa.VARCHAR(length=255), nullable=False
        )

    with op.batch_alter_table("region_version", schema=None) as batch_op:
        batch_op.alter_column(
            "name", existing_type=sa.VARCHAR(length=255), nullable=False
        )
        batch_op.alter_column(
            "hierarchy", existing_type=sa.VARCHAR(length=1023), nullable=False
        )

    with op.batch_alter_table("run_version", schema=None) as batch_op:
        batch_op.alter_column("model__id", existing_type=sa.INTEGER(), nullable=False)
        batch_op.alter_column(
            "scenario__id", existing_type=sa.INTEGER(), nullable=False
        )
        batch_op.alter_column("version", existing_type=sa.INTEGER(), nullable=False)
        batch_op.alter_column("is_default", existing_type=sa.BOOLEAN(), nullable=False)

    with op.batch_alter_table("runmetaentry_version", schema=None) as batch_op:
        batch_op.alter_column("run__id", existing_type=sa.INTEGER(), nullable=False)
        batch_op.alter_column(
            "key", existing_type=sa.VARCHAR(length=1023), nullable=False
        )
        batch_op.alter_column(
            "dtype", existing_type=sa.VARCHAR(length=20), nullable=False
        )

    with op.batch_alter_table("scenario_version", schema=None) as batch_op:
        batch_op.alter_column(
            "name", existing_type=sa.VARCHAR(length=255), nullable=False
        )

    with op.batch_alter_table("transaction", schema=None) as batch_op:
        batch_op.alter_column("issued_at", existing_type=sa.DateTime(), nullable=False)
        batch_op.drop_column("remote_addr")

    with op.batch_alter_table("unit_version", schema=None) as batch_op:
        batch_op.alter_column(
            "name", existing_type=sa.VARCHAR(length=255), nullable=False
        )


def downgrade():
    with op.batch_alter_table("unit_version", schema=None) as batch_op:
        batch_op.alter_column(
            "name", existing_type=sa.VARCHAR(length=255), nullable=True
        )

    with op.batch_alter_table("transaction", schema=None) as batch_op:
        batch_op.add_column(
            sa.Column(
                "remote_addr", sa.VARCHAR(length=50), autoincrement=False, nullable=True
            )
        )
        batch_op.alter_column("issued_at", existing_type=sa.DateTime(), nullable=True)

    with op.batch_alter_table("scenario_version", schema=None) as batch_op:
        batch_op.alter_column(
            "name", existing_type=sa.VARCHAR(length=255), nullable=True
        )

    with op.batch_alter_table("runmetaentry_version", schema=None) as batch_op:
        batch_op.alter_column(
            "dtype", existing_type=sa.VARCHAR(length=20), nullable=True
        )
        batch_op.alter_column(
            "key", existing_type=sa.VARCHAR(length=1023), nullable=True
        )
        batch_op.alter_column("run__id", existing_type=sa.INTEGER(), nullable=True)

    with op.batch_alter_table("run_version", schema=None) as batch_op:
        batch_op.alter_column("is_default", existing_type=sa.BOOLEAN(), nullable=True)
        batch_op.alter_column("version", existing_type=sa.INTEGER(), nullable=True)
        batch_op.alter_column("scenario__id", existing_type=sa.INTEGER(), nullable=True)
        batch_op.alter_column("model__id", existing_type=sa.INTEGER(), nullable=True)

    with op.batch_alter_table("region_version", schema=None) as batch_op:
        batch_op.alter_column(
            "hierarchy", existing_type=sa.VARCHAR(length=1023), nullable=True
        )
        batch_op.alter_column(
            "name", existing_type=sa.VARCHAR(length=255), nullable=True
        )

    with op.batch_alter_table("model_version", schema=None) as batch_op:
        batch_op.alter_column(
            "name", existing_type=sa.VARCHAR(length=255), nullable=True
        )

    with op.batch_alter_table("iamc_variable_version", schema=None) as batch_op:
        batch_op.alter_column(
            "name", existing_type=sa.VARCHAR(length=255), nullable=True
        )

    with op.batch_alter_table("iamc_timeseries_version", schema=None) as batch_op:
        batch_op.alter_column("run__id", existing_type=sa.INTEGER(), nullable=True)
        batch_op.alter_column(
            "measurand__id", existing_type=sa.INTEGER(), nullable=True
        )
        batch_op.alter_column("region__id", existing_type=sa.INTEGER(), nullable=True)

    with op.batch_alter_table("iamc_measurand_version", schema=None) as batch_op:
        batch_op.alter_column("unit__id", existing_type=sa.INTEGER(), nullable=True)
        batch_op.alter_column("variable__id", existing_type=sa.INTEGER(), nullable=True)

    with op.batch_alter_table(
        "iamc_datapoint_universal_version", schema=None
    ) as batch_op:
        batch_op.alter_column(
            "type", existing_type=sa.VARCHAR(length=255), nullable=True
        )
        batch_op.alter_column(
            "time_series__id", existing_type=sa.INTEGER(), nullable=True
        )

    with op.batch_alter_table("checkpoint", schema=None) as batch_op:
        batch_op.alter_column(
            "transaction__id", existing_type=sa.INTEGER(), nullable=False
        )
