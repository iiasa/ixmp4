# type: ignore
"""make iamc_datapoint.value not nullable

Revision ID: 87987b875e9b
Revises: 8b0797ebf42f
Create Date: 2026-02-15 15:42:41.850785

"""

import sqlalchemy as sa
from alembic import op

# Revision identifiers, used by Alembic.
revision = "87987b875e9b"
down_revision = "8b0797ebf42f"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("iamc_datapoint_universal", schema=None) as batch_op:
        batch_op.alter_column("value", existing_type=sa.FLOAT(), nullable=False)


def downgrade():
    with op.batch_alter_table("iamc_datapoint_universal", schema=None) as batch_op:
        batch_op.alter_column("value", existing_type=sa.FLOAT(), nullable=True)
