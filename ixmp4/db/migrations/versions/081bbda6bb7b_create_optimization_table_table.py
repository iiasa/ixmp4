# type: ignore
"""Create optimization_table table

Revision ID: 081bbda6bb7b
Revises: 97ba231770e2
Create Date: 2024-04-12 10:00:13.662611

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# Revision identifiers, used by Alembic.
revision = "081bbda6bb7b"
down_revision = "97ba231770e2"
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table(
        "optimization_table",
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column(
            "data",
            sa.JSON().with_variant(
                postgresql.JSONB(astext_type=sa.Text()), "postgresql"
            ),
            nullable=False,
        ),
        sa.Column("run__id", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=True),
        sa.Column("created_by", sa.String(length=255), nullable=True),
        sa.Column(
            "id",
            sa.Integer(),
            sa.Identity(always=False, on_null=True, start=1, increment=1),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["run__id"], ["run.id"], name=op.f("fk_optimization_table_run__id_run")
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_optimization_table")),
        sa.UniqueConstraint(
            "name", "run__id", name=op.f("uq_optimization_table_name_run__id")
        ),
    )
    with op.batch_alter_table("optimization_table", schema=None) as batch_op:
        batch_op.create_index(
            batch_op.f("ix_optimization_table_run__id"), ["run__id"], unique=False
        )

    op.create_table(
        "optimization_column",
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("dtype", sa.String(length=255), nullable=False),
        sa.Column("table__id", sa.Integer(), nullable=False),
        sa.Column("constrained_to_indexset", sa.Integer(), nullable=False),
        sa.Column("unique", sa.Boolean(), nullable=False),
        sa.Column(
            "id",
            sa.Integer(),
            sa.Identity(always=False, on_null=True, start=1, increment=1),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["constrained_to_indexset"],
            ["optimization_indexset.id"],
            name=op.f("fk_optimization_column_constrained_to_indexset_optimiza_8432"),
        ),
        sa.ForeignKeyConstraint(
            ["table__id"],
            ["optimization_table.id"],
            name=op.f("fk_optimization_column_table__id_optimization_table"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_optimization_column")),
        sa.UniqueConstraint(
            "name", "table__id", name=op.f("uq_optimization_column_name_table__id")
        ),
    )
    with op.batch_alter_table("optimization_column", schema=None) as batch_op:
        batch_op.create_index(
            batch_op.f("ix_optimization_column_constrained_to_indexset"),
            ["constrained_to_indexset"],
            unique=False,
        )
        batch_op.create_index(
            batch_op.f("ix_optimization_column_table__id"), ["table__id"], unique=False
        )

    op.create_table(
        "optimization_table_docs",
        sa.Column("description", sa.Text(), nullable=False),
        sa.Column("dimension__id", sa.Integer(), nullable=True),
        sa.Column(
            "id",
            sa.Integer(),
            sa.Identity(always=False, on_null=True, start=1, increment=1),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["dimension__id"],
            ["optimization_table.id"],
            name=op.f("fk_optimization_table_docs_dimension__id_optimization_table"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_optimization_table_docs")),
        sa.UniqueConstraint(
            "dimension__id", name=op.f("uq_optimization_table_docs_dimension__id")
        ),
    )
    op.create_table(
        "optimization_column_docs",
        sa.Column("description", sa.Text(), nullable=False),
        sa.Column("dimension__id", sa.Integer(), nullable=True),
        sa.Column(
            "id",
            sa.Integer(),
            sa.Identity(always=False, on_null=True, start=1, increment=1),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["dimension__id"],
            ["optimization_column.id"],
            name=op.f("fk_optimization_column_docs_dimension__id_optimization_column"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_optimization_column_docs")),
        sa.UniqueConstraint(
            "dimension__id", name=op.f("uq_optimization_column_docs_dimension__id")
        ),
    )
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table("optimization_column_docs")
    op.drop_table("optimization_table_docs")
    with op.batch_alter_table("optimization_column", schema=None) as batch_op:
        batch_op.drop_index(batch_op.f("ix_optimization_column_table__id"))
        batch_op.drop_index(
            batch_op.f("ix_optimization_column_constrained_to_indexset")
        )

    op.drop_table("optimization_column")
    with op.batch_alter_table("optimization_table", schema=None) as batch_op:
        batch_op.drop_index(batch_op.f("ix_optimization_table_run__id"))

    op.drop_table("optimization_table")
    # ### end Alembic commands ###
