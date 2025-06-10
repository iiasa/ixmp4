from contextlib import contextmanager
from typing import Generator

from alembic import op
from sqlalchemy import text


@contextmanager
def pause_foreign_key_checks() -> Generator[None, None, None]:
    """
    Context manager to pause foreign key checks for the duration of the block.

    This is useful for performing operations that would otherwise violate
    foreign key constraints, such as dropping or altering tables.
    """
    context = op.get_context()

    if context.dialect.name == "sqlite":
        op.execute("PRAGMA foreign_keys = OFF;")
    elif context.dialect.name == "postgresql":
        # y i k e s
        tables = [
            row[0]
            for row in op.get_bind().execute(
                text("SELECT tablename FROM pg_tables WHERE schemaname = 'public';")
            )
        ]
        for table in tables:
            op.execute(f'ALTER TABLE "{table}" DISABLE TRIGGER ALL;')
    else:
        raise NotImplementedError(
            "Foreign key checks cannot be paused for dialect "
            f"'{op.get_context().dialect.name}'"
        )

    try:
        yield
    finally:
        if context.dialect.name == "sqlite":
            op.execute("PRAGMA foreign_keys = ON;")
        elif context.dialect.name == "postgresql":
            tables = [
                row[0]
                for row in op.get_bind().execute(
                    text("SELECT tablename FROM pg_tables WHERE schemaname = 'public';")
                )
            ]
            for table in tables:
                op.execute(f'ALTER TABLE "{table}" ENABLE TRIGGER ALL;')
