"""
When making schema migrations for versioned tables
you need to remember to call `sync_trigger` in order
to keep the version trigger up-to-date.

```
from alembic import context

if not context.is_offline_mode():
    conn = context.get_bind()
    sync_trigger(conn, "my_table_version")
```
"""

from typing import Literal

from alembic import context
from sqlalchemy import Engine, create_engine
from sqlalchemy.orm import configure_mappers
from sqlalchemy.schema import SchemaItem

from ixmp4.conf import settings
from ixmp4.data.db import BaseModel

# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config
dsn = config.get_main_option("sqlalchemy.url", settings.migration_db_uri)
dsn = dsn.replace("postgresql://", "postgresql+psycopg://")

configure_mappers()

target_metadata = BaseModel.metadata

# other values from the config, defined by the needs of env.py,
# can be acquired:
# my_important_option = config.get_main_option("my_important_option")
# ... etc.


def include_object(
    obj: SchemaItem,
    name: str | None,
    type_: Literal[
        "schema",
        "table",
        "column",
        "index",
        "unique_constraint",
        "foreign_key_constraint",
    ],
    reflected: bool,
    compare_to: SchemaItem | None,
) -> bool:
    if type_ == "column" and not reflected and obj.info.get("skip_autogenerate", False):
        return False
    else:
        return True


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode.

    This configures the context with just a URL
    and not an Engine, though an Engine is acceptable
    here as well.  By skipping the Engine creation
    we don't even need a DBAPI to be available.

    Calls to context.execute() here emit the given string to the
    script output.

    """
    context.configure(
        url=dsn,
        target_metadata=target_metadata,
        compare_type=True,
        literal_binds=True,
        render_as_batch=True,
        include_object=include_object,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode.

    In this scenario we need to create an Engine
    and associate a connection with the context.

    """
    engine: Engine | None = context.config.attributes.get("connection", None)
    if engine is None:
        engine = create_engine(dsn, max_identifier_length=63)

    with engine.connect() as conn:
        context.configure(
            connection=conn,
            target_metadata=target_metadata,
            compare_type=True,
            render_as_batch=True,
            include_object=include_object,
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
