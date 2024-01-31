from logging.config import fileConfig

from alembic import context
from sqlalchemy import create_engine

from ixmp4.conf import settings
from ixmp4.data.db import BaseModel

# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config
dsn = config.get_main_option("sqlalchemy.url", settings.migration_db_uri)
dsn = dsn.replace("postgresql://", "postgresql+psycopg://")

# Interpret the config file for Python logging.
# This line sets up loggers basically.
assert config.config_file_name is not None
fileConfig(config.config_file_name)

target_metadata = BaseModel.metadata

# other values from the config, defined by the needs of env.py,
# can be acquired:
# my_important_option = config.get_main_option("my_important_option")
# ... etc.


def include_object(object, name, type_, reflected, compare_to):
    if (
        type_ == "column"
        and not reflected
        and object.info.get("skip_autogenerate", False)
    ):
        return False
    else:
        return True


def run_migrations_offline():
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


def run_migrations_online():
    """Run migrations in 'online' mode.

    In this scenario we need to create an Engine
    and associate a connection with the context.

    """
    connectable = create_engine(dsn)

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
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
