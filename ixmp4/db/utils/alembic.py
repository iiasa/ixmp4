from alembic import command
from alembic.config import Config
from alembic.migration import MigrationContext
from alembic.script import ScriptDirectory
from sqlalchemy import create_engine

alembic_config = Config("alembic.ini")


def get_database_revision(dsn: str) -> str | None:
    """Returns the current revision hash of a given database.
    To do this a connection to the database and a call to alembic are required."""

    engine = create_engine(dsn)
    conn = engine.connect()

    context = MigrationContext.configure(conn)
    return context.get_current_revision()


def get_head_revision() -> str | tuple[str, ...] | None:
    """Returns the revision hash of the newest migration
    in the alembic `versions/` directory."""

    script_directory = ScriptDirectory.from_config(alembic_config)
    return script_directory.as_revision_number("head")


def upgrade_database(dsn: str, revision: str = "head") -> None:
    """Uses alembic to upgrade the given database.
    Optionally a desired revision can be supplied instead
    of the default `head` (newest) revision."""

    alembic_config.set_main_option("sqlalchemy.url", dsn)
    command.upgrade(alembic_config, revision)


def stamp_database(dsn: str, revision: str) -> None:
    """Uses alembic to stamp the given database with the given revision.
    Only touches the `alembic_version` table, nothing else."""

    alembic_config.set_main_option("sqlalchemy.url", dsn)
    command.stamp(alembic_config, revision, purge=True)
