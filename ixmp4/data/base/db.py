import sqlalchemy as sa
from sqlalchemy import orm
from toolkit.db.models import DeclarativeBase, HasConventionalMetadata
from toolkit.db.types import DateTime, IntegerId, String


class BaseModel(DeclarativeBase, HasConventionalMetadata):
    id: IntegerId = orm.mapped_column(info={"skip_autogenerate": True})


BaseModel.reset_metadata()


class HasCreationInfo:
    """Mixin for adding creation audit info to a model.
    The added fields `created_at`, `created_by` are automatically set
    by ixmp4's sqlalchemy event handlers for 'before_insert' and 'do_orm_execute'.
    """

    created_at: DateTime = orm.mapped_column(sa.DateTime(timezone=False), nullable=True)
    created_by: String = orm.mapped_column(sa.String(255), nullable=True)


class HasUpdateInfo(HasCreationInfo):
    """Mixin for adding update and creation audit info to a model.
    The added fields `updated_at`, `updated_by` are automatically set
    by ixmp4's sqlalchemy event handlers for 'before_update' and 'do_orm_execute'
    (in addition to the inherited fields from `HasCreationInfo`).
    """

    updated_at: DateTime = orm.mapped_column(sa.DateTime(timezone=False), nullable=True)
    updated_by: String = orm.mapped_column(sa.String(255), nullable=True)
