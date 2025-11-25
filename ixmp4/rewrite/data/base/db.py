from typing import TYPE_CHECKING

import sqlalchemy as sa
from sqlalchemy import orm
from toolkit import db

if TYPE_CHECKING:
    pass


class BaseModel(db.m.DeclarativeBase, db.m.HasConventionalMetadata):
    id: db.t.IntegerId = orm.mapped_column(info={"skip_autogenerate": True})


BaseModel.reset_metadata()


class HasCreationInfo:
    """Mixin for adding creation audit info to a model.
    The added fields `created_at`, `created_by` are automatically set
    by ixmp4's sqlalchemy event handlers for 'before_insert' and 'do_orm_execute'.
    """

    created_at: db.t.DateTime = orm.mapped_column(
        sa.DateTime(timezone=False), nullable=True
    )
    created_by: db.t.String = orm.mapped_column(sa.String(255), nullable=True)


class HasUpdateInfo(HasCreationInfo):
    """Mixin for adding update and creation audit info to a model.
    The added fields `updated_at`, `updated_by` are automatically set
    by ixmp4's sqlalchemy event handlers for 'before_update' and 'do_orm_execute'
    (in addition to the inherited fields from `HasCreationInfo`).
    """

    updated_at: db.t.DateTime = orm.mapped_column(
        sa.DateTime(timezone=False), nullable=True
    )
    updated_by: db.t.String = orm.mapped_column(sa.String(255), nullable=True)
