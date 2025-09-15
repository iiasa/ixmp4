from datetime import datetime, timezone
from typing import TYPE_CHECKING

import sqlalchemy as sa
from sqlalchemy import orm
from toolkit import db

if TYPE_CHECKING:
    from ixmp4.data.auth.context import AuthorizationContext


class BaseModel(db.m.DeclarativeBase, db.m.HasConventionalMetadata):
    id: db.t.IntegerId


BaseModel.reset_metadata()


class HasCreationInfo:
    """Mixin for adding creation audit info to a model.
    The added fields `created_at`, `created_by` are automatically set
    by ixmp4's sqlalchemy event handlers for 'before_insert' and 'do_orm_execute'.
    """

    created_at: db.t.DateTime = orm.mapped_column(nullable=True)
    created_by: db.t.String = orm.mapped_column(sa.String(255), nullable=True)

    @staticmethod
    def get_username(auth_context: "AuthorizationContext | None") -> str:
        return auth_context.user.username if auth_context is not None else "@unknown"

    @staticmethod
    def get_timestamp() -> datetime:
        return datetime.now(tz=timezone.utc)

    def set_creation_info(self, auth_context: "AuthorizationContext | None") -> None:
        self.created_at = self.get_timestamp()
        self.created_by = self.get_username(auth_context)


class HasUpdateInfo(HasCreationInfo):
    """Mixin for adding update and creation audit info to a model.
    The added fields `updated_at`, `updated_by` are automatically set
    by ixmp4's sqlalchemy event handlers for 'before_update' and 'do_orm_execute'
    (in addition to the inherited fields from `HasCreationInfo`).
    """

    updated_at: db.t.DateTime = orm.mapped_column(nullable=True)
    updated_by: db.t.String = orm.mapped_column(sa.String(255), nullable=True)

    def set_update_info(self, auth_context: "AuthorizationContext | None") -> None:
        self.updated_at = self.get_timestamp()
        self.updated_by = self.get_username(auth_context)
