from datetime import datetime, timezone
from typing import TYPE_CHECKING

from ixmp4 import db
from ixmp4.data import types

if TYPE_CHECKING:
    from ixmp4.data.auth.context import AuthorizationContext


class HasCreationInfo:
    """Mixin for adding creation audit info to a model.
    The added fields `created_at`, `created_by` are automatically set
    by ixmp4's sqlalchemy event handlers for 'before_insert' and 'do_orm_execute'.
    """

    created_at: types.DateTime = db.Column(db.DateTime, nullable=True)
    created_by: types.Username

    @staticmethod
    def get_username(auth_context: "AuthorizationContext | None"):
        if auth_context is not None:
            return auth_context.user.username
        else:
            return "@unknown"

    @staticmethod
    def get_timestamp():
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

    updated_at: types.DateTime = db.Column(db.DateTime, nullable=True)
    updated_by: types.Username

    def set_update_info(self, auth_context: "AuthorizationContext | None") -> None:
        self.updated_at = self.get_timestamp()
        self.updated_by = self.get_username(auth_context)
