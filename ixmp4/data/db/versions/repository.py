from typing import Any, TypeVar

from ixmp4 import db

from .. import base
from .model import DefaultVersionModel, Operation

VModelType = TypeVar("VModelType", bound=DefaultVersionModel)


class VersionRepository(
    base.Retriever[VModelType],
    base.Enumerator[VModelType],
):
    model_class: type[VModelType]

    def where_matches_kwargs(
        self, exc: db.sql.Select[Any], **kwargs: Any
    ) -> db.sql.Select[Any]:
        for key in kwargs:
            columns = db.utils.get_columns(self.model_class)
            if key in columns:
                exc = exc.where(columns[key] == kwargs[key])
        return exc

    def where_valid_at_transaction(
        self,
        exc: db.sql.Select[Any],
        transaction__id: int,
        cls: type[DefaultVersionModel] | None = None,
    ) -> db.sql.Select[Any]:
        if cls is None:
            cls = self.model_class
        return exc.where(
            db.and_(
                cls.transaction_id <= transaction__id,
                cls.operation_type != Operation.DELETE,
                db.or_(
                    cls.end_transaction_id > transaction__id,
                    cls.end_transaction_id == db.null(),
                ),
            )
        )

    def select(
        self, transaction__id: int | None = None, **kwargs: Any
    ) -> db.sql.Select[Any]:
        exc = db.select(self.model_class)

        if transaction__id is not None:
            exc = self.where_valid_at_transaction(exc, transaction__id)

        exc = self.where_matches_kwargs(exc, **kwargs)

        return exc.order_by(self.model_class.id.asc())
