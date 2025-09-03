from typing import Any, Literal, TypeVar

import pandas as pd

from ixmp4 import db

from .. import base
from .model import (
    DefaultVersionModel,
    NamedVersionModel,
    Operation,
    RunLinkedVersionModel,
)

VModelType = TypeVar("VModelType", bound=DefaultVersionModel)
NamedVModelType = TypeVar("NamedVModelType", bound=NamedVersionModel)
RunLinkedVModelType = TypeVar("RunLinkedVModelType", bound=RunLinkedVersionModel)

SelectType = TypeVar("SelectType", bound=tuple[DefaultVersionModel, ...])


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

    def where_recorded_after_transaction(
        self, exc: db.sql.Select[SelectType], transaction__id: int
    ) -> db.sql.Select[SelectType]:
        return exc.where(self.model_class.transaction_id > transaction__id)

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

    # NOTE This is only inside the class because the where...() functions are inside a
    # class
    # TODO Could you extract that by requiring a vclass/cls argument?
    def _apply_transaction__id(
        self,
        exc: db.sql.Select[Any],
        vclass: type[DefaultVersionModel],
        transaction__id: int | None,
        valid: Literal["at_transaction", "after_transaction"] = "at_transaction",
    ) -> db.sql.Select[Any]:
        _exc = exc

        if transaction__id is not None:
            match valid:
                case "at_transaction":
                    _exc = self.where_valid_at_transaction(
                        _exc, transaction__id, vclass
                    )
                case "after_transaction":
                    _exc = self.where_recorded_after_transaction(_exc, transaction__id)

        return _exc

    def select(
        self,
        transaction__id: int | None = None,
        valid: Literal["at_transaction", "after_transaction"] = "at_transaction",
        **kwargs: Any,
    ) -> db.sql.Select[Any]:
        exc = db.select(self.model_class)

        exc = self._apply_transaction__id(
            exc=exc,
            vclass=self.model_class,
            transaction__id=transaction__id,
            valid=valid,
        )

        exc = self.where_matches_kwargs(exc, **kwargs)

        return exc.order_by(self.model_class.id.asc())

    def tabulate(
        self,
        *args: Any,
        _raw: bool = False,
        valid: Literal["at_transaction", "after_transaction"] = "at_transaction",
        **kwargs: Any,
    ) -> pd.DataFrame:
        return super().tabulate(*args, _raw=_raw, valid=valid, **kwargs)


# NOTE We could likely adapt `select` for this, but would then also need to safeguard/
# test against misuse, so I'm hoping this is less maintenance burden
class NamedVersionRepository(
    VersionRepository[NamedVModelType],
    base.NamedSelecter[NamedVModelType],
):
    model_class: type[NamedVModelType]

    def _select_for_id_map(
        self, transaction__id: int | None = None
    ) -> db.sql.Select[Any]:
        exc = super()._select_for_id_map()
        return self._apply_transaction__id(
            exc=exc, vclass=self.model_class, transaction__id=transaction__id
        )


class RunLinkedVersionRepository(
    VersionRepository[RunLinkedVModelType],
    base.RunLinkedSelecter[RunLinkedVModelType],
):
    model_class: type[RunLinkedVModelType]

    def _select_for_id_map(
        self, run__id: int, transaction__id: int | None = None
    ) -> db.sql.Select[Any]:
        exc = super()._select_for_id_map(run__id=run__id)
        return self._apply_transaction__id(
            exc=exc, vclass=self.model_class, transaction__id=transaction__id
        )
