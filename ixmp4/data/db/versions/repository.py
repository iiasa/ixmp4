from typing import Any, Literal, TypeVar

import pandas as pd

from ixmp4 import db
from ixmp4.db.utils.revert import apply_transaction__id

from .. import base
from .model import DefaultVersionModel

VModelType = TypeVar("VModelType", bound=DefaultVersionModel)


class VersionRepository(
    base.Retriever[VModelType],
    base.Enumerator[VModelType],
):
    model_class: type[VModelType]

    def select(
        self,
        transaction__id: int | None = None,
        valid: Literal["at_transaction", "after_transaction"] = "at_transaction",
        **kwargs: Any,
    ) -> db.sql.Select[Any]:
        exc = db.select(self.model_class)

        exc = apply_transaction__id(
            exc=exc,
            model_class=self.model_class,
            transaction__id=transaction__id,
            valid=valid,
        )

        exc = db.utils.where_matches_kwargs(exc, model_class=self.model_class, **kwargs)

        return exc.order_by(self.model_class.id.asc())

    def tabulate(
        self,
        *args: Any,
        _raw: bool = False,
        valid: Literal["at_transaction", "after_transaction"] = "at_transaction",
        **kwargs: Any,
    ) -> pd.DataFrame:
        return super().tabulate(*args, _raw=_raw, valid=valid, **kwargs)
