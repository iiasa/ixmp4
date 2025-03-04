from collections.abc import Iterable
from typing import Any

from ixmp4.data.auth.decorators import guard

from .. import base
from ..docs import AbstractDocs, BaseDocsRepository, docs_model
from .model import Unit

UnitDocs = docs_model(Unit)


class UnitDocsRepository(
    BaseDocsRepository[Any],
    base.BaseRepository[Unit],
):
    model_class = UnitDocs
    dimension_model_class = Unit

    @guard("view")
    def list(
        self,
        *,
        dimension_id: int | None = None,
        dimension_id__in: Iterable[int] | None = None,
    ) -> list[AbstractDocs]:
        return super().list(
            dimension_id=dimension_id, dimension_id__in=dimension_id__in
        )
