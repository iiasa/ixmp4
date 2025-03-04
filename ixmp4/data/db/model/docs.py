from collections.abc import Iterable
from typing import Any

from .. import base
from ..docs import AbstractDocs, BaseDocsRepository, docs_model
from .model import Model

ModelDocs = docs_model(Model)


class ModelDocsRepository(BaseDocsRepository[Any], base.BaseRepository[Model]):
    model_class = ModelDocs
    dimension_model_class = Model

    def list(
        self,
        *,
        dimension_id: int | None = None,
        dimension_id__in: Iterable[int] | None = None,
    ) -> list[AbstractDocs]:
        return super().list(
            dimension_id=dimension_id, dimension_id__in=dimension_id__in
        )
