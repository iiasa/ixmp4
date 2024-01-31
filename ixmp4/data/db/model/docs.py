from ixmp4.data import abstract

from .. import base
from ..docs import BaseDocsRepository, docs_model
from .model import Model

ModelDocs = docs_model(Model)


class ModelDocsRepository(BaseDocsRepository, base.BaseRepository):
    model_class = ModelDocs
    dimension_model_class = Model

    def list(self, *, dimension_id: int | None = None) -> list[abstract.Docs]:
        return super().list(dimension_id=dimension_id)
