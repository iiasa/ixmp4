from ixmp4.data import abstract
from ixmp4.data.auth.decorators import guard

from .. import base
from ..docs import BaseDocsRepository, docs_model
from .model import Unit

UnitDocs = docs_model(Unit)


class UnitDocsRepository(
    BaseDocsRepository,
    base.BaseRepository,
):
    model_class = UnitDocs
    dimension_model_class = Unit

    @guard("view")
    def list(self, *, dimension_id: int | None = None) -> list[abstract.Docs]:
        return super().list(dimension_id=dimension_id)
