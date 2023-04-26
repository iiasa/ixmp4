from typing import Iterable

from ixmp4.data import abstract
from ixmp4.data.auth.decorators import guard

from ..docs import docs_model, BaseDocsRepository
from .model import Region
from .. import base

RegionDocs = docs_model(Region)


class RegionDocsRepository(BaseDocsRepository, base.BaseRepository):
    model_class = RegionDocs
    dimension_model_class = Region

    @guard("view")
    def list(self, *, dimension_id: int | None = None) -> Iterable[abstract.Docs]:
        return super().list(dimension_id=dimension_id)
