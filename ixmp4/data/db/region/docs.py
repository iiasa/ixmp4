from ixmp4.data import abstract
from ixmp4.data.auth.decorators import guard

from .. import base
from ..docs import BaseDocsRepository, docs_model
from .model import Region

RegionDocs = docs_model(Region)


class RegionDocsRepository(BaseDocsRepository, base.BaseRepository):
    model_class = RegionDocs
    dimension_model_class = Region

    @guard("view")
    def list(self, *, dimension_id: int | None = None) -> list[abstract.Docs]:
        return super().list(dimension_id=dimension_id)
