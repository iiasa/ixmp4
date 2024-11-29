from typing import Any

from ixmp4.data.db.docs import BaseDocsRepository, docs_model

from .model import IndexSet

IndexSetDocs = docs_model(IndexSet)


class IndexSetDocsRepository(BaseDocsRepository[Any]):
    model_class = IndexSetDocs
    dimension_model_class = IndexSet
