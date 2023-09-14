from ixmp4.data.db.docs import BaseDocsRepository, docs_model

from .model import IndexSet


class IndexSetDocsRepository(BaseDocsRepository):
    model_class = docs_model(IndexSet)  # IndexSetDocs
    dimension_model_class = IndexSet
