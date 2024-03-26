from ixmp4.data.db.docs import BaseDocsRepository, docs_model

from .model import Table


class TableDocsRepository(BaseDocsRepository):
    model_class = docs_model(Table)  # TableDocs
    dimension_model_class = Table
