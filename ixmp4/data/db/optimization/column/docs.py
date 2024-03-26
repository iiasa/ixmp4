from ixmp4.data.db.docs import BaseDocsRepository, docs_model

from .model import Column


class ColumnDocsRepository(BaseDocsRepository):
    model_class = docs_model(Column)  # ColumnDocs
    dimension_model_class = Column
