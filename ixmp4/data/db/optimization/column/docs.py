from ixmp4.data.db.docs import BaseDocsRepository, docs_model

from .model import Column


class ColumnDocsRepository(BaseDocsRepository):  # type: ignore[type-arg]
    model_class = docs_model(Column)  # ColumnDocs
    dimension_model_class = Column
