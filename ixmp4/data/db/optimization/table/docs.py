from typing import Any

from ixmp4.data.db.docs import BaseDocsRepository, docs_model

from .model import Table

TableDocs = docs_model(Table)


class TableDocsRepository(BaseDocsRepository[Any]):
    model_class = TableDocs
    dimension_model_class = Table
