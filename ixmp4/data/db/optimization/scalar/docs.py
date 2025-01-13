from typing import Any

from ixmp4.data.db.docs import BaseDocsRepository, docs_model

from .model import Scalar

ScalarDocs = docs_model(Scalar)


class ScalarDocsRepository(BaseDocsRepository[Any]):
    model_class = ScalarDocs
    dimension_model_class = Scalar
