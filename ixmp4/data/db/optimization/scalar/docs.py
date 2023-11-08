from ixmp4.data.db.docs import BaseDocsRepository, docs_model

from .model import Scalar


class ScalarDocsRepository(BaseDocsRepository):
    model_class = docs_model(Scalar)  # ScalarDocs
    dimension_model_class = Scalar
