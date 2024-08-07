from ixmp4.data.db.docs import BaseDocsRepository, docs_model

from .model import Parameter


class ParameterDocsRepository(BaseDocsRepository):
    model_class = docs_model(Parameter)  # ParameterDocs
    dimension_model_class = Parameter
