from typing import Any

from ixmp4.data.db.docs import BaseDocsRepository, docs_model

from .model import Parameter

ParameterDocs = docs_model(Parameter)


class ParameterDocsRepository(BaseDocsRepository[Any]):
    model_class = ParameterDocs
    dimension_model_class = Parameter
