from typing import Any

from ixmp4.data.db.docs import BaseDocsRepository, docs_model

from .model import Variable

VariableDocs = docs_model(Variable)


class VariableDocsRepository(BaseDocsRepository[Any]):
    model_class = VariableDocs
    dimension_model_class = Variable
