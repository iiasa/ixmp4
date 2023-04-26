from ixmp4.data.db.docs import BaseDocsRepository, docs_model

from .model import Variable


class VariableDocsRepository(BaseDocsRepository):
    model_class = docs_model(Variable)  # VariableDocs
    dimension_model_class = Variable
