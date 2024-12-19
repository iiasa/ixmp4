from typing import Any

from ixmp4.data.db.docs import BaseDocsRepository, docs_model

from .model import OptimizationVariable as Variable

OptimizationVariableDocs = docs_model(Variable)


class OptimizationVariableDocsRepository(BaseDocsRepository[Any]):
    model_class = OptimizationVariableDocs
    dimension_model_class = Variable
