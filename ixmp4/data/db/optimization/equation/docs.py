from ixmp4.data.db.docs import BaseDocsRepository, docs_model

from .model import Equation


class EquationDocsRepository(BaseDocsRepository):
    model_class = docs_model(Equation)  # EquationDocs
    dimension_model_class = Equation
