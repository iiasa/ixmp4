from typing import Any

from ixmp4.data.db.docs import BaseDocsRepository, docs_model

from .model import Equation

EquationDocs = docs_model(Equation)


# NOTE Mypy is a static type checker, but we create the class(es) that would need to go
# in BaseDocsRepository[...] dynamically, so I don't know if there's any way to type
# hint them properly. TypeVar, TypeAlias, type(), type[], and NewType all did not work.
class EquationDocsRepository(BaseDocsRepository[Any]):
    model_class = EquationDocs
    dimension_model_class = Equation
