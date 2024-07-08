import pandas as pd

from ixmp4 import db
from ixmp4.data.abstract import iamc as abstract
from ixmp4.data.auth.decorators import guard

from .. import base
from .docs import VariableDocsRepository
from .model import Variable


class VariableRepository(
    base.Creator[Variable],
    base.Retriever[Variable],
    base.Enumerator[Variable],
    abstract.VariableRepository,
):
    model_class = Variable

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.docs = VariableDocsRepository(*args, **kwargs)

        from .filter import VariableFilter

        self.filter_class = VariableFilter

    def add(self, name: str) -> Variable:
        variable = Variable(name=name)
        self.session.add(variable)
        return variable

    @guard("view")
    def get(self, name: str) -> Variable:
        exc = db.select(Variable).where(Variable.name == name)
        try:
            return self.session.execute(exc).scalar_one()
        except db.NoResultFound:
            raise Variable.NotFound

    @guard("edit")
    def create(self, *args, **kwargs) -> Variable:
        return super().create(*args, **kwargs)

    @guard("view")
    def list(self, *args, **kwargs) -> list[Variable]:
        return super().list(*args, **kwargs)

    @guard("view")
    def tabulate(self, *args, **kwargs) -> pd.DataFrame:
        return super().tabulate(*args, **kwargs)
