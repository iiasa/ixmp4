import pandas as pd

from ixmp4 import db
from ixmp4.data import abstract
from ixmp4.data.auth.decorators import guard

from .. import base
from .docs import ModelDocsRepository
from .model import Model


class ModelRepository(
    base.Creator[Model],
    base.Retriever[Model],
    base.Enumerator[Model],
    abstract.ModelRepository,
):
    model_class = Model
    docs: ModelDocsRepository

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.docs = ModelDocsRepository(*args, **kwargs)

        from .filter import ModelFilter

        self.filter_class = ModelFilter

    def add(self, name: str) -> Model:
        model = Model(name=name)
        self.session.add(model)
        return model

    @guard("view")
    def get(self, name: str) -> Model:
        exc = self.select(name=name)
        try:
            return self.session.execute(exc).scalar_one()
        except db.NoResultFound:
            raise Model.NotFound

    @guard("edit")
    def create(self, *args, **kwargs) -> Model:
        return super().create(*args, **kwargs)

    @guard("view")
    def list(self, *args, **kwargs) -> list[Model]:
        return super().list(*args, **kwargs)

    @guard("view")
    def tabulate(self, *args, **kwargs) -> pd.DataFrame:
        return super().tabulate(*args, **kwargs)
