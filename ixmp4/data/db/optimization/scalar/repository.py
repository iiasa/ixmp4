from typing import Iterable

import pandas as pd

from ixmp4 import db
from ixmp4.data.abstract import optimization as abstract
from ixmp4.data.auth.decorators import guard

from .. import base
from .docs import ScalarDocsRepository
from .model import Scalar


class ScalarRepository(
    base.Creator[Scalar],
    base.Retriever[Scalar],
    base.Enumerator[Scalar],
    abstract.ScalarRepository,
):
    model_class = Scalar

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.docs = ScalarDocsRepository(*args, **kwargs)

        from .filter import OptimizationScalarFilter

        self.filter_class = OptimizationScalarFilter

    def add(self, name: str, value: float | int, unit_id: int, run_id: int) -> Scalar:
        scalar = Scalar(
            name=name, value=value, unit__id=unit_id, run__id=run_id, **self.get_creation_info()
        )
        self.session.add(scalar)
        return scalar

    @guard("view")
    def get(self, run_id: int, name: str, unit_id: int | None = None) -> Scalar | Iterable[Scalar]:
        exc = db.select(Scalar).where((Scalar.name == name) & (Scalar.run__id == run_id))
        if unit_id is not None:
            exc = exc.where(Scalar.unit__id == unit_id)
        try:
            return self.session.execute(exc).scalar_one()
        except db.NoResultFound:
            raise Scalar.NotFound
        except db.MultipleResultsFound:
            return self.session.execute(exc).scalars().all()

    @guard("view")
    def get_by_id(self, id: int) -> Scalar:
        obj = self.session.get(self.model_class, id)

        if obj is None:
            raise Scalar.NotFound(id=id)

        return obj

    @guard("edit")
    def create(self, name: str, value: float, unit_id: int, run_id: int, **kwargs) -> Scalar:
        return super().create(name=name, value=value, unit_id=unit_id, run_id=run_id, **kwargs)

    @guard("view")
    def list(self, *args, **kwargs) -> Iterable[Scalar]:
        return super().list(*args, **kwargs)

    @guard("view")
    def tabulate(self, *args, **kwargs) -> pd.DataFrame:
        return super().tabulate(*args, **kwargs)
