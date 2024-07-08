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

    def add(self, name: str, value: float | int, unit_name: str, run_id: int) -> Scalar:
        unit_id = self.backend.units.get(unit_name).id
        scalar = Scalar(
            name=name,
            value=value,
            unit__id=unit_id,
            run__id=run_id,
        )
        self.session.add(scalar)
        return scalar

    @guard("view")
    def get(self, run_id: int, name: str) -> Scalar:
        exc = db.select(Scalar).where(
            (Scalar.name == name) & (Scalar.run__id == run_id)
        )
        try:
            return self.session.execute(exc).scalar_one()
        except db.NoResultFound:
            raise Scalar.NotFound

    @guard("view")
    def get_by_id(self, id: int) -> Scalar:
        obj = self.session.get(self.model_class, id)

        if obj is None:
            raise Scalar.NotFound(id=id)

        return obj

    @guard("edit")
    def create(
        self, name: str, value: float, unit_name: str, run_id: int, **kwargs
    ) -> Scalar:
        return super().create(
            name=name, value=value, unit_name=unit_name, run_id=run_id, **kwargs
        )

    @guard("edit")
    def update(
        self, id: int, value: float | None = None, unit_id: int | None = None
    ) -> Scalar:
        exc = db.update(Scalar).where(
            Scalar.id == id,
        )

        if value is not None:
            exc = exc.values(value=value)
        if unit_id is not None:
            exc = exc.values(unit__id=unit_id)

        self.session.execute(exc)
        self.session.commit()
        return self.get_by_id(id)

    @guard("view")
    def list(self, *args, **kwargs) -> Iterable[Scalar]:
        return super().list(*args, **kwargs)

    @guard("view")
    def tabulate(self, *args, **kwargs) -> pd.DataFrame:
        return super().tabulate(*args, **kwargs)
