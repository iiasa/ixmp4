from collections.abc import Iterable
from typing import TYPE_CHECKING

# TODO Import this from typing when dropping Python 3.11
from typing_extensions import Unpack

if TYPE_CHECKING:
    from ixmp4.data.backend.db import SqlAlchemyBackend

import pandas as pd

from ixmp4 import db
from ixmp4.data.abstract import optimization as abstract
from ixmp4.data.abstract.annotations import HasUnitIdFilter
from ixmp4.data.auth.decorators import guard

from .. import base
from .docs import ScalarDocsRepository
from .model import Scalar


class EnumerateKwargs(base.EnumerateKwargs, HasUnitIdFilter, total=False): ...


class ScalarRepository(
    base.Creator[Scalar],
    base.Deleter[Scalar],
    base.Retriever[Scalar],
    base.Enumerator[Scalar],
    abstract.ScalarRepository,
):
    model_class = Scalar

    def __init__(self, *args: "SqlAlchemyBackend") -> None:
        super().__init__(*args)
        self.docs = ScalarDocsRepository(*args)

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

    @guard("edit")
    def delete(self, id: int) -> None:
        return super().delete(id=id)

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
    def create(self, name: str, value: float, unit_name: str, run_id: int) -> Scalar:
        return super().create(
            name=name, value=value, unit_name=unit_name, run_id=run_id
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
    def list(self, **kwargs: Unpack[EnumerateKwargs]) -> Iterable[Scalar]:
        return super().list(**kwargs)

    @guard("view")
    def tabulate(self, **kwargs: Unpack[EnumerateKwargs]) -> pd.DataFrame:
        return super().tabulate(**kwargs)
