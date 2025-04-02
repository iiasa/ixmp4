from typing import TYPE_CHECKING, ClassVar

import pandas as pd
from sqlalchemy.exc import NoResultFound

# TODO Import this from typing when dropping Python 3.11
from typing_extensions import Unpack

from ixmp4 import db
from ixmp4.data import abstract, types
from ixmp4.data.auth.decorators import guard
from ixmp4.data.db import mixins

from ..unit import Unit

if TYPE_CHECKING:
    from ixmp4.data.backend.db import SqlAlchemyBackend

from . import base
from .variable import Variable


class Measurand(base.BaseModel, mixins.HasCreationInfo):
    __versioned__ = {}

    NotFound: ClassVar = abstract.Measurand.NotFound
    NotUnique: ClassVar = abstract.Measurand.NotUnique
    DeletionPrevented: ClassVar = abstract.Measurand.DeletionPrevented

    __table_args__ = (db.UniqueConstraint("variable__id", "unit__id"),)

    variable__id: types.Integer = db.Column(
        db.Integer, db.ForeignKey("iamc_variable.id"), nullable=False, index=True
    )
    variable: types.Mapped[Variable] = db.relationship(
        "Variable",
        backref="measurands",
        foreign_keys=[variable__id],
        lazy="select",
    )

    unit__id: types.Integer = db.Column(
        db.Integer, db.ForeignKey("unit.id"), nullable=False, index=True
    )
    unit: types.Mapped[Unit] = db.relationship(
        "Unit",
        backref="measurands",
        foreign_keys=[unit__id],
        lazy="select",
    )


class MeasurandRepository(
    base.Creator[Measurand],
    base.Retriever[Measurand],
    base.Enumerator[Measurand],
    base.VersionManager[Measurand],
    abstract.MeasurandRepository,
):
    model_class = Measurand

    def __init__(self, *args: "SqlAlchemyBackend") -> None:
        super().__init__(*args)

    @guard("view")
    def get(self, variable_name: str, unit__id: int) -> Measurand:
        exc = (
            db.select(Measurand)
            .join(Measurand.variable)
            .where(Measurand.unit__id == unit__id)
            .where(Variable.name == variable_name)
        )

        try:
            return self.session.execute(exc).scalar_one()
        except NoResultFound:
            raise Measurand.NotFound

    def add(self, variable_name: str, unit__id: int) -> Measurand:
        q_exc = db.select(Variable).where(Variable.name == variable_name)
        try:
            variable = self.session.execute(q_exc).scalar_one()
        except NoResultFound:
            variable = Variable(name=variable_name)

        measurand = Measurand(variable=variable, unit__id=unit__id)
        self.session.add(measurand)
        self.session.commit()
        return measurand

    @guard("edit")
    def create(self, *args: Unpack[tuple[str, int]]) -> Measurand:
        return super().create(*args)

    def select(self) -> db.sql.Select[tuple[Measurand]]:
        return db.select(Measurand)

    @guard("view")
    def list(self) -> list[Measurand]:
        return super().list()

    @guard("view")
    def tabulate(self) -> pd.DataFrame:
        return super().tabulate()

    @guard("view")
    def tabulate_versions(
        self, /, **kwargs: Unpack[base.TabulateVersionsKwargs]
    ) -> pd.DataFrame:
        return super().tabulate_versions(**kwargs)
