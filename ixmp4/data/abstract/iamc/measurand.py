from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from .. import Unit, Variable

import pandas as pd

from ixmp4.data import types

from .. import base


class Measurand(base.BaseModel, Protocol):
    """Measurand data model."""

    variable__id: types.Integer
    "Foreign unique integer id of a variable."
    variable: types.Mapped["Variable"]
    "Associated variable."

    unit__id: types.Integer
    "Foreign unique integer id of a unit."
    unit: types.Mapped["Unit"]
    "Associated unit."

    def __str__(self) -> str:
        return f"<Measurand {self.id}>"


class MeasurandRepository(
    base.Creator,
    base.Retriever,
    base.Enumerator,
    base.VersionManager,
    Protocol,
):
    def create(self, variable_name: str, unit__id: int) -> Measurand: ...

    def get(self, variable_name: str, unit__id: int) -> Measurand: ...

    def get_or_create(self, variable_name: str, unit__id: int) -> Measurand:
        try:
            return self.get(variable_name, unit__id)
        except Measurand.NotFound:
            return self.create(variable_name, unit__id)

    def list(self) -> list[Measurand]: ...

    def tabulate(self) -> pd.DataFrame: ...
