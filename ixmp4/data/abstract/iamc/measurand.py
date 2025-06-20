from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from .. import Unit, Variable

import pandas as pd

from .. import base


class Measurand(base.BaseModel, Protocol):
    """Measurand data model."""

    variable__id: int
    "Foreign unique integer id of a variable."
    variable: "Variable"
    "Associated variable."

    unit__id: int
    "Foreign unique integer id of a unit."
    unit: "Unit"
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
