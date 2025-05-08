from datetime import datetime
from typing import Literal

from sqlalchemy.orm import Mapped as Mapped

from ixmp4 import db

Boolean = Mapped[bool]
DateTime = Mapped[datetime]
Float = Mapped[float]
EquationId = Mapped[db.EquationIdType]
IndexSetId = Mapped[db.IndexSetIdType]
IndexSet__Id = Mapped[db.IndexSet__IdType]
Integer = Mapped[int]
JsonDict = Mapped[dict[str, list[float] | list[int] | list[str]]]
Name = Mapped[db.NameType]
OptimizationDataList = Mapped[list[float] | list[int] | list[str]]
OptimizationDataType = Mapped[Literal["float", "int", "str"] | None]
OptimizationVariableType = Mapped[db.OptimizationVariableIdType]
ParameterId = Mapped[db.ParameterIdType]
RunId = Mapped[db.RunIdType]
String = Mapped[str]
TableId = Mapped[db.TableIdType]
UniqueName = Mapped[db.UniqueNameType]
Username = Mapped[db.UsernameType]
