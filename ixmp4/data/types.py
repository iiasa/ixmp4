from datetime import datetime
from typing import Any, Literal

from sqlalchemy.orm import Mapped as Mapped

from ixmp4 import db

Boolean = Mapped[bool]
DateTime = Mapped[datetime]
Float = Mapped[float]
IndexSetId = Mapped[db.IndexSetIdType]
Integer = Mapped[int]
OptimizationDataList = Mapped[list[float] | list[int] | list[str]]
JsonDict = Mapped[dict[str, Any]]
OptimizationDataType = Mapped[Literal["float", "int", "str"] | None]
ParameterId = Mapped[db.ParameterIdType]
String = Mapped[str]
Name = Mapped[db.NameType]
UniqueName = Mapped[db.UniqueNameType]
RunId = Mapped[db.RunIdType]
Username = Mapped[db.UsernameType]
