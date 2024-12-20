from datetime import datetime
from typing import Any, Literal

from sqlalchemy.orm import Mapped as Mapped

from ixmp4 import db

Boolean = Mapped[bool]
DateTime = Mapped[datetime]
Float = Mapped[float]
IndexSetId = Mapped[db.IndexSetIdType]
Integer = Mapped[int]
JsonDict = Mapped[dict[str, Any]]
Name = Mapped[db.NameType]
OptimizationDataList = Mapped[list[float] | list[int] | list[str]]
OptimizationDataType = Mapped[Literal["float", "int", "str"] | None]
RunId = Mapped[db.RunIdType]
String = Mapped[str]
TableId = Mapped[db.TableIdType]
UniqueName = Mapped[db.UniqueNameType]
Username = Mapped[db.UsernameType]
