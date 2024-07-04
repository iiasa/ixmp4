from datetime import datetime
from typing import Any

from sqlalchemy.orm import Mapped as Mapped

from ixmp4 import db

Boolean = Mapped[bool]
DateTime = Mapped[datetime]
Float = Mapped[float]
Integer = Mapped[int]
JsonList = Mapped[list[float | int | str]]
JsonDict = Mapped[dict[str, Any]]
String = Mapped[str]
Name = Mapped[db.NameType]
UniqueName = Mapped[db.UniqueNameType]
RunId = Mapped[db.RunIdType]
Username = Mapped[db.UsernameType]
