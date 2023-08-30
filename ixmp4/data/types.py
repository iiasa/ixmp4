from datetime import datetime
from typing import Any

from sqlalchemy.orm import Mapped as Mapped

Boolean = Mapped[bool]
DateTime = Mapped[datetime]
Float = Mapped[float]
Integer = Mapped[int]
JsonList = Mapped[list[int | str]]
JsonDict = Mapped[dict[str, Any]]
String = Mapped[str]
