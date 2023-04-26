from datetime import datetime

from sqlalchemy.orm import (
    Mapped as Mapped,
)


Integer = Mapped[int]
Float = Mapped[float]
String = Mapped[str]
Boolean = Mapped[bool]
DateTime = Mapped[datetime]
