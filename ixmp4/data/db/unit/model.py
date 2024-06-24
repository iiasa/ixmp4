from typing import ClassVar

from sqlalchemy.orm import Mapped as Mapped

from ixmp4 import db
from ixmp4.data import abstract

from .. import base


class Unit(base.BaseModel, base.TimestampMixin):
    NotFound: ClassVar = abstract.Unit.NotFound
    NotUnique: ClassVar = abstract.Unit.NotUnique
    DeletionPrevented: ClassVar = abstract.Unit.DeletionPrevented

    name: Mapped[db.UniqueName]
