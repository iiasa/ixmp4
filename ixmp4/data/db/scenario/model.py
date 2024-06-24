from typing import ClassVar

from sqlalchemy.orm import Mapped as Mapped

from ixmp4 import db
from ixmp4.data import abstract

from .. import base


class Scenario(base.BaseModel, base.TimestampMixin):
    NotFound: ClassVar = abstract.Scenario.NotFound
    NotUnique: ClassVar = abstract.Scenario.NotUnique
    DeletionPrevented: ClassVar = abstract.Scenario.DeletionPrevented

    name: Mapped[db.UniqueName]
