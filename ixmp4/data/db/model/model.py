from typing import ClassVar

from sqlalchemy.orm import Mapped as Mapped

from ixmp4 import db
from ixmp4.data import abstract

from .. import base


class Model(base.BaseModel, base.TimestampMixin):
    NotFound: ClassVar = abstract.Model.NotFound
    NotUnique: ClassVar = abstract.Model.NotUnique
    DeletionPrevented: ClassVar = abstract.Model.DeletionPrevented

    name: Mapped[db.UniqueName]
