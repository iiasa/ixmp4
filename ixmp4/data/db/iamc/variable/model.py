from typing import ClassVar

from sqlalchemy.orm import Mapped as Mapped

from ixmp4 import db
from ixmp4.data.abstract import iamc as abstract

from .. import base


class Variable(base.BaseModel, base.TimestampMixin):
    NotFound: ClassVar = abstract.Variable.NotFound
    NotUnique: ClassVar = abstract.Variable.NotUnique
    DeletionPrevented: ClassVar = abstract.Variable.DeletionPrevented

    name: Mapped[db.UniqueName]
