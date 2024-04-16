from typing import ClassVar

from ixmp4 import db
from ixmp4.data import abstract, types

from .. import base


class Scenario(base.BaseModel, base.TimestampMixin):
    NotFound: ClassVar = abstract.Scenario.NotFound
    NotUnique: ClassVar = abstract.Scenario.NotUnique
    DeletionPrevented: ClassVar = abstract.Scenario.DeletionPrevented

    name: types.String = db.Column(db.String(255), unique=True, nullable=False)
