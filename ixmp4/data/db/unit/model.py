from typing import ClassVar

from ixmp4 import db
from ixmp4.data import abstract, types

from .. import base


class Unit(base.BaseModel):
    NotFound: ClassVar = abstract.Unit.NotFound
    NotUnique: ClassVar = abstract.Unit.NotUnique
    DeletionPrevented: ClassVar = abstract.Unit.DeletionPrevented

    name: types.String = db.Column(db.String(255), nullable=False, unique=True)

    created_at = db.Column(db.DateTime)
    created_by = db.Column(db.String(255))
