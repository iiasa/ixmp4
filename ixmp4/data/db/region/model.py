from typing import ClassVar

from ixmp4 import db
from ixmp4.data import abstract, types

from .. import base


class Region(base.BaseModel):
    NotFound: ClassVar = abstract.Region.NotFound
    NotUnique: ClassVar = abstract.Region.NotUnique
    DeletionPrevented: ClassVar = abstract.Region.DeletionPrevented

    name: types.String = db.Column(db.String(1023), unique=True, nullable=False)
    hierarchy: types.String = db.Column(db.String(1023), nullable=False)

    created_at: types.DateTime = db.Column(db.DateTime, nullable=True)
    created_by: types.String = db.Column(db.String(255), nullable=True)
