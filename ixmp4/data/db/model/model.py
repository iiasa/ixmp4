from typing import ClassVar

from ixmp4 import db
from ixmp4.data import abstract, types

from .. import base


class Model(base.BaseModel):
    NotFound: ClassVar = abstract.Model.NotFound
    NotUnique: ClassVar = abstract.Model.NotUnique
    DeletionPrevented: ClassVar = abstract.Model.DeletionPrevented

    name: types.String = db.Column(db.String(255), unique=True)

    created_at: types.DateTime = db.Column(db.DateTime, nullable=True)
    created_by: types.String = db.Column(db.String(255), nullable=True)
