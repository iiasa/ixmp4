from typing import ClassVar

from ixmp4 import db
from ixmp4.data import abstract, types
from ixmp4.data.db import mixins

from .. import base


class Region(base.BaseModel, mixins.HasCreationInfo):
    NotFound: ClassVar = abstract.Region.NotFound
    NotUnique: ClassVar = abstract.Region.NotUnique
    DeletionPrevented: ClassVar = abstract.Region.DeletionPrevented

    name: types.UniqueName
    hierarchy: types.String = db.Column(db.String(1023), nullable=False)
