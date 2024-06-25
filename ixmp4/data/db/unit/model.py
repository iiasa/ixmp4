from typing import ClassVar

from ixmp4.data import abstract, types

from .. import base


class Unit(base.BaseModel, base.TimestampMixin):
    NotFound: ClassVar = abstract.Unit.NotFound
    NotUnique: ClassVar = abstract.Unit.NotUnique
    DeletionPrevented: ClassVar = abstract.Unit.DeletionPrevented

    name: types.UniqueName
