from typing import ClassVar

from ixmp4.data import abstract, types

from .. import base


class Model(base.BaseModel, base.NameMixin, base.TimestampMixin):
    NotFound: ClassVar = abstract.Model.NotFound
    NotUnique: ClassVar = abstract.Model.NotUnique
    DeletionPrevented: ClassVar = abstract.Model.DeletionPrevented

    name: types.UniqueName
