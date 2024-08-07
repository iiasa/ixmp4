from typing import ClassVar

from ixmp4.data import abstract, types
from ixmp4.data.db import mixins

from .. import base


class Model(base.BaseModel, mixins.HasCreationInfo):
    NotFound: ClassVar = abstract.Model.NotFound
    NotUnique: ClassVar = abstract.Model.NotUnique
    DeletionPrevented: ClassVar = abstract.Model.DeletionPrevented

    name: types.UniqueName
