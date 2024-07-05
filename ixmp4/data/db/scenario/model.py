from typing import ClassVar

from ixmp4.data import abstract, types
from ixmp4.data.db import mixins

from .. import base


class Scenario(base.BaseModel, mixins.HasCreationInfo):
    NotFound: ClassVar = abstract.Scenario.NotFound
    NotUnique: ClassVar = abstract.Scenario.NotUnique
    DeletionPrevented: ClassVar = abstract.Scenario.DeletionPrevented

    name: types.UniqueName
