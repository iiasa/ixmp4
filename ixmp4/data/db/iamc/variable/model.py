from typing import ClassVar

from ixmp4.data import types
from ixmp4.data.abstract import iamc as abstract
from ixmp4.data.db import mixins

from .. import base


class Variable(base.BaseModel, mixins.HasCreationInfo):
    NotFound: ClassVar = abstract.Variable.NotFound
    NotUnique: ClassVar = abstract.Variable.NotUnique
    DeletionPrevented: ClassVar = abstract.Variable.DeletionPrevented

    name: types.UniqueName
