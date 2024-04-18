from typing import ClassVar

from ixmp4.data import types
from ixmp4.data.abstract import iamc as abstract

from .. import base


class Variable(base.BaseModel, base.NameMixin, base.TimestampMixin):
    NotFound: ClassVar = abstract.Variable.NotFound
    NotUnique: ClassVar = abstract.Variable.NotUnique
    DeletionPrevented: ClassVar = abstract.Variable.DeletionPrevented

    name: types.UniqueName
