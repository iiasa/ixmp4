from typing import ClassVar

from ixmp4.data import abstract

from .. import base


class Scenario(base.BaseModel, base.NameMixin, base.TimestampMixin):
    NotFound: ClassVar = abstract.Scenario.NotFound
    NotUnique: ClassVar = abstract.Scenario.NotUnique
    DeletionPrevented: ClassVar = abstract.Scenario.DeletionPrevented
