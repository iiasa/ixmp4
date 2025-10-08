from toolkit import db

from ixmp4.rewrite.exceptions import DeletionPrevented, NotFound, NotUnique, registry

from .db import Checkpoint
from .filter import CheckpointFilter


@registry.register()
class CheckpointNotFound(NotFound):
    pass


@registry.register()
class CheckpointNotUnique(NotUnique):
    pass


@registry.register()
class CheckpointDeletionPrevented(DeletionPrevented):
    pass


class ItemRepository(db.r.ItemRepository[Checkpoint]):
    NotFound = CheckpointNotFound

    NotUnique = CheckpointNotUnique
    target = db.r.ModelTarget(Checkpoint)
    filter = db.r.Filter(CheckpointFilter, Checkpoint)


class PandasRepository(db.r.PandasRepository):
    NotFound = CheckpointNotFound
    NotUnique = CheckpointNotUnique
    target = db.r.ModelTarget(Checkpoint)
    filter = db.r.Filter(CheckpointFilter, Checkpoint)
