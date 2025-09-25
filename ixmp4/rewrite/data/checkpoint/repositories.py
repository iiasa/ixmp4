from typing import TYPE_CHECKING

from toolkit import db
from toolkit.exceptions import NotFound, NotUnique

from ixmp4.core.exceptions import DeletionPrevented

from .db import Checkpoint
from .filter import CheckpointFilter

if TYPE_CHECKING:
    pass


class CheckpointNotFound(NotFound):
    pass


class CheckpointNotUnique(NotUnique):
    pass


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
