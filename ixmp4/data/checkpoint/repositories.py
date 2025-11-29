from toolkit import db

from .db import Checkpoint
from .exceptions import CheckpointNotFound, CheckpointNotUnique
from .filter import CheckpointFilter


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
