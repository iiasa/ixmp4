from typing import TYPE_CHECKING

import pandas as pd

from ixmp4.data.checkpoint.dto import Checkpoint
from ixmp4.data.checkpoint.exceptions import (
    CheckpointDeletionPrevented,
    CheckpointNotFound,
    CheckpointNotUnique,
)
from ixmp4.data.checkpoint.service import CheckpointService

from .base import BaseServiceFacade

if TYPE_CHECKING:
    from .run import Run


class RunCheckpoints(BaseServiceFacade[CheckpointService]):
    run: "Run"

    NotFound = CheckpointNotFound
    NotUnique = CheckpointNotUnique
    DeletionPrevented = CheckpointDeletionPrevented

    def __init__(self, service: CheckpointService, run: "Run") -> None:
        super().__init__(service)
        self.run = run

    def tabulate(self) -> pd.DataFrame:
        return self.service.tabulate(run__id=self.run.id)

    def create(self, message: str) -> Checkpoint:
        self.run.require_lock()
        return self.service.create(run__id=self.run.id, message=message)
