from typing import TYPE_CHECKING

import pandas as pd

from ixmp4.backend import Backend
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

    def __init__(self, backend: Backend, run: "Run") -> None:
        super().__init__(backend)
        self.run = run

    def _get_service(self, backend: Backend) -> CheckpointService:
        return backend.checkpoints

    def tabulate(self) -> pd.DataFrame:
        return self._service.tabulate(run__id=self.run.id)

    def create(self, message: str) -> Checkpoint:
        self.run.require_lock()
        return self._service.create(run__id=self.run.id, message=message)
