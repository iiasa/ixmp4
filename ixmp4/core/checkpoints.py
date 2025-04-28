from typing import TYPE_CHECKING

import pandas as pd

from ixmp4.data.abstract.checkpoint import Checkpoint
from ixmp4.data.backend import Backend

from .base import BaseFacade

if TYPE_CHECKING:
    from .run import Run


class RunCheckpoints(BaseFacade):
    def __init__(self, run: "Run", _backend: Backend | None = None) -> None:
        self.run = run
        super().__init__(_backend)

    def tabulate(self) -> pd.DataFrame:
        return self.backend.checkpoints.tabulate(run__id=self.run.id)

    def create(self, message: str) -> Checkpoint:
        self.run.require_lock()

        return self.backend.checkpoints.create(run__id=self.run.id, message=message)
