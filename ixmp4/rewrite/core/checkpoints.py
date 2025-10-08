from typing import TYPE_CHECKING

import pandas as pd

from ixmp4.rewrite.backend import Backend
from ixmp4.rewrite.data.checkpoint.dto import Checkpoint

from .base import BaseFacade

if TYPE_CHECKING:
    from .run import Run


# TODO:
class RunCheckpoints(BaseFacade):
    run: "Run"

    def __init__(self, backend: Backend, run: "Run") -> None:
        super().__init__(backend)
        self.run = run

    def tabulate(self) -> pd.DataFrame:
        return self._backend.checkpoints.tabulate(run__id=self.run.id)

    def create(self, message: str) -> Checkpoint:
        self.run.require_lock()
        return self._backend.checkpoints.create(run__id=self.run.id, message=message)
