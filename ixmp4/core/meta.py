import pandas as pd

from .run import RunRepository
from .base import BaseFacade


class MetaRepository(BaseFacade):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self.runs = RunRepository(_backend=self.backend)

    def tabulate(self, **kwargs) -> pd.DataFrame:
        # TODO: accept list of `Run` instances as arg
        runs = self.runs.tabulate(**kwargs.get("run", {}))
        meta = self.backend.meta.tabulate(**kwargs).merge(
            runs, left_on="run__id", right_on="id"
        )
        return meta[["model", "scenario", "version", "key", "value"]]
