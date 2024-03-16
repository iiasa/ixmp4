import pandas as pd

from .base import BaseFacade


class MetaRepository(BaseFacade):
    def tabulate(self, **kwargs) -> pd.DataFrame:
        # TODO: accept list of `Run` instances as argument
        return self.backend.meta.tabulate(join_run_index=True, **kwargs).drop(
            columns=["id", "type"]
        )
