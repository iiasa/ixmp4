import pandas as pd

from .base import BaseFacade


class MetaRepository(BaseFacade):
    def tabulate(self, **kwargs) -> pd.DataFrame:
        # TODO: accept list of `Run` instances as arg
        # TODO: expand run-id to model-scenario-version-id columns
        return (
            self.backend.meta.tabulate(**kwargs)
            .drop(columns=["id", "type"])
            .rename(columns={"run__id": "run_id"})
        )
