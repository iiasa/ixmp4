from typing import List

import pandas as pd

from .base import BaseFacade


class MetaRepository(BaseFacade):
    def tabulate(self, run_ids: List[int]) -> pd.DataFrame:
        # TODO: accept list of `Run` instances as arg
        # TODO: expand run-id to model-scenario-version-id columns
        return (
            self.backend.meta.tabulate(run_ids=run_ids)
            .drop(columns=["id", "type"])
            .rename(columns={"run__id": "run_id"})
        )
