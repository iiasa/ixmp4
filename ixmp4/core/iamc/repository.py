import pandas as pd

from ..base import BaseFacade
from .variable import VariableRepository


class IamcRepository(BaseFacade):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self.variables = VariableRepository(_backend=self.backend)

    def tabulate(self, join_runs=True, **filters) -> pd.DataFrame:
        # return only default runs unless a run-filter is provided
        if "run" not in filters:
            filters["run"] = {"default_only": True}

        df = self.backend.iamc.datapoints.tabulate(
            join_parameters=True,
            join_runs=join_runs,
            **filters,
        ).dropna(how="all", axis="columns")

        if not df.empty:
            df = df.drop(columns=["time_series__id"])
            df.unit = df.unit.replace({"dimensionless": ""})

        return df
