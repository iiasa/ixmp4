import pandas as pd

from ..base import BaseFacade
from .variable import VariableRepository

# column for the year or datetime value by datapoint type
MAP_STEP_COLUMN = {
    "ANNUAL": "step_year",
    "CATEGORICAL": "step_year",
    "DATETIME": "step_time",
}


class IamcRepository(BaseFacade):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self.variables = VariableRepository(_backend=self.backend)

    def tabulate(
        self, join_runs: bool = True, raw: bool = False, **filters
    ) -> pd.DataFrame:
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

            # shorten step-[year/time/categorical] format to standard IAMC format
            if raw is False:
                df.rename(columns={"step_category": "subannual"}, inplace=True)

                if set(df.type.unique()).issubset(["ANNUAL", "CATEGORICAL"]):
                    df.rename(columns={"step_year": "year"}, inplace=True)
                    time_col = "year"
                else:

                    def map_step_column(df: pd.Series):
                        df["time"] = df[MAP_STEP_COLUMN[df.type]]
                        return df

                    df = df.apply(map_step_column, axis=1)
                    time_col = "time"

                columns = ["model", "scenario", "version"] if join_runs else []
                columns += ["region", "variable", "unit"] + [time_col]
                if "subannual" in df.columns:
                    columns += ["subannual"]
                return df[columns + ["value"]]

        return df
