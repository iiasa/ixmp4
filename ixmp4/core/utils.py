import pandas as pd

from ixmp4.data.abstract import DataPoint as DataPointModel


def substitute_type(df: pd.DataFrame, type: DataPointModel.Type | None = None):
    if "type" not in df.columns:
        # `type` given explicitly
        if type is not None:
            if type not in DataPointModel.Type:
                raise ValueError(f"Invalid data point type: {type}")
            df["type"] = type

        # pyam `data` format for annual timeseries data
        elif "year" in df.columns:
            df.rename(columns={"year": "step_year"}, inplace=True)
            df["type"] = DataPointModel.Type.ANNUAL

        else:
            raise ValueError("Required keyword argument `type` is `None`.")
