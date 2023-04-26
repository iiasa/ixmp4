import pandas as pd

from ixmp4.data.abstract import DataPoint as DataPointModel


def substitute_type(df: pd.DataFrame, type: DataPointModel.Type | None = None):
    if type is not None:
        if type not in DataPointModel.Type:
            raise ValueError(f"Invalid data point type: {type}")
        else:
            df["type"] = type
    else:
        if "type" not in df.columns:
            raise ValueError("Required keyword argument `type` is `None`.")
