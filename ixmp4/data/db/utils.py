from typing import Any

import numpy as np
import pandas as pd


def map_existing(
    df: pd.DataFrame,
    existing_df: pd.DataFrame,
    join_on: tuple[str, str],
    map: tuple[str, str],
) -> tuple[pd.DataFrame, np.ndarray[Any, np.dtype[np.str_]]]:
    _, join_to = join_on
    _, map_to = map
    existing_df = existing_df.rename(columns=dict([join_on, map]))[[join_to, map_to]]
    df = df.merge(existing_df, how="left", on=[join_to])
    missing = df.where(pd.isnull(df[map_to]))[join_to].dropna().unique()
    return df, missing
