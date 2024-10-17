from typing import TYPE_CHECKING, Any

import pandas as pd

from . import base

if TYPE_CHECKING:
    from .column import Column


def collect_indexsets_to_check(
    columns: list["Column"],
) -> dict[str, Any]:
    """Creates a {key:value} dict from linked Column.names and their
    IndexSet.data."""
    collection: dict[str, Any] = {}
    for column in columns:
        collection[column.name] = column.indexset.data
    return collection


def validate_data(host: base.BaseModel, data: dict[str, Any], columns: list["Column"]):
    data_frame: pd.DataFrame = pd.DataFrame.from_dict(data)
    # TODO for all of the following, we might want to create unique exceptions
    # Could me make both more specific by specifiying missing/extra columns?
    if len(data_frame.columns) < len(columns):
        raise host.DataInvalid(
            f"While handling {host.__str__()}: \n"
            f"Data is missing for some Columns! \n Data: {data} \n "
            f"Columns: {[column.name for column in columns]}"
        )
    elif len(data_frame.columns) > len(columns):
        raise host.DataInvalid(
            f"While handling {host.__str__()}: \n"
            f"Trying to add data to unknown Columns! \n Data: {data} \n "
            f"Columns: {[column.name for column in columns]}"
        )

    # We could make this more specific maybe by pointing to the missing values
    if data_frame.isna().any(axis=None):
        raise host.DataInvalid(
            f"While handling {host.__str__()}: \n"
            "The data is missing values, please make sure it "
            "does not contain None or NaN, either!"
        )
    # We can make this more specific e.g. highlighting all duplicate rows via
    # pd.DataFrame.duplicated(keep="False")
    if data_frame.value_counts().max() > 1:
        raise host.DataInvalid(
            f"While handling {host.__str__()}: \n" "The data contains duplicate rows!"
        )

    # Can we make this more specific? Iterating over columns; if any is False,
    # return its name or something?
    limited_to_indexsets = collect_indexsets_to_check(columns=columns)
    if not data_frame.isin(limited_to_indexsets).all(axis=None):
        raise host.DataInvalid(
            f"While handling {host.__str__()}: \n"
            "The data contains values that are not allowed as per the IndexSets "
            "and Columns it is constrained to!"
        )

    return data_frame.to_dict(orient="list")
