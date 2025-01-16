from typing import TYPE_CHECKING, Any

import pandas as pd

from . import base

if TYPE_CHECKING:
    from .column import Column
    from .indexset import IndexSet


def collect_indexsets_to_check(
    columns: list["Column"],
) -> dict[str, list[float] | list[int] | list[str]]:
    """Creates a {key:value} dict from linked Column.names and their
    IndexSet.data."""
    return {column.name: column.indexset.data for column in columns}


def validate_data(
    host: base.BaseModel,
    data: dict[str, Any],
    columns: list["Column"] | list["IndexSet"],
    column_names: list[str] | None = None,
    has_values_and_units: bool = True,
) -> None:
    data_frame = pd.DataFrame.from_dict(data)

    # We don't want to validate "values" and "units" when they are present
    number_columns = (
        len(data_frame.columns) - 2 if has_values_and_units else len(data_frame.columns)
    )

    # TODO for all of the following, we might want to create unique exceptions
    # Could me make both more specific by specifiying missing/extra columns?
    if number_columns < len(columns):
        raise host.DataInvalid(
            f"While handling {host.__str__()}: \n"
            f"Data is missing for some Columns! \n Data: {data} \n "
            f"Columns: {[column.name for column in columns]}"
        )
    elif number_columns > len(columns):
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

    # TODO adapt once we remove Columns as a class
    # No way to properly type check generics
    try:
        # columns are indexsets
        limited_to_indexsets = (
            {column.name: column.data for column in columns}  # type: ignore[union-attr]
            if not column_names
            else {column_names[i]: columns[i].data for i in range(len(columns))}  # type: ignore[union-attr]
        )
    except AttributeError:
        # columns are columns
        limited_to_indexsets = collect_indexsets_to_check(columns=columns)  # type: ignore[arg-type]

    # We can make this more specific e.g. highlighting all duplicate rows via
    # pd.DataFrame.duplicated(keep="False")
    if data_frame[limited_to_indexsets.keys()].value_counts().max() > 1:
        raise host.DataInvalid(
            f"While handling {host.__str__()}: \n" "The data contains duplicate rows!"
        )

    # Can we make this more specific? Iterating over columns; if any is False,
    # return its name or something?
    if (
        not data_frame[limited_to_indexsets.keys()]
        .isin(limited_to_indexsets)
        .all(axis=None)
    ):
        raise host.DataInvalid(
            f"While handling {host.__str__()}: \n"
            "The data contains values that are not allowed as per the IndexSets "
            "and Columns it is constrained to!"
        )
