import json
from datetime import datetime
from typing import Any, Literal, cast

import pandas as pd

from tests.conftest import fixture_dir


def json_timestamp_decoder(obj: dict[Any, Any]) -> dict[Any, Any]:
    """
    Hook method that takes a dictionary and returns one in which qualifying
    have been parsed into `date` and `datetime` objects.
    """

    for key, value in obj.items():
        if not isinstance(value, str):
            continue
        try:
            obj[key] = datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            continue

    return obj


def get_migration_data(name: str) -> list[dict[str, Any]]:
    """Get the migration data as a list of dictionaries."""
    with open(fixture_dir / "migrations" / (name + ".json"), "r") as f:
        return cast(
            list[dict[str, Any]], json.load(f, object_hook=json_timestamp_decoder)
        )


def get_csv_data(size: Literal["big"], name: str) -> pd.DataFrame:
    """Get the csv data as a dataframe."""
    return pd.read_csv(fixture_dir / size / (name + ".csv"))
