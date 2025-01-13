from typing import Any

import pandas as pd
import pytest

import ixmp4

from .conftest import Profiled
from .fixtures import BigIamcDataset

big = BigIamcDataset()


@pytest.mark.parametrize(
    "filters",
    [
        {
            "model": {"name": "Model 0"},
            "scenario": {"name": "Scenario 0"},
            "run": {"default_only": False},
        },
        {
            "model": {"name__like": "Model *"},
        },
        {
            "unit": {"name__in": [f"Unit {i}" for i in range(10)]},
            "run": {"default_only": False},
        },
        {"region": {"name__in": [f"Region {i}" for i in range(10)]}},
        {"variable": {"name__like": "Variable 1*"}},
        {
            "model": {"name__like": "Model *"},
            "unit": {"name__in": [f"Unit {i}" for i in range(10)]},
            "variable": {"name__like": "Variable 1*"},
            "region": {"name__in": [f"Region {i}" for i in range(10)]},
            "run": {"default_only": False},
        },
    ],
)
def test_filter_datapoints_benchmark(
    platform: ixmp4.Platform,
    profiled: Profiled,
    # NOTE can be specified once https://github.com/ionelmc/pytest-benchmark/issues/212
    # is closed
    benchmark: Any,
    filters: dict[str, dict[str, bool | str | list[str]]],
) -> None:
    """Benchmarks the filtration of `test_data_big`."""

    big.load_regions(platform)
    big.load_units(platform)
    big.load_runs(platform)
    big.load_datapoints(platform)

    def run() -> pd.DataFrame:
        with profiled():
            # Not sure why mypy complains here, maybe about covariance?
            return platform.iamc.tabulate(**filters)  # type: ignore[arg-type]

    df = benchmark.pedantic(run, warmup_rounds=5, rounds=10)
    assert not df.empty
