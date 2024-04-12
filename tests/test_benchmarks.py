"""This module only contains benchmarks, no assertions are made to validate the
results."""

import pandas as pd
import pytest

from ixmp4 import NotFound, Run

from .conftest import TEST_DATA_BIG
from .utils import add_regions, add_units, all_platforms

# skip tests if performance test file not found
if TEST_DATA_BIG is None:
    pytestmark = pytest.mark.skip(reason="Performance test file not found")  # type: ignore


def add_datapoints(test_mp, df, type=None):
    df = df.copy()
    for ms, run_df in df.groupby(["model", "scenario"]):
        model_name, scenario_name = ms

        try:
            run = test_mp.runs.get(model=model_name, scenario=scenario_name, version=1)
        except NotFound:
            run = test_mp.runs.create(model=model_name, scenario=scenario_name)

        run.iamc.add(run_df.drop(columns=["model", "scenario"]), type=type)


def remove_datapoints(test_mp, df, type=None):
    df = df.copy()
    for ms, run_df in df.groupby(["model", "scenario"]):
        model_name, scenario_name = ms

        try:
            run = test_mp.runs.get(model=model_name, scenario=scenario_name, version=1)
        except NotFound:
            run = test_mp.runs.create(model=model_name, scenario=scenario_name)

        run.iamc.remove(run_df.drop(columns=["model", "scenario"]), type=type)


def tabulate_datapoints(test_mp, **kwargs):
    runs = test_mp.backend.runs.list(default_only=False)

    dfs = []
    for run_model in runs:
        run = Run(_backend=test_mp.backend, _model=run_model)
        df = run.iamc.tabulate(**kwargs, raw=True)
        df["model"] = run.model.name
        df["scenario"] = run.scenario.name
        dfs.append(df)

    return pd.concat(dfs).dropna(axis="columns").sort_index(axis=1)


@all_platforms
class TestBenchmarks:
    def test_add_datapoints_full_benchmark(
        self, test_mp, profiled, benchmark, test_data_big, request
    ):
        """Benchmarks a full insert of `test_data_big`."""

        test_mp = request.getfixturevalue(test_mp)

        def setup():
            add_regions(test_mp, test_data_big["region"].unique())
            add_units(test_mp, test_data_big["unit"].unique())
            return (test_mp,), {}

        def run(mp):
            with profiled():
                add_datapoints(mp, test_data_big)

        benchmark.pedantic(run, setup=setup)

    def test_add_datapoints_half_unchanged_benchmark(
        self, test_mp, profiled, benchmark, test_data_big, request
    ):
        """Benchmarks a full insert of `test_data_big` on a half-filled database."""

        test_mp = request.getfixturevalue(test_mp)

        def setup():
            add_regions(test_mp, test_data_big["region"].unique())
            add_units(test_mp, test_data_big["unit"].unique())
            add_datapoints(test_mp, test_data_big.head(len(test_data_big) // 2))

            return (test_mp,), {}

        def run(mp):
            with profiled():
                add_datapoints(mp, test_data_big)

        benchmark.pedantic(run, setup=setup)

    def test_add_datapoints_half_insert_half_update_benchmark(
        self, test_mp, profiled, benchmark, test_data_big, request
    ):
        """Benchmarks a full insert of `test_data_big` with changed values on a
        half-filled database."""

        test_mp = request.getfixturevalue(test_mp)

        def setup():
            add_regions(test_mp, test_data_big["region"].unique())
            add_units(test_mp, test_data_big["unit"].unique())
            add_datapoints(test_mp, test_data_big.head(len(test_data_big) // 2))
            data = test_data_big.copy()
            data["value"] = -9999
            return (test_mp, data), {}

        def run(mp, data):
            with profiled():
                add_datapoints(mp, data)

            ret = tabulate_datapoints(mp).drop(columns=["id"])
            assert ret["value"].unique() == [-9999]

        benchmark.pedantic(run, setup=setup)

    def test_remove_datapoints_benchmark(
        self, test_mp, profiled, benchmark, test_data_big, request
    ):
        """Benchmarks a full removal of `test_data_big` from a filled database."""

        test_mp = request.getfixturevalue(test_mp)

        def setup():
            add_regions(test_mp, test_data_big["region"].unique())
            add_units(test_mp, test_data_big["unit"].unique())
            add_datapoints(test_mp, test_data_big)
            data = test_data_big.drop(columns=["value"])
            return (test_mp, data), {}

        def run(mp, data):
            with profiled():
                remove_datapoints(mp, data)

        benchmark.pedantic(run, setup=setup)

    def test_tabulate_datapoints_benchmark(
        self, test_mp, profiled, benchmark, test_data_big, request
    ):
        """Benchmarks a full retrieval of `test_data_big` from a filled database."""

        test_mp = request.getfixturevalue(test_mp)

        def setup():
            add_regions(test_mp, test_data_big["region"].unique())
            add_units(test_mp, test_data_big["unit"].unique())
            add_datapoints(test_mp, test_data_big)
            return (test_mp,), {}

        def run(mp):
            with profiled():
                tabulate_datapoints(mp)

        benchmark.pedantic(run, setup=setup)
