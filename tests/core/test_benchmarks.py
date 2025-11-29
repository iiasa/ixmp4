"""This module only contains benchmarks, no assertions are made to validate the
results."""

import pandas as pd
import pytest
from pytest_benchmark.fixture import BenchmarkFixture

import ixmp4
from tests import backends
from tests.fixtures import get_csv_data
from tests.profiling import ProfiledContextManager

platform = backends.get_platform_fixture(scope="class")


@pytest.fixture(scope="session")
def regions() -> pd.DataFrame:
    return get_csv_data("big", "regions")


@pytest.fixture(scope="session")
def units() -> pd.DataFrame:
    return get_csv_data("big", "units")


@pytest.fixture(scope="session")
def runs() -> pd.DataFrame:
    return get_csv_data("big", "runs")


@pytest.fixture(scope="session")
def datapoints_full_insert() -> pd.DataFrame:
    return get_csv_data("big", "datapoints")


@pytest.fixture(scope="session")
def datapoints_half_insert() -> pd.DataFrame:
    data = get_csv_data("big", "datapoints")
    return data.head(len(data) // 2)


@pytest.fixture(scope="session")
def datapoints_half_insert_half_update() -> pd.DataFrame:
    data = get_csv_data("big", "datapoints")
    data["value"] = -99.99
    return data


class TestBenchmarks:
    @pytest.mark.benchmark(group="create_regions")
    def test_create_regions_benchmark(
        self,
        platform: ixmp4.Platform,
        profiled: ProfiledContextManager,
        benchmark: BenchmarkFixture,
        regions: pd.DataFrame,
    ):
        def run(mp: ixmp4.Platform) -> None:
            with profiled():
                for name, hierarchy in regions.itertuples(index=False):
                    mp.regions.create(name, hierarchy)

        benchmark.pedantic(run, args=(platform,))

    @pytest.mark.benchmark(group="create_units")
    def test_create_units_benchmark(
        self,
        platform: ixmp4.Platform,
        profiled: ProfiledContextManager,
        benchmark: BenchmarkFixture,
        units: pd.DataFrame,
    ):
        def run(mp: ixmp4.Platform) -> None:
            with profiled():
                for name, *_ in units.itertuples(index=False):
                    mp.units.create(name)

        benchmark.pedantic(run, args=(platform,))

    @pytest.mark.benchmark(group="create_runs")
    def test_create_runs_benchmark(
        self,
        platform: ixmp4.Platform,
        profiled: ProfiledContextManager,
        benchmark: BenchmarkFixture,
        runs: pd.DataFrame,
    ):
        def run(mp: ixmp4.Platform) -> None:
            with profiled():
                for model, scenario, version, is_default in runs.itertuples(
                    index=False
                ):
                    # version is ignored, but should be sequential
                    run = mp.runs.create(model, scenario)
                    assert run.version == int(version)
                    if is_default:
                        run.set_as_default()

        benchmark.pedantic(run, args=(platform,))

    def add_datapoints(self, platform: ixmp4.Platform, datapoints: pd.DataFrame):
        for run, rows in datapoints.groupby(
            ["model", "scenario", "version"], group_keys=False
        ):
            model, scenario, version = run
            # version is ignored, but should be sequential
            run = platform.runs.get(str(model), str(scenario), int(version))
            with run.transact("Benchmark: Add DataPoints Full"):
                run.iamc.add(rows.drop(columns=["model", "scenario", "version"]))

    def remove_datapoints(self, platform: ixmp4.Platform, datapoints: pd.DataFrame):
        for run, rows in datapoints.groupby(
            ["model", "scenario", "version"], group_keys=False
        ):
            model, scenario, version = run
            # version is ignored, but should be sequential
            run = platform.runs.get(str(model), str(scenario), int(version))
            with run.transact("Benchmark: Remove DataPoints Full"):
                run.iamc.remove(
                    rows.drop(columns=["model", "scenario", "version", "value"])
                )

    @pytest.mark.benchmark(group="add_datapoints")
    def test_add_datapoints_benchmark(
        self,
        platform: ixmp4.Platform,
        profiled: ProfiledContextManager,
        benchmark: BenchmarkFixture,
        datapoints_full_insert: pd.DataFrame,
    ) -> None:
        """Benchmarks a full insert of `test_data_big`."""

        def run(mp: ixmp4.Platform) -> None:
            with profiled():
                self.add_datapoints(mp, datapoints_full_insert)

        benchmark.pedantic(run, args=(platform,))

    @pytest.mark.benchmark(group="remove_datapoints")
    def test_remove_datapoints_benchmark(
        self,
        platform: ixmp4.Platform,
        profiled: ProfiledContextManager,
        benchmark: BenchmarkFixture,
        datapoints_full_insert: pd.DataFrame,
    ) -> None:
        """Benchmarks a full insert of `test_data_big`."""

        def run(mp: ixmp4.Platform) -> None:
            with profiled():
                self.remove_datapoints(mp, datapoints_full_insert)

        benchmark.pedantic(run, args=(platform,))

    @pytest.mark.benchmark(group="upsert_datapoints")
    def test_upsert_datapoints_benchmark(
        self,
        platform: ixmp4.Platform,
        profiled: ProfiledContextManager,
        benchmark: BenchmarkFixture,
        datapoints_half_insert: pd.DataFrame,
        datapoints_half_insert_half_update: pd.DataFrame,
    ) -> None:
        """Benchmarks a full insert of `test_data_big`."""

        def setup() -> None:
            self.add_datapoints(platform, datapoints_half_insert)
            return ((platform,), {})

        def run(mp: ixmp4.Platform) -> None:
            with profiled():
                self.add_datapoints(mp, datapoints_half_insert_half_update)

        benchmark.pedantic(run, setup=setup)

    @pytest.mark.benchmark(group="tabulate_datapoints")
    def test_tabulate_datapoints_benchmark(
        self,
        platform: ixmp4.Platform,
        profiled: ProfiledContextManager,
        benchmark: BenchmarkFixture,
        datapoints_half_insert_half_update: pd.DataFrame,
    ) -> None:
        """Benchmarks a full insert of `test_data_big`."""

        def run(mp: ixmp4.Platform) -> None:
            with profiled():
                return mp.iamc.tabulate()

        result = benchmark.pedantic(run, args=(platform,))
        assert len(result) == len(datapoints_half_insert_half_update)
