"""This module only contains benchmarks, no assertions are made to validate the
results."""

from typing import Any, cast

import numpy as np
import pandas as pd
import pytest
import sqlalchemy as sa
from pytest_benchmark.fixture import BenchmarkFixture
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session

import ixmp4
from ixmp4.data.backend import Backend
from ixmp4.transport import Transport
from tests import auth, backends
from tests.base import TransportTest
from tests.fixtures import get_csv_data
from tests.profiling import ProfiledContextManager

platform = backends.get_platform_fixture(scope="class")


@pytest.fixture(scope="session")
def regions() -> pd.DataFrame:
    return get_csv_data("benchmark", "regions")


@pytest.fixture(scope="session")
def units() -> pd.DataFrame:
    return get_csv_data("benchmark", "units")


@pytest.fixture(scope="session")
def runs() -> pd.DataFrame:
    return get_csv_data("benchmark", "runs")


@pytest.fixture(scope="session")
def datapoints_full_insert() -> pd.DataFrame:
    return get_csv_data("benchmark", "datapoints")


@pytest.fixture(scope="session")
def datapoints_half_insert() -> pd.DataFrame:
    data = get_csv_data("benchmark", "datapoints")
    return data.head(len(data) // 2)


@pytest.fixture(scope="session")
def datapoints_half_insert_half_update() -> pd.DataFrame:
    data = get_csv_data("benchmark", "datapoints")
    data["value"] = -99.99
    return data


class BenchmarkDataMixin:
    AUTH_BENCHMARK_TARGET_DATAPOINTS = 1_000_000

    def add_datapoints(
        self, platform: ixmp4.Platform, datapoints: pd.DataFrame
    ) -> None:
        for run_tuple, rows in datapoints.groupby(
            ["model", "scenario", "version"], group_keys=False
        ):
            model, scenario, version = run_tuple
            run = platform.runs.get(
                str(model), str(scenario), int(cast(np.int64, version))
            )
            with run.transact("Benchmark: Add DataPoints Full"):
                run.iamc.add(rows.drop(columns=["model", "scenario", "version"]))

    def remove_datapoints(
        self, platform: ixmp4.Platform, datapoints: pd.DataFrame
    ) -> None:
        for run_tuple, rows in datapoints.groupby(
            ["model", "scenario", "version"], group_keys=False
        ):
            model, scenario, version = run_tuple
            run = platform.runs.get(
                str(model), str(scenario), int(cast(np.int64, version))
            )
            with run.transact("Benchmark: Remove DataPoints Full"):
                run.iamc.remove(
                    rows.drop(columns=["model", "scenario", "version", "value"])
                )

    @staticmethod
    def _insert_nested_records_fast(
        session: Session,
        *,
        n_records: int,
        n_series: int,
        start_year: int,
    ) -> None:
        if n_records <= 0:
            return

        if n_series <= 0:
            raise ValueError("Cannot insert datapoints without existing time series.")

        dialect = session.get_bind().dialect.name
        if dialect == "postgresql":
            session.execute(
                sa.text(
                    """
                    WITH series AS (
                        SELECT id, ROW_NUMBER() OVER (ORDER BY id) AS rn
                        FROM iamc_timeseries
                    )
                    INSERT INTO iamc_datapoint_universal (
                        time_series__id,
                        value,
                        type,
                        step_year
                    )
                    SELECT
                        series.id,
                        ((g.i % 1000)::float / 10.0),
                        'ANNUAL',
                        :start_year + ((g.i - 1) / :n_series)
                    FROM generate_series(1, :n_records) AS g(i)
                    JOIN series ON series.rn = ((g.i - 1) % :n_series) + 1
                    """
                ),
                {
                    "n_records": n_records,
                    "n_series": n_series,
                    "start_year": start_year,
                },
            )
            return

        # SQLite fast path: build numbers with a 10x10x10x10x10x10 Cartesian
        # product to avoid recursion limits and Python-side list materialization.
        if n_records > 1_000_000:
            raise ValueError(
                "SQLite fast path currently supports at most 1,000,000 rows."
            )

        session.execute(
            sa.text(
                """
                WITH digits(d) AS (
                    VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)
                ),
                nums(n) AS (
                    SELECT
                        d0.d
                        + d1.d * 10
                        + d2.d * 100
                        + d3.d * 1000
                        + d4.d * 10000
                        + d5.d * 100000
                        + 1
                    FROM digits d0
                    CROSS JOIN digits d1
                    CROSS JOIN digits d2
                    CROSS JOIN digits d3
                    CROSS JOIN digits d4
                    CROSS JOIN digits d5
                ),
                series AS (
                    SELECT id, ROW_NUMBER() OVER (ORDER BY id) AS rn
                    FROM iamc_timeseries
                )
                INSERT INTO iamc_datapoint_universal (
                    time_series__id,
                    value,
                    type,
                    step_year
                )
                SELECT
                    series.id,
                    ((nums.n % 1000) / 10.0),
                    'ANNUAL',
                    :start_year + CAST(((nums.n - 1) / :n_series) AS INTEGER)
                FROM nums
                JOIN series ON series.rn = ((nums.n - 1) % :n_series) + 1
                WHERE nums.n <= :n_records
                """
            ),
            {
                "n_records": n_records,
                "n_series": n_series,
                "start_year": start_year,
            },
        )

    def seed_auth_benchmark_data(
        self,
        platform: ixmp4.Platform,
        transport: Transport,
        regions: pd.DataFrame,
        units: pd.DataFrame,
        runs: pd.DataFrame,
        datapoints: pd.DataFrame,
    ) -> None:
        for name, hierarchy in regions.itertuples(index=False):
            platform.regions.create(name, hierarchy)

        for name, *_ in units.itertuples(index=False):
            platform.units.create(name)

        for model, scenario, version, is_default in runs.itertuples(index=False):
            run = platform.runs.create(model, scenario)
            assert run.version == int(version)
            if is_default:
                run.set_as_default()
        self.add_datapoints(platform, datapoints)
        direct = cast(TransportTest, self).get_unauthorized_direct_or_skip(transport)
        session = direct.session

        current_count = int(
            session.execute(
                sa.text("SELECT COUNT(*) FROM iamc_datapoint_universal")
            ).scalar_one()
        )

        if current_count > self.AUTH_BENCHMARK_TARGET_DATAPOINTS:
            raise ValueError(
                "Seeded datapoints already exceed auth benchmark target of "
                f"{self.AUTH_BENCHMARK_TARGET_DATAPOINTS}."
            )

        n_series = int(
            session.execute(
                sa.text("SELECT COUNT(*) FROM iamc_timeseries")
            ).scalar_one()
        )
        max_step_year = int(
            session.execute(
                sa.text(
                    "SELECT COALESCE(MAX(step_year), 0) FROM iamc_datapoint_universal"
                )
            ).scalar_one()
        )

        self._insert_nested_records_fast(
            session,
            n_records=self.AUTH_BENCHMARK_TARGET_DATAPOINTS - current_count,
            n_series=n_series,
            start_year=max_step_year + 1,
        )
        session.commit()

        final_count = int(
            session.execute(
                sa.text("SELECT COUNT(*) FROM iamc_datapoint_universal")
            ).scalar_one()
        )
        if final_count != self.AUTH_BENCHMARK_TARGET_DATAPOINTS:
            raise ValueError(
                "Auth benchmark datapoint seed mismatch. "
                f"Expected {self.AUTH_BENCHMARK_TARGET_DATAPOINTS}, got {final_count}."
            )


class TestBenchmarks(BenchmarkDataMixin):
    @pytest.mark.benchmark(group="create_regions")
    def test_create_regions_benchmark(
        self,
        platform: ixmp4.Platform,
        profiled: ProfiledContextManager,
        benchmark: BenchmarkFixture,
        regions: pd.DataFrame,
    ) -> None:
        def run(mp: ixmp4.Platform) -> None:
            with profiled():
                for name, hierarchy in regions.itertuples(index=False):
                    mp.regions.create(name, hierarchy)

        benchmark.pedantic(run, args=(platform,))  # type: ignore[no-untyped-call]

    @pytest.mark.benchmark(group="create_units")
    def test_create_units_benchmark(
        self,
        platform: ixmp4.Platform,
        profiled: ProfiledContextManager,
        benchmark: BenchmarkFixture,
        units: pd.DataFrame,
    ) -> None:
        def run(mp: ixmp4.Platform) -> None:
            with profiled():
                for name, *_ in units.itertuples(index=False):
                    mp.units.create(name)

        benchmark.pedantic(run, args=(platform,))  # type: ignore[no-untyped-call]

    @pytest.mark.benchmark(group="create_runs")
    def test_create_runs_benchmark(
        self,
        platform: ixmp4.Platform,
        profiled: ProfiledContextManager,
        benchmark: BenchmarkFixture,
        runs: pd.DataFrame,
    ) -> None:
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

        benchmark.pedantic(run, args=(platform,))  # type: ignore[no-untyped-call]

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

        benchmark.pedantic(run, args=(platform,))  # type: ignore[no-untyped-call]

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

        benchmark.pedantic(run, args=(platform,))  # type: ignore[no-untyped-call]

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

        def setup() -> tuple[tuple[ixmp4.Platform], dict[str, Any]]:
            self.add_datapoints(platform, datapoints_half_insert)
            return ((platform,), {})

        def run(mp: ixmp4.Platform) -> None:
            with profiled():
                self.add_datapoints(mp, datapoints_half_insert_half_update)

        benchmark.pedantic(run, setup=setup)  # type: ignore[no-untyped-call]

    @pytest.mark.benchmark(group="tabulate_datapoints")
    def test_tabulate_datapoints_benchmark(
        self,
        platform: ixmp4.Platform,
        profiled: ProfiledContextManager,
        benchmark: BenchmarkFixture,
        datapoints_half_insert_half_update: pd.DataFrame,
    ) -> None:
        """Benchmarks a full insert of `test_data_big`."""

        def run(mp: ixmp4.Platform) -> pd.DataFrame:
            with profiled():
                return mp.iamc.tabulate(run={"default_only": False})

        result = benchmark.pedantic(run, args=(platform,))  # type: ignore[no-untyped-call]
        assert len(result) == len(datapoints_half_insert_half_update)

    @pytest.mark.benchmark(group="filter_datapoints")
    def test_filter_datapoints_benchmark(
        self,
        platform: ixmp4.Platform,
        profiled: ProfiledContextManager,
        benchmark: BenchmarkFixture,
        datapoints_half_insert_half_update: pd.DataFrame,
    ) -> None:
        """Benchmarks a full insert of `test_data_big`."""

        def run(mp: ixmp4.Platform) -> pd.DataFrame:
            with profiled():
                return mp.iamc.tabulate(
                    run={"default_only": False},
                    unit={"id__in": [i for i in range(20, 90)]},
                    variable={
                        "name__in": ["Variable " + str(i) for i in range(80, 120)]
                    },
                    region={"name__like": "Region 2*1"},
                )

        result = benchmark.pedantic(run, args=(platform,))  # type: ignore[no-untyped-call]
        assert len(result) == 100

    @pytest.mark.benchmark(group="filter_year_range_type")
    def test_filter_year_range_type_benchmark(
        self,
        platform: ixmp4.Platform,
        profiled: ProfiledContextManager,
        benchmark: BenchmarkFixture,
    ) -> None:
        """Benchmarks filtering by ANNUAL type and a year range."""

        def run(mp: ixmp4.Platform) -> pd.DataFrame:
            with profiled():
                return mp.iamc.tabulate(
                    run={"default_only": False},
                    type="ANNUAL",
                    year__gte=1980,
                    year__lte=2010,
                )

        result = benchmark.pedantic(run, args=(platform,))  # type: ignore[no-untyped-call]
        assert len(result) == 4600

    @pytest.mark.benchmark(group="filter_model_scenario_cross")
    def test_filter_model_scenario_cross_benchmark(
        self,
        platform: ixmp4.Platform,
        profiled: ProfiledContextManager,
        benchmark: BenchmarkFixture,
    ) -> None:
        """Benchmarks filtering by model name__in and scenario name__in.
        DataPoint -> timeseries -> run -> model
        DataPoint -> timeseries -> run -> scenario
        """

        def run(mp: ixmp4.Platform) -> pd.DataFrame:
            with profiled():
                return mp.iamc.tabulate(
                    run={"default_only": False},
                    model={"name__in": ["Model " + str(i) for i in range(5)]},
                    scenario={"name__in": ["Scenario 0", "Scenario 1"]},
                )

        result = benchmark.pedantic(run, args=(platform,))  # type: ignore[no-untyped-call]
        assert len(result) == 22474

    @pytest.mark.benchmark(group="filter_all_dimensions")
    def test_filter_all_dimensions_benchmark(
        self,
        platform: ixmp4.Platform,
        profiled: ProfiledContextManager,
        benchmark: BenchmarkFixture,
    ) -> None:
        """Benchmarks combining all five filterable dimensions at once."""

        def run(mp: ixmp4.Platform) -> pd.DataFrame:
            with profiled():
                return mp.iamc.tabulate(
                    run={"default_only": False},
                    model={"name": "Model 0"},
                    scenario={"name": "Scenario 0"},
                    variable={"name__like": "Variable 1*"},
                    region={"name__like": "Region 1*"},
                )

        result = benchmark.pedantic(run, args=(platform,))  # type: ignore[no-untyped-call]
        assert len(result) == 290

    @pytest.mark.benchmark(group="filter_large_variable_in")
    def test_filter_large_variable_in_benchmark(
        self,
        platform: ixmp4.Platform,
        profiled: ProfiledContextManager,
        benchmark: BenchmarkFixture,
    ) -> None:
        """Benchmarks filtering with a 50-element variable name__in list."""

        def run(mp: ixmp4.Platform) -> pd.DataFrame:
            with profiled():
                return mp.iamc.tabulate(
                    run={"default_only": False},
                    variable={"name__in": ["Variable " + str(i) for i in range(50)]},
                )

        result = benchmark.pedantic(run, args=(platform,))  # type: ignore[no-untyped-call]
        assert len(result) == 16934

    @pytest.mark.benchmark(group="filter_variable_like_large_result")
    def test_filter_variable_like_large_result_benchmark(
        self,
        platform: ixmp4.Platform,
        profiled: ProfiledContextManager,
        benchmark: BenchmarkFixture,
    ) -> None:
        """Benchmarks a LIKE filter on the joined variable table."""

        def run(mp: ixmp4.Platform) -> pd.DataFrame:
            with profiled():
                return mp.iamc.tabulate(
                    run={"default_only": False},
                    variable={"name__like": "Variable 1*"},
                )

        result = benchmark.pedantic(run, args=(platform,))  # type: ignore[no-untyped-call]
        assert len(result) == 37906

    @pytest.mark.benchmark(group="filter_combined_analytical")
    def test_filter_combined_analytical_benchmark(
        self,
        platform: ixmp4.Platform,
        profiled: ProfiledContextManager,
        benchmark: BenchmarkFixture,
    ) -> None:
        """Benchmarks a real-world analytical query pattern."""

        def run(mp: ixmp4.Platform) -> pd.DataFrame:
            with profiled():
                return mp.iamc.tabulate(
                    run={"default_only": False},
                    model={"name": "Model 3"},
                    variable={"name__like": "Variable 1*"},
                    type="ANNUAL",
                    year__gte=2000,
                    year__lte=2019,
                )

        result = benchmark.pedantic(run, args=(platform,))  # type: ignore[no-untyped-call]
        assert len(result) == 79

    @pytest.mark.benchmark(group="filter_scenario_large_variable_in")
    def test_filter_scenario_large_variable_in_benchmark(
        self,
        platform: ixmp4.Platform,
        profiled: ProfiledContextManager,
        benchmark: BenchmarkFixture,
    ) -> None:
        """Benchmarks scenario filter combined with a 100-element variable name__in."""

        def run(mp: ixmp4.Platform) -> pd.DataFrame:
            with profiled():
                return mp.iamc.tabulate(
                    run={"default_only": False},
                    scenario={"name": "Scenario 0"},
                    variable={"name__in": ["Variable " + str(i) for i in range(100)]},
                )

        result = benchmark.pedantic(run, args=(platform,))  # type: ignore[no-untyped-call]
        assert len(result) == 11548

    @pytest.mark.benchmark(group="filter_unit_in_variable_like")
    def test_filter_unit_in_variable_like_benchmark(
        self,
        platform: ixmp4.Platform,
        profiled: ProfiledContextManager,
        benchmark: BenchmarkFixture,
    ) -> None:
        """Benchmarks two simultaneous measurand-path predicates: unit name__in and
        variable name__like.
        """

        def run(mp: ixmp4.Platform) -> pd.DataFrame:
            with profiled():
                return mp.iamc.tabulate(
                    run={"default_only": False},
                    unit={"name__in": ["Unit " + str(i) for i in range(30)]},
                    variable={"name__like": "Variable 2*"},
                )

        result = benchmark.pedantic(run, args=(platform,))  # type: ignore[no-untyped-call]
        assert len(result) == 4110

    @pytest.mark.benchmark(group="test_filter_all_dimension_ids")
    def test_filter_all_dimension_ids(
        self,
        platform: ixmp4.Platform,
        profiled: ProfiledContextManager,
        benchmark: BenchmarkFixture,
    ) -> None:
        """Benchmarks a typical expensive query from the data explorer."""

        def run(mp: ixmp4.Platform) -> pd.DataFrame:
            with profiled():
                return mp.iamc.tabulate(
                    run={"default_only": False},
                    unit={"id__in": [i for i in range(30)]},
                    variable={
                        "id__in": [3, 4, 5, 6, 10, 14, 15, 19, 23, 34, 42, 12, 45]
                    },
                    region={"id__in": [3, 4, 5, 6, 10, 14, 15, 19, 23, 34, 42, 12, 45]},
                )

        result = benchmark.pedantic(run, args=(platform,))  # type: ignore[no-untyped-call]
        print(len(result))
        assert len(result)


class TestRestAuthBenchmarks(
    auth.CarinaTest, auth.PublicPlatformTest, TransportTest, BenchmarkDataMixin
):
    """Benchmarks the authenticated read path with data seeded directly."""

    transport = staticmethod(backends.get_transport_fixture(scope="class"))

    de_filter: dict[str, Any] = dict(
        run={
            "default_only": False,
            "id__in": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        },
        variable={
            "id__in": [1, 2, 3, 4, 5, 6, 7, 8, 9, 21, 22, 25, 26, 27, 30, 55, 61, 100]
        },
        region={"id__in": [1, 2, 3, 4, 5, 6, 7, 8, 100, 102, 104, 106, 108, 200, 220]},
    )

    @pytest.fixture(scope="class")
    def platform(
        self,
        transport: Transport,
        regions: pd.DataFrame,
        units: pd.DataFrame,
        runs: pd.DataFrame,
        datapoints_full_insert: pd.DataFrame,
    ) -> ixmp4.Platform:
        try:
            unauthorized_platform = ixmp4.Platform(
                Backend(self.get_unauthorized_direct_or_skip(transport))
            )
            self.seed_auth_benchmark_data(
                unauthorized_platform,
                transport,
                regions,
                units,
                runs,
                datapoints_full_insert,
            )
            return ixmp4.Platform(Backend(transport))
        except OperationalError as e:
            pytest.skip("Database is not reachable: " + str(e))

    @pytest.mark.benchmark(group="test_filter_like_a_data_explorer")
    def test_filter_like_a_data_explorer(
        self,
        platform: ixmp4.Platform,
        profiled: ProfiledContextManager,
        benchmark: BenchmarkFixture,
    ) -> None:
        """Benchmarks the data-explorer pattern: filter by pre-resolved run IDs."""
        unauthorized_platform = ixmp4.Platform(
            Backend(self.get_unauthorized_direct_or_skip(platform.backend.transport))
        )

        def run(mp: ixmp4.Platform) -> pd.DataFrame:
            with profiled():
                return mp.iamc.tabulate(**self.de_filter)

        result = benchmark.pedantic(
            run, args=(unauthorized_platform,), warmup_rounds=5, rounds=5
        )  # type: ignore[no-untyped-call]
        assert len(result)

    @pytest.mark.benchmark(group="test_filter_like_a_data_explorer_with_auth")
    def test_filter_like_a_data_explorer_with_auth(
        self,
        platform: ixmp4.Platform,
        profiled: ProfiledContextManager,
        benchmark: BenchmarkFixture,
    ) -> None:
        def run(mp: ixmp4.Platform) -> pd.DataFrame:
            with profiled():
                return mp.iamc.tabulate(**self.de_filter)

        result = benchmark.pedantic(run, args=(platform,), warmup_rounds=5, rounds=5)  # type: ignore[no-untyped-call]
        assert len(result) == 2446
