import asyncio
import warnings
from typing import TypedDict

import numpy as np
import pandas as pd
import pytest

import ixmp4
from ixmp4 import DataPoint
from ixmp4.conf import settings
from ixmp4.core.exceptions import RunLockRequired, SchemaError
from ixmp4.data.abstract.annotations import (
    HasRegionFilter,
    HasUnitFilter,
    HasVariableFilter,
)
from ixmp4.data.backend import SqlAlchemyBackend

from .. import utils
from ..fixtures import FilterIamcDataset, SmallIamcDataset


class DataPointTabulateFilters(TypedDict, total=False):
    region: HasRegionFilter
    unit: HasUnitFilter
    variable: HasVariableFilter


class TestCoreIamc:
    small = SmallIamcDataset()
    filter = FilterIamcDataset()

    def test_run_annual_datapoints_raw(self, platform: ixmp4.Platform) -> None:
        self.do_run_datapoints(
            platform, self.small.annual.copy(), True, DataPoint.Type.ANNUAL
        )

    def test_run_annual_datapoints_iamc(self, platform: ixmp4.Platform) -> None:
        # convert to test data to standard IAMC format
        df = self.small.annual.copy().rename(columns={"step_year": "year"})
        self.do_run_datapoints(platform, df, False)

    @pytest.mark.parametrize(
        "invalid_type", (DataPoint.Type.CATEGORICAL, DataPoint.Type.DATETIME)
    )
    def test_run_inconsistent_annual_raises(
        self, platform: ixmp4.Platform, invalid_type: DataPoint.Type
    ) -> None:
        with pytest.raises(SchemaError):
            self.do_run_datapoints(
                platform, self.small.annual.copy(), True, invalid_type
            )

    def test_run_categorical_datapoints_raw(self, platform: ixmp4.Platform) -> None:
        self.do_run_datapoints(
            platform, self.small.categorical.copy(), True, DataPoint.Type.CATEGORICAL
        )

    @pytest.mark.parametrize(
        "invalid_type", (DataPoint.Type.ANNUAL, DataPoint.Type.DATETIME)
    )
    def test_run_inconsistent_categorical_raises(
        self, platform: ixmp4.Platform, invalid_type: DataPoint.Type
    ) -> None:
        with pytest.raises(SchemaError):
            self.do_run_datapoints(
                platform, self.small.categorical.copy(), True, invalid_type
            )

    def test_run_datetime_datapoints_raw(self, platform: ixmp4.Platform) -> None:
        self.do_run_datapoints(
            platform, self.small.datetime.copy(), True, DataPoint.Type.DATETIME
        )

    @pytest.mark.parametrize(
        "invalid_type", (DataPoint.Type.ANNUAL, DataPoint.Type.CATEGORICAL)
    )
    def test_run_inconsistent_datetime_type_raises(
        self, platform: ixmp4.Platform, invalid_type: DataPoint.Type
    ) -> None:
        with pytest.raises(SchemaError):
            self.do_run_datapoints(
                platform, self.small.datetime.copy(), True, invalid_type
            )

    def test_unit_dimensionless_raw(self, platform: ixmp4.Platform) -> None:
        test_data = self.small.annual.copy()
        test_data.loc[0, "unit"] = ""
        platform.units.create("")
        self.do_run_datapoints(platform, test_data, True, DataPoint.Type.ANNUAL)

    def do_run_datapoints(
        self,
        platform: ixmp4.Platform,
        data: pd.DataFrame,
        raw: bool = True,
        _type: DataPoint.Type | None = None,
    ) -> None:
        # Test adding, updating, removing data to a run
        # either as ixmp4-database format (columns `step_[year/datetime/categorical]`)
        # or as standard iamc format  (column names 'year' or 'time')

        # Define required regions and units in the database
        self.small.load_regions(platform)
        self.small.load_units(platform)

        run = platform.runs.create("Model", "Scenario")
        # == Full Addition ==
        # Save to database
        with run.transact("Full Addition"):
            run.iamc.add(data, type=_type)

        # Retrieve from database via Run
        ret = run.iamc.tabulate(raw=raw)
        if raw:
            ret = ret.drop(columns=["id", "type"])
        else:
            data = data.drop(columns=["is_input"])
        utils.assert_unordered_equality(data, ret, check_like=True)

        # If not set as default, retrieve from database
        # via Platform returns an empty frame
        ret = platform.iamc.tabulate(raw=raw)
        assert ret.empty

        # Retrieve from database via Platform
        # (including model, scenario, version columns)
        ret = platform.iamc.tabulate(raw=raw, run={"default_only": False})
        if raw:
            ret = ret.drop(columns=["id", "type"])

        test_mp_data = data.copy()
        test_mp_data["model"] = run.model.name
        test_mp_data["scenario"] = run.scenario.name
        test_mp_data["version"] = run.version
        test_mp_data = test_mp_data[ret.columns]
        utils.assert_unordered_equality(test_mp_data, ret, check_like=True)

        # Retrieve from database after setting the run to default
        run.set_as_default()
        ret = platform.iamc.tabulate(raw=raw)
        if raw:
            ret = ret.drop(columns=["id", "type"])
        utils.assert_unordered_equality(test_mp_data, ret, check_like=True)

        # == Partial Removal ==
        # Remove half the data
        remove_data = data.head(len(data) // 2).drop(columns=["value"])
        remaining_data = data.tail(len(data) // 2).reset_index(drop=True)
        with run.transact("Partial Removal"):
            run.iamc.remove(remove_data, type=_type)

        # Retrieve from database
        ret = run.iamc.tabulate(raw=raw)
        if raw:
            ret = ret.drop(columns=["id", "type"])
        utils.assert_unordered_equality(remaining_data, ret, check_like=True)

        ts_after_delete = platform.backend.iamc.timeseries.tabulate(
            join_parameters=True
        )
        all_dp_after_delete = platform.backend.iamc.datapoints.tabulate()
        assert set(ts_after_delete["id"].unique()) == set(
            all_dp_after_delete["time_series__id"].unique()
        )

        # == Partial Update / Partial Addition ==
        # Update all data values
        data["value"] = -9.9

        with run.transact("Partial Update / Partial Addition"):
            # Results in a half insert / half update
            run.iamc.add(data, type=_type)

        # Retrieve from database
        ret = run.iamc.tabulate(raw=raw)
        if raw:
            ret = ret.drop(columns=["id", "type"])
        utils.assert_unordered_equality(data, ret, check_like=True)

        # == Full Removal ==
        # Remove all data
        remove_data = data.drop(columns=["value"])
        with run.transact("Full Removal"):
            # Results in a half insert / half update
            run.iamc.remove(remove_data, type=_type)

        # Retrieve from database
        ret = run.iamc.tabulate(raw=raw)
        assert ret.empty

    @pytest.mark.parametrize(
        "filters, run, exp_len",
        (
            (dict(variable={"name": "Variable 4"}), ("Model 1", "Scenario 1", 1), 1),
            (
                dict(
                    variable={"name": "Variable 4"},
                    unit={"name": "Unit 2"},
                ),
                ("Model 1", "Scenario 1", 1),
                1,
            ),
            (
                dict(
                    variable={"name__like": "Variable *"},
                    unit={"name__like": "Unit *"},
                ),
                ("Model 2", "Scenario 2", 1),
                4,
            ),
            (
                dict(variable={"name__in": ["Variable 3", "Variable 4"]}),
                ("Model 1", "Scenario 1", 1),
                2,
            ),
            (dict(variable="Variable 1"), ("Model 1", "Scenario 1", 1), 2),
            (
                dict(variable="Variable 1", unit="Unit 2"),
                ("Model 1", "Scenario 1", 1),
                1,
            ),
            (
                dict(variable=["Variable 1", "Variable 2", "Variable 3", "Variable 4"]),
                ("Model 1", "Scenario 1", 1),
                4,
            ),
        ),
    )
    def test_run_tabulate_with_filter_raw(
        self,
        platform: ixmp4.Platform,
        filters: DataPointTabulateFilters,
        run: tuple[str, str, int],
        exp_len: int,
    ) -> None:
        self.filter.load_dataset(platform)
        _run = platform.runs.get(*run)
        obs = _run.iamc.tabulate(raw=True, **filters)
        assert len(obs) == exp_len

    def test_iamc_versioning(self, pg_platform: ixmp4.Platform) -> None:
        self.do_run_datapoints(
            pg_platform, self.small.annual.copy(), True, DataPoint.Type.ANNUAL
        )

        @utils.versioning_test(pg_platform.backend)
        def assert_versions(backend: SqlAlchemyBackend) -> None:
            vdf = backend.iamc.datapoints.versions.tabulate()
            expected_versions = pd.read_csv(
                "./tests/core/expected_versions/test_iamc_versioning.csv"
            ).replace({np.nan: None})
            utils.assert_unordered_equality(expected_versions, vdf, check_dtype=False)

    def test_iamc_rollback_sqlite(self, sqlite_platform: ixmp4.Platform) -> None:
        self.small.load_regions(sqlite_platform)
        self.small.load_units(sqlite_platform)
        data = self.small.annual.copy().rename(columns={"step_year": "year"})

        run = sqlite_platform.runs.create("Model", "Scenario")

        with run.transact("Full Addition"):
            run.iamc.add(data)

        remove_data = data.head(len(data) // 2).drop(columns=["value"])
        remaining_data = data.tail(len(data) // 2).drop(columns=["is_input"])

        with warnings.catch_warnings(record=True) as w:
            try:
                with (
                    run.transact("Partial Removal Failure"),
                ):
                    run.iamc.remove(remove_data)
                    raise utils.CustomException("Whoops!!!")
            except utils.CustomException:
                pass

        ret = run.iamc.tabulate()
        utils.assert_unordered_equality(remaining_data, ret, check_like=True)

        assert (
            "An exception occurred but the `Run` was not reverted because "
            "versioning is not supported by this platform" in str(w[0].message)
        )

    def test_iamc_rollback(self, pg_platform: ixmp4.Platform) -> None:
        self.small.load_regions(pg_platform)
        self.small.load_units(pg_platform)
        data = self.small.annual.copy().rename(columns={"step_year": "year"})

        run = pg_platform.runs.create("Model", "Scenario")

        # == Full Addition ==
        # Save to database
        with run.transact("Full Addition"):
            run.iamc.add(data)

        # == Partial Removal ==
        # Remove half the data
        remove_data = data.head(len(data) // 2).drop(columns=["value"])
        try:
            with run.transact("Partial Removal Failure"):
                run.iamc.remove(remove_data)
                raise utils.CustomException("Whoops!!!")
        except utils.CustomException:
            pass

        ret = run.iamc.tabulate()
        utils.assert_unordered_equality(
            data.drop(columns=["is_input"]), ret, check_like=True
        )

        with run.transact("Partial Removal"):
            run.iamc.remove(remove_data)

        update_data = data.copy()
        update_data["value"] = -9.9

        try:
            with run.transact("Partial Update / Partial Addition Failure"):
                run.iamc.add(update_data)
                raise utils.CustomException("Whoops!!!")
        except utils.CustomException:
            pass

        remaining_data = data.tail(len(data) // 2)
        ret = run.iamc.tabulate()
        utils.assert_unordered_equality(
            remaining_data.drop(columns=["is_input"]), ret, check_like=True
        )

    def test_iamc_rollback_to_checkpoint(self, pg_platform: ixmp4.Platform) -> None:
        self.small.load_regions(pg_platform)
        self.small.load_units(pg_platform)

        data = self.small.annual.copy().rename(columns={"step_year": "year"})
        remove_data = data.head(len(data) // 2).drop(columns=["value"])

        run = pg_platform.runs.create("Model", "Scenario")

        try:
            with run.transact("Full Addition / Partial Removal"):
                run.iamc.add(data)
                run.checkpoints.create("Full Addition")
                run.iamc.remove(remove_data)
                raise utils.CustomException("Whoops!!!")
        except utils.CustomException:
            pass

        ret = run.iamc.tabulate()
        utils.assert_unordered_equality(
            data.drop(columns=["is_input"]), ret, check_like=True
        )

        assert len(run.checkpoints.tabulate()) == 1

    def test_iamc_requires_lock(self, platform: ixmp4.Platform) -> None:
        # Create a run
        run = platform.runs.create("Model", "Scenario")

        # Attempt to add data without owning a lock
        with pytest.raises(RunLockRequired):
            run.iamc.add(self.small.annual.copy())

        # Attempt to remove data without owning a lock
        with pytest.raises(RunLockRequired):
            run.iamc.remove(self.small.annual.copy())

    def test_iamc_tabulate_is_input(self, platform: ixmp4.Platform) -> None:
        self.small.load_regions(platform)
        self.small.load_units(platform)
        data = self.small.annual.copy().rename(columns={"step_year": "year"})
        run = platform.runs.create("Model", "Scenario")

        with run.transact("Add datapoints to test is_input filtering"):
            run.iamc.add(data)

        # Test specifying the default explicitly
        all_datapoints = run.iamc.tabulate(is_input=None)
        utils.assert_unordered_equality(data.drop(columns=["is_input"]), all_datapoints)

        # Test loading only solution data
        solution_datapoints = run.iamc.tabulate(is_input=False)
        utils.assert_unordered_equality(
            data[~data["is_input"]].drop(columns=["is_input"]), solution_datapoints
        )

        # Test loading only input data
        input_datapoints = run.iamc.tabulate(is_input=True)
        utils.assert_unordered_equality(
            data[data["is_input"]].drop(columns=["is_input"]), input_datapoints
        )

    def test_iamc_addition_without_is_input(self, platform: ixmp4.Platform) -> None:
        self.small.load_regions(platform)
        self.small.load_units(platform)
        data = self.small.annual.copy().drop(columns=["is_input"])
        run = platform.runs.create("Model", "Scenario")

        # Test addition without is_input column works
        with run.transact("Add datapoints without is_input"):
            run.iamc.add(data, type=DataPoint.Type.ANNUAL)

        datapoints = platform.backend.iamc.datapoints.tabulate(
            join_parameters=True,
            join_runs=False,
            run={"id": run.id, "default_only": False},
        )

        # Test default value of is_input is False (after conversion from np.False_)
        utils.assert_unordered_equality(data, datapoints[data.columns])
        assert len(datapoints["is_input"].unique()) == 1
        assert bool(datapoints["is_input"][0]) is False


class TestCoreIamcReadOnly:
    def test_mp_tabulate_big_async(self, platform_med: ixmp4.Platform) -> None:
        """Tests if big tabulations work in async contexts."""

        async def tabulate() -> pd.DataFrame:
            return platform_med.iamc.tabulate(raw=True, run={"default_only": False})

        res = asyncio.run(tabulate())
        assert len(res) > settings.default_page_size

    def test_mp_tabulate_big(self, platform_med: ixmp4.Platform) -> None:
        res = platform_med.iamc.tabulate(raw=True, run={"default_only": False})
        assert len(res) > settings.default_page_size
