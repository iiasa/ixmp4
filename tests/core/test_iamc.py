import asyncio
from collections.abc import Iterable

import numpy as np
import pandas as pd
import pytest

import ixmp4
from ixmp4 import DataPoint
from ixmp4.conf import settings
from ixmp4.core.exceptions import RunLockRequired, SchemaError

from ..fixtures import FilterIamcDataset, SmallIamcDataset
from ..utils import (
    assert_unordered_equality,
)


class CustomException(Exception):
    pass


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
        assert_unordered_equality(data, ret, check_like=True)

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
        assert_unordered_equality(test_mp_data, ret, check_like=True)

        # Retrieve from database after setting the run to default
        run.set_as_default()
        ret = platform.iamc.tabulate(raw=raw)
        if raw:
            ret = ret.drop(columns=["id", "type"])
        assert_unordered_equality(test_mp_data, ret, check_like=True)

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
        assert_unordered_equality(remaining_data, ret, check_like=True)

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
        assert_unordered_equality(data, ret, check_like=True)

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
        filters: dict[str, dict[str, str | Iterable[str]]],
        run: tuple[str, str, int],
        exp_len: int,
    ) -> None:
        self.filter.load_dataset(platform)
        _run = platform.runs.get(*run)
        obs = _run.iamc.tabulate(raw=True, **filters)
        assert len(obs) == exp_len

    def test_iamc_versioning(self, platform: ixmp4.Platform) -> None:
        self.do_run_datapoints(
            platform, self.small.annual.copy(), True, DataPoint.Type.ANNUAL
        )
        vdf = platform.backend.iamc.datapoints.tabulate_versions()
        expected_versions = pd.DataFrame(
            [
                [
                    0.5,
                    "ANNUAL",
                    None,
                    2000,
                    None,
                    1,
                    1,
                    22,
                    26.0,
                    0,
                    "Region 1",
                    "Unit 1",
                    "Variable 1",
                ],
                [
                    1.0,
                    "ANNUAL",
                    None,
                    2010,
                    None,
                    2,
                    2,
                    22,
                    26.0,
                    0,
                    "Region 1",
                    "Unit 2",
                    "Variable 1",
                ],
                [
                    1.5,
                    "ANNUAL",
                    None,
                    2020,
                    None,
                    3,
                    3,
                    22,
                    32.0,
                    0,
                    "Region 3",
                    "Unit 3",
                    "Variable 3",
                ],
                [
                    1.7,
                    "ANNUAL",
                    None,
                    2020,
                    None,
                    4,
                    4,
                    22,
                    32.0,
                    0,
                    "Region 3",
                    "Unit 2",
                    "Variable 4",
                ],
                [
                    0.5,
                    "ANNUAL",
                    None,
                    2000,
                    None,
                    1,
                    1,
                    26,
                    None,
                    2,
                    "Region 1",
                    "Unit 1",
                    "Variable 1",
                ],
                [
                    1.0,
                    "ANNUAL",
                    None,
                    2010,
                    None,
                    2,
                    2,
                    26,
                    None,
                    2,
                    "Region 1",
                    "Unit 2",
                    "Variable 1",
                ],
                [
                    -9.9,
                    "ANNUAL",
                    None,
                    2000,
                    None,
                    5,
                    5,
                    31,
                    35.0,
                    0,
                    "Region 1",
                    "Unit 1",
                    "Variable 1",
                ],
                [
                    -9.9,
                    "ANNUAL",
                    None,
                    2010,
                    None,
                    6,
                    6,
                    31,
                    35.0,
                    0,
                    "Region 1",
                    "Unit 2",
                    "Variable 1",
                ],
                [
                    -9.9,
                    "ANNUAL",
                    None,
                    2020,
                    None,
                    3,
                    3,
                    32,
                    35.0,
                    1,
                    "Region 3",
                    "Unit 3",
                    "Variable 3",
                ],
                [
                    -9.9,
                    "ANNUAL",
                    None,
                    2020,
                    None,
                    4,
                    4,
                    32,
                    35.0,
                    1,
                    "Region 3",
                    "Unit 2",
                    "Variable 4",
                ],
                [
                    -9.9,
                    "ANNUAL",
                    None,
                    2000,
                    None,
                    5,
                    5,
                    35,
                    None,
                    2,
                    "Region 1",
                    "Unit 1",
                    "Variable 1",
                ],
                [
                    -9.9,
                    "ANNUAL",
                    None,
                    2010,
                    None,
                    6,
                    6,
                    35,
                    None,
                    2,
                    "Region 1",
                    "Unit 2",
                    "Variable 1",
                ],
                [
                    -9.9,
                    "ANNUAL",
                    None,
                    2020,
                    None,
                    3,
                    3,
                    35,
                    None,
                    2,
                    "Region 3",
                    "Unit 3",
                    "Variable 3",
                ],
                [
                    -9.9,
                    "ANNUAL",
                    None,
                    2020,
                    None,
                    4,
                    4,
                    35,
                    None,
                    2,
                    "Region 3",
                    "Unit 2",
                    "Variable 4",
                ],
            ],
            columns=[
                "value",
                "type",
                "step_category",
                "step_year",
                "step_datetime",
                "time_series__id",
                "id",
                "transaction_id",
                "end_transaction_id",
                "operation_type",
                "region",
                "unit",
                "variable",
            ],
        ).replace({np.nan: None})
        assert_unordered_equality(expected_versions, vdf, check_dtype=False)

    def test_iamc_rollback(self, platform: ixmp4.Platform) -> None:
        self.small.load_regions(platform)
        self.small.load_units(platform)
        data = self.small.annual.copy().rename(columns={"step_year": "year"})

        run = platform.runs.create("Model", "Scenario")

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
                raise CustomException("Whoops!!!")
        except CustomException:
            pass

        ret = run.iamc.tabulate()
        assert_unordered_equality(data, ret, check_like=True)

        with run.transact("Partial Removal"):
            run.iamc.remove(remove_data)

        update_data = data.copy()
        update_data["value"] = -9.9

        try:
            with run.transact("Partial Update / Partial Addition Failure"):
                run.iamc.add(update_data)
                raise CustomException("Whoops!!!")
        except CustomException:
            pass

        remaining_data = data.tail(len(data) // 2)
        ret = run.iamc.tabulate()
        assert_unordered_equality(remaining_data, ret, check_like=True)

    def test_iamc_rollback_to_checkpoint(self, platform: ixmp4.Platform) -> None:
        self.small.load_regions(platform)
        self.small.load_units(platform)

        data = self.small.annual.copy().rename(columns={"step_year": "year"})
        remove_data = data.head(len(data) // 2).drop(columns=["value"])

        run = platform.runs.create("Model", "Scenario")

        try:
            with run.transact("Full Addition / Partial Removal"):
                run.iamc.add(data)
                run.checkpoints.create("Full Addition")
                run.iamc.remove(remove_data)
                raise CustomException("Whoops!!!")
        except CustomException:
            pass

        ret = run.iamc.tabulate()
        assert_unordered_equality(data, ret, check_like=True)

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
