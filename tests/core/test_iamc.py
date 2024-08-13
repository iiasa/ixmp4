import asyncio

import pytest

import ixmp4
from ixmp4 import DataPoint
from ixmp4.conf import settings
from ixmp4.core.exceptions import SchemaError

from ..fixtures import FilterIamcDataset, MediumIamcDataset, SmallIamcDataset
from ..utils import (
    assert_unordered_equality,
)


class TestCoreIamc:
    small = SmallIamcDataset()
    filter = FilterIamcDataset()

    def test_run_annual_datapoints_raw(self, platform: ixmp4.Platform):
        self.do_run_datapoints(
            platform, self.small.annual.copy(), True, DataPoint.Type.ANNUAL
        )

    def test_run_annual_datapoints_iamc(self, platform: ixmp4.Platform):
        # convert to test data to standard IAMC format
        df = self.small.annual.copy().rename(columns={"step_year": "year"})
        self.do_run_datapoints(platform, df, False)

    @pytest.mark.parametrize(
        "invalid_type", (DataPoint.Type.CATEGORICAL, DataPoint.Type.DATETIME)
    )
    def test_run_inconsistent_annual_raises(
        self, platform: ixmp4.Platform, invalid_type
    ):
        with pytest.raises(SchemaError):
            self.do_run_datapoints(
                platform, self.small.annual.copy(), True, invalid_type
            )

    def test_run_categorical_datapoints_raw(self, platform: ixmp4.Platform):
        self.do_run_datapoints(
            platform, self.small.categorical.copy(), True, DataPoint.Type.CATEGORICAL
        )

    @pytest.mark.parametrize(
        "invalid_type", (DataPoint.Type.ANNUAL, DataPoint.Type.DATETIME)
    )
    def test_run_inconsistent_categorical_raises(
        self, platform: ixmp4.Platform, invalid_type
    ):
        with pytest.raises(SchemaError):
            self.do_run_datapoints(
                platform, self.small.categorical.copy(), True, invalid_type
            )

    def test_run_datetime_datapoints_raw(self, platform: ixmp4.Platform):
        self.do_run_datapoints(
            platform, self.small.datetime.copy(), True, DataPoint.Type.DATETIME
        )

    @pytest.mark.parametrize(
        "invalid_type", (DataPoint.Type.ANNUAL, DataPoint.Type.CATEGORICAL)
    )
    def test_run_inconsistent_datetime_type_raises(
        self, platform: ixmp4.Platform, invalid_type
    ):
        with pytest.raises(SchemaError):
            self.do_run_datapoints(
                platform, self.small.datetime.copy(), True, invalid_type
            )

    def test_unit_dimensionless_raw(self, platform: ixmp4.Platform):
        test_data = self.small.annual.copy()
        test_data.loc[0, "unit"] = ""
        platform.units.create("")
        self.do_run_datapoints(platform, test_data, True, DataPoint.Type.ANNUAL)

    def do_run_datapoints(self, platform: ixmp4.Platform, data, raw=True, _type=None):
        # Test adding, updating, removing data to a run
        # either as ixmp4-database format (columns `step_[year/datetime/categorical]`)
        # or as standard iamc format  (column names 'year' or 'time')

        # Define required regions and units in the database
        self.small.load_regions(platform)
        self.small.load_units(platform)

        run = platform.runs.create("Model", "Scenario")

        # == Full Addition ==
        # Save to database
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
        self, platform: ixmp4.Platform, filters, run, exp_len
    ):
        self.filter.load_dataset(platform)
        run = platform.runs.get(*run)
        obs = run.iamc.tabulate(raw=True, **filters)
        assert len(obs) == exp_len


class TestCoreIamcReadOnly:
    medium = MediumIamcDataset()

    def test_mp_tabulate_big_async(self, platform_med: ixmp4.Platform):
        """Tests if big tabulations work in async contexts."""

        async def tabulate():
            return platform_med.iamc.tabulate(raw=True, run={"default_only": False})

        res = asyncio.run(tabulate())
        assert len(res) > settings.default_page_size

    def test_mp_tabulate_big(self, platform_med: ixmp4.Platform):
        res = platform_med.iamc.tabulate(raw=True, run={"default_only": False})
        assert len(res) > settings.default_page_size
