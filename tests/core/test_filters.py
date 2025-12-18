"""This module only contains benchmarks, no assertions are made to validate the
results."""

import pandas as pd
import pytest

import ixmp4
from tests import backends
from tests.fixtures import get_csv_data

platform = backends.get_platform_fixture(scope="class")


@pytest.fixture(scope="session")
def regions() -> pd.DataFrame:
    return get_csv_data("filter", "regions")


@pytest.fixture(scope="session")
def units() -> pd.DataFrame:
    return get_csv_data("filter", "units")


@pytest.fixture(scope="session")
def runs() -> pd.DataFrame:
    return get_csv_data("filter", "runs")


@pytest.fixture(scope="session")
def datapoints() -> pd.DataFrame:
    return get_csv_data("filter", "datapoints")


class TestFilters:
    def test_load_data(
        self,
        platform: ixmp4.Platform,
        regions: pd.DataFrame,
        units: pd.DataFrame,
        runs: pd.DataFrame,
        datapoints: pd.DataFrame,
    ) -> None:
        for name, hierarchy in regions.itertuples(index=False):
            platform.regions.create(name, hierarchy)

        for (name,) in units.itertuples(index=False):
            platform.units.create(name)

        for model, scenario, version, is_default in runs.itertuples(index=False):
            run = platform.runs.create(model, scenario)
            assert run.version == int(version)
            if is_default:
                run.set_as_default()

        for run_tuple, rows in datapoints.groupby(
            ["model", "scenario", "version"], group_keys=False
        ):
            model, scenario, version = run_tuple
            run = platform.runs.get(str(model), str(scenario), int(version))
            with run.transact("Add datapoints"):
                run.iamc.add(rows.drop(columns=["model", "scenario", "version"]))

    def test_filter_regions(self, platform: ixmp4.Platform) -> None:
        [region_no_data] = platform.regions.list(iamc=False)
        assert region_no_data.name == "Region Without Data"

        df_no_data = platform.regions.tabulate(iamc=False)
        assert df_no_data["name"].to_list() == ["Region Without Data"]

        [region1, region10] = platform.regions.list(name__like="Region 1*")
        assert region1.name == "Region 1"
        assert region10.name == "Region 10"

        df_name_like = platform.regions.tabulate(name__like="Region 1*")
        assert df_name_like["name"].to_list() == ["Region 1", "Region 10"]

        df_iamc_var = platform.regions.tabulate(
            iamc={"variable": {"name": "Variable 3"}}
        )
        assert df_iamc_var["name"].to_list() == ["Region 2", "Region 10"]

        df_iamc_unit = platform.regions.tabulate(iamc={"unit": {"name": "Unit 4"}})
        assert df_iamc_unit["name"].to_list() == ["Region 3"]

        [region7] = platform.regions.list(
            iamc={
                "unit": {"name": "Unit 3"},
                "variable": {"name": "Variable 7"},
            }
        )
        assert region7.name == "Region 7"

        [region1, region5, region6, region7, region8, region9, region10] = (
            platform.regions.list(
                iamc={
                    "run": {"default_only": False, "is_default": False},
                }
            )
        )
        assert region1.name == "Region 1"
        assert region5.name == "Region 5"
        assert region6.name == "Region 6"
        assert region7.name == "Region 7"
        assert region8.name == "Region 8"
        assert region9.name == "Region 9"
        assert region10.name == "Region 10"

        [region8, region9, region10] = platform.regions.list(
            iamc={"run": {"model": {"name": "Model 2"}, "default_only": False}}
        )
        assert region8.name == "Region 8"
        assert region9.name == "Region 9"
        assert region10.name == "Region 10"

    def test_filter_units(self, platform: ixmp4.Platform) -> None:
        [unit_no_data] = platform.units.list(iamc=False)
        assert unit_no_data.name == "Unit Without Data"

        df_no_data = platform.units.tabulate(iamc=False)
        assert df_no_data["name"].to_list() == ["Unit Without Data"]

        [unit1, unit10] = platform.units.list(name__like="Unit 1*")
        assert unit1.name == "Unit 1"
        assert unit10.name == "Unit 10"

        df_name_like = platform.units.tabulate(name__like="Unit 1*")
        assert df_name_like["name"].to_list() == ["Unit 1", "Unit 10"]

        df_iamc_var = platform.units.tabulate(iamc={"variable": {"name": "Variable 5"}})
        assert df_iamc_var["name"].to_list() == ["Unit 2", "Unit 3", "Unit 7"]

        df_iamc_unit = platform.units.tabulate(iamc={"region": {"name": "Region 5"}})
        assert df_iamc_unit["name"].to_list() == ["Unit 3"]

        [unit3] = platform.units.list(
            iamc={
                "region": {"name": "Region 5"},
                "variable": {"name": "Variable 5"},
            }
        )
        assert unit3.name == "Unit 3"

        [unit1, unit2, unit3, unit6, unit8, unit9, unit10] = platform.units.list(
            iamc={
                "run": {"default_only": False, "is_default": False},
            }
        )
        assert unit1.name == "Unit 1"
        assert unit2.name == "Unit 2"
        assert unit3.name == "Unit 3"
        assert unit6.name == "Unit 6"
        assert unit8.name == "Unit 8"
        assert unit9.name == "Unit 9"
        assert unit10.name == "Unit 10"

        [unit8, unit9, unit10] = platform.units.list(
            iamc={"run": {"model": {"name": "Model 2"}}}
        )
        assert unit8.name == "Unit 8"
        assert unit9.name == "Unit 9"
        assert unit10.name == "Unit 10"

    def test_filter_iamc_variables(self, platform: ixmp4.Platform) -> None:
        [var1, var10] = platform.iamc.variables.list(name__like="Variable 1*")
        assert var1.name == "Variable 1"
        assert var10.name == "Variable 10"

        df_name_like = platform.iamc.variables.tabulate(name__like="Variable 1*")
        assert df_name_like["name"].to_list() == ["Variable 1", "Variable 10"]

        df_unit = platform.iamc.variables.tabulate(unit={"name": "Unit 5"})
        assert df_unit["name"].to_list() == ["Variable 7"]

        df_region = platform.iamc.variables.tabulate(region={"name": "Region 5"})
        assert df_region["name"].to_list() == ["Variable 5"]

        [var1, var2] = platform.iamc.variables.list(
            region={"name": "Region 1"}, unit={"name": "Unit 1"}
        )
        assert var1.name == "Variable 1"
        assert var2.name == "Variable 2"

        [var1, var2, var5, var6, var7, var8, var9, var10] = (
            platform.iamc.variables.list(
                run={"default_only": False, "is_default": False},
            )
        )
        assert var1.name == "Variable 1"
        assert var2.name == "Variable 2"
        assert var5.name == "Variable 5"
        assert var6.name == "Variable 6"
        assert var7.name == "Variable 7"
        assert var8.name == "Variable 8"
        assert var9.name == "Variable 9"
        assert var10.name == "Variable 10"

        [var8, var9, var10] = platform.iamc.variables.list(
            run={"model": {"name": "Model 2"}, "default_only": False}
        )
        assert var8.name == "Variable 8"
        assert var9.name == "Variable 9"
        assert var10.name == "Variable 10"

    def test_filter_datapoints(self, platform: ixmp4.Platform) -> None:
        df = platform.iamc.tabulate()
        assert len(df) == 426

        df_runs = platform.iamc.tabulate(run={"id__in": [1, 7]})
        assert len(df_runs) == 120

        df_scenario2 = platform.iamc.tabulate(scenario={"name": "Scenario 2"})
        assert len(df_scenario2) == 100

        df_model1x = platform.iamc.tabulate(model={"name__like": "Model 1*"})
        assert len(df_model1x) == 244

        df_region7 = platform.iamc.tabulate(region={"name": "Region 7"})
        assert len(df_region7) == 40

        df_unit1x = platform.iamc.tabulate(unit={"name__like": "Unit 1*"})
        assert len(df_unit1x) == 154

        df_iamc_vars = platform.iamc.tabulate(
            variable={"name__in": ["Variable 2", "Variable 8"]}
        )
        assert len(df_iamc_vars) == 91

        df_categorical = platform.iamc.tabulate(type="CATEGORICAL")
        assert len(df_categorical) == 99

        df_category1 = platform.iamc.tabulate(step_category="Category 1")
        assert len(df_category1) == 35

        df_year_gte = platform.iamc.tabulate(step_year__gte=2000)
        assert len(df_year_gte) == 13
