import pandas as pd
import pandas.testing as pdt

from ixmp4 import DataPoint


def add_regions(mp, regions):
    for region in regions:
        mp.regions.create(region, hierarchy="default")


def add_units(mp, units):
    for unit in units:
        mp.units.create(unit)


def assert_unordered_equality(df1, df2, **kwargs):
    df1 = df1.sort_index(axis=1)
    df1 = df1.sort_values(by=list(df1.columns)).reset_index(drop=True)
    df2 = df2.sort_index(axis=1)
    df2 = df2.sort_values(by=list(df2.columns)).reset_index(drop=True)
    pdt.assert_frame_equal(df1, df2, **kwargs)


def create_iamc_query_test_data(test_mp):
    run1 = test_mp.runs.create("Model 1", "Scenario 1")
    run1.set_as_default()
    run2 = test_mp.runs.create("Model 2", "Scenario 2")
    run2.set_as_default()
    r1 = test_mp.backend.regions.create("Region 1", "Hierarchy")
    r2 = test_mp.backend.regions.create("Region 2", "Hierarchy")
    r3 = test_mp.backend.regions.create("Region 3", "Different Hierarchy")
    _ = test_mp.backend.regions.create("Region 4", "Hierarchy")

    units = [test_mp.backend.units.create(f"Unit {str(i)}") for i in range(1, 4)]

    dps = pd.DataFrame(
        [
            ["Region 1", "Variable 1", "Unit 1", 2000, 0.5],
            ["Region 2", "Variable 2", "Unit 2", 2010, 1.0],
            ["Region 3", "Variable 3", "Unit 3", 2020, 1.5],
        ],
        columns=["region", "variable", "unit", "step_year", "value"],
    )

    run1.iamc.add(dps, type=DataPoint.Type.ANNUAL)
    run1.meta = {"run": 1, "test": 0.1293, "bool": True}
    dps["variable"] = "Variable 4"
    run2.iamc.add(dps, type=DataPoint.Type.ANNUAL)
    run2.meta = {"run": 2, "test": "string", "bool": False}

    return [r1, r2, r3], units
