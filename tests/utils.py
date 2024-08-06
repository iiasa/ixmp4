import pandas as pd
import pandas.testing as pdt
import pytest

from ixmp4 import DataPoint
from ixmp4.core import Platform, Run, Unit

from .conftest import SKIP_PGSQL_TESTS


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


all_platforms = pytest.mark.parametrize(
    "test_mp",
    [
        "test_sqlite_mp",
        pytest.param(
            "test_pgsql_mp",
            marks=pytest.mark.skipif(
                SKIP_PGSQL_TESTS,
                reason="Cannot connect to PostgreSQL database service, skipping test",
            ),
        ),
        "test_api_sqlite_mp",
        pytest.param(
            "test_api_pgsql_mp",
            marks=pytest.mark.skipif(
                SKIP_PGSQL_TESTS,
                reason="Cannot connect to PostgreSQL database service, skipping test",
            ),
        ),
    ],
)

generated_platforms = pytest.mark.parametrize(
    "generated_mp",
    [
        "test_sqlite_mp_generated",
        pytest.param(
            "test_pgsql_mp_generated",
            marks=pytest.mark.skipif(
                SKIP_PGSQL_TESTS,
                reason="Cannot connect to PostgreSQL database service, skipping test",
            ),
        ),
        "test_api_sqlite_mp_generated",
        pytest.param(
            "test_api_pgsql_mp_generated",
            marks=pytest.mark.skipif(
                SKIP_PGSQL_TESTS,
                reason="Cannot connect to PostgreSQL database service, skipping test",
            ),
        ),
    ],
)

generated_db_platforms = pytest.mark.parametrize(
    "generated_mp",
    [
        "test_sqlite_mp_generated",
        pytest.param(
            "test_pgsql_mp_generated",
            marks=pytest.mark.skipif(
                SKIP_PGSQL_TESTS,
                reason="Cannot connect to PostgreSQL database service, skipping test",
            ),
        ),
    ],
)


generated_api_platforms = pytest.mark.parametrize(
    "generated_mp",
    [
        "test_api_sqlite_mp_generated",
        pytest.param(
            "test_api_pgsql_mp_generated",
            marks=pytest.mark.skipif(
                SKIP_PGSQL_TESTS,
                reason="Cannot connect to PostgreSQL database service, skipping test",
            ),
        ),
    ],
)

api_platforms = pytest.mark.parametrize(
    "test_mp",
    [
        "test_api_sqlite_mp",
        pytest.param(
            "test_api_pgsql_mp",
            marks=pytest.mark.skipif(
                SKIP_PGSQL_TESTS,
                reason="Cannot connect to PostgreSQL database service, skipping test",
            ),
        ),
    ],
)

database_platforms = pytest.mark.parametrize(
    "test_mp",
    [
        "test_sqlite_mp",
        pytest.param(
            "test_pgsql_mp",
            marks=pytest.mark.skipif(
                SKIP_PGSQL_TESTS,
                reason="Cannot connect to PostgreSQL database service, skipping test",
            ),
        ),
    ],
)


def create_filter_test_data(test_mp):
    for i in range(1, 7):
        test_mp.backend.regions.create(f"Region {str(i)}", "Hierarchy")
    test_mp.backend.regions.create("Region 7", "Hierarchy")

    for i in range(1, 5):
        test_mp.backend.units.create(f"Unit {str(i)}")
    test_mp.backend.units.create("Unit 5")

    run1 = test_mp.runs.create("Model 1", "Scenario 1")
    run1.set_as_default()
    run1_data = pd.DataFrame(
        [
            ["Region 1", "Variable 1", "Unit 1", 2020, 1],
            ["Region 2", "Variable 2", "Unit 1", 2010, 6.0],
            ["Region 3", "Variable 1", "Unit 2", 2000, 0.5],
            ["Region 4", "Variable 2", "Unit 2", 1990, 3],
        ],
        columns=["region", "variable", "unit", "step_year", "value"],
    )
    run1.iamc.add(run1_data, type=DataPoint.Type.ANNUAL)

    run2 = test_mp.runs.create("Model 1", "Scenario 1")
    run2_data = pd.DataFrame(
        [
            ["Region 3", "Variable 1", "Unit 3", 2020, 1],
            ["Region 4", "Variable 2", "Unit 3", 2010, 6.0],
            ["Region 5", "Variable 1", "Unit 4", 2000, 0.5],
            ["Region 6", "Variable 2", "Unit 4", 1990, 3],
        ],
        columns=["region", "variable", "unit", "step_year", "value"],
    )
    run2.iamc.add(run2_data, type=DataPoint.Type.ANNUAL)

    run3 = test_mp.runs.create("Model 2", "Scenario 2")
    run3.iamc.add(run2_data, type=DataPoint.Type.ANNUAL)
    run3.set_as_default()

    test_mp.runs.create("Model 3", "Scenario 3")

    return run1, run2


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


def create_dantzig_run(mp: Platform) -> Run:
    """Create a Run for the transport tutorial.

    Please see the tutorial file for explanation.
    """
    # Only needed once for each mp
    try:
        cases = mp.units.get("cases")
        km = mp.units.get("km")
        unit_cost_per_case = mp.units.get("USD/km")
    except Unit.NotFound:
        cases = mp.units.create("cases")
        km = mp.units.create("km")
        unit_cost_per_case = mp.units.create("USD/km")

    # Create run and all data sets
    run = mp.runs.create(model="transport problem", scenario="standard")
    a_data = {
        "i": ["seattle", "san-diego"],
        "values": [350, 600],
        "units": [cases.name, cases.name],
    }
    b_data = pd.DataFrame(
        [
            ["new-york", 325, cases.name],
            ["chicago", 300, cases.name],
            ["topeka", 275, cases.name],
        ],
        columns=["j", "values", "units"],
    )
    d_data = {
        "i": ["seattle", "seattle", "seattle", "san-diego", "san-diego", "san-diego"],
        "j": ["new-york", "chicago", "topeka", "new-york", "chicago", "topeka"],
        "values": [2.5, 1.7, 1.8, 2.5, 1.8, 1.4],
        "units": [km.name] * 6,
    }

    # Add all data to the run
    run.optimization.indexsets.create("i").add(["seattle", "san-diego"])
    run.optimization.indexsets.create("j").add(["new-york", "chicago", "topeka"])
    run.optimization.parameters.create(name="a", constrained_to_indexsets=["i"]).add(
        data=a_data
    )
    run.optimization.parameters.create("b", constrained_to_indexsets=["j"]).add(
        data=b_data
    )
    run.optimization.parameters.create("d", constrained_to_indexsets=["i", "j"]).add(
        data=d_data
    )
    run.optimization.scalars.create(name="f", value=90, unit=unit_cost_per_case)

    # Create further optimization items to store solution data
    run.optimization.variables.create("z")
    run.optimization.variables.create("x", constrained_to_indexsets=["i", "j"])
    run.optimization.equations.create("supply", constrained_to_indexsets=["i"])
    run.optimization.equations.create("demand", constrained_to_indexsets=["j"])

    return run
