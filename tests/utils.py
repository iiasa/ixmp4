import pandas as pd
import pandas.testing as pdt
import pytest

from ixmp4 import DataPoint
from ixmp4.core import Platform
from ixmp4.data.abstract import IndexSet

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


def create_indexsets_for_run(
    platform: Platform, run_id: int, amount: int = 2, offset: int = 1
) -> tuple[IndexSet, ...]:
    """Create `amount` indexsets called `Indexset n` for `run`."""
    return tuple(
        platform.backend.optimization.indexsets.create(
            run_id=run_id, name=f"Indexset {i}"
        )
        for i in range(offset, offset + amount)
    )
