import pytest

from ixmp4 import DataPoint, InconsistentIamcType

from ..utils import add_regions, add_units, assert_unordered_equality, all_platforms


@all_platforms
def test_run_annual_datapoints(test_mp, test_data_annual):
    do_run_datapoints(test_mp, test_data_annual, DataPoint.Type.ANNUAL)


@all_platforms
def test_run_annual_datapoints_from_pyam(test_mp, test_data_annual):
    # convert to pyam.data format
    df = test_data_annual.rename(columns={"step_year": "year"})
    do_run_datapoints(test_mp, test_data_annual, arg_data=df)


@all_platforms
@pytest.mark.parametrize("_type", (DataPoint.Type.CATEGORICAL, DataPoint.Type.DATETIME))
def test_run_inconsistent_annual_raises(test_mp, test_data_annual, _type):
    with pytest.raises(InconsistentIamcType):
        do_run_datapoints(test_mp, test_data_annual, _type)


@all_platforms
def test_run_categorical_datapoints(test_mp, test_data_categorical):
    do_run_datapoints(test_mp, test_data_categorical, DataPoint.Type.CATEGORICAL)


@all_platforms
@pytest.mark.parametrize("_type", (DataPoint.Type.ANNUAL, DataPoint.Type.DATETIME))
def test_run_inconsistent_categorical_raises(test_mp, test_data_categorical, _type):
    with pytest.raises(InconsistentIamcType):
        do_run_datapoints(test_mp, test_data_categorical, _type)


@all_platforms
def test_run_datetime_datapoints(test_mp, test_data_datetime):
    do_run_datapoints(test_mp, test_data_datetime, DataPoint.Type.DATETIME)


@all_platforms
@pytest.mark.parametrize("_type", (DataPoint.Type.ANNUAL, DataPoint.Type.CATEGORICAL))
def test_run_inconsistent_datetime_type_raises(test_mp, test_data_datetime, _type):
    with pytest.raises(InconsistentIamcType):
        do_run_datapoints(test_mp, test_data_datetime, _type)


@all_platforms
def test_unit_dimensionless(test_mp, test_data_annual):
    test_data_annual.loc[0, "unit"] = ""
    do_run_datapoints(test_mp, test_data_annual, DataPoint.Type.ANNUAL)


@all_platforms
def test_unit_as_string_dimensionless_raises(test_mp, test_data_annual):
    test_data_annual.loc[0, "unit"] = "dimensionless"
    with pytest.raises(ValueError, match="Unit name 'dimensionless' is reserved,"):
        do_run_datapoints(test_mp, test_data_annual, DataPoint.Type.ANNUAL)


@all_platforms
def test_run_tabulate_with_filter(test_mp, test_data_annual):
    # Filter run directly
    add_regions(test_mp, test_data_annual["region"].unique())
    add_units(test_mp, test_data_annual["unit"].unique())

    run = test_mp.Run("Model", "Scenario", version="new")
    run.iamc.add(test_data_annual, type=DataPoint.Type.ANNUAL)
    obs = run.iamc.tabulate(
        variable={"name": "Primary Energy"}, unit={"name": "EJ/yr"}
    ).drop(["id", "type"], axis=1)
    exp = test_data_annual[test_data_annual.variable == "Primary Energy"]
    assert_unordered_equality(obs, exp, check_like=True)


def do_run_datapoints(test_mp, ixmp_data, type=None, arg_data=None):
    # ixmp_data: expected return format from Run.iamc.tabulate() (column names 'step_*')
    # arg_data: passed to Run.iamc.[add/remove](),
    # can be ixmp4 or pyam format (column names 'year' or 'time')

    if arg_data is None:
        arg_data = ixmp_data.copy()

    # Define required regions and units in the database
    add_regions(test_mp, ixmp_data["region"].unique())
    add_units(test_mp, ixmp_data["unit"].unique())

    run = test_mp.Run("Model", "Scenario", version="new")

    # == Full Addition ==
    # Save to database
    run.iamc.add(arg_data, type=type)

    # Retrieve from database via Run
    ret = run.iamc.tabulate()
    ret = ret.drop(columns=["id", "type"])
    assert_unordered_equality(ixmp_data, ret, check_like=True)

    # If not set as default, retrieve from database via Platform returns an empty frame
    ret = test_mp.iamc.tabulate()
    assert ret.empty

    # Retrieve from database via Platform (including model, scenario, version columns)
    ret = test_mp.iamc.tabulate(run={"default_only": False})
    ret = ret.drop(columns=["id", "type"])

    test_mp_data = ixmp_data.copy()
    test_mp_data["model"] = run.model.name
    test_mp_data["scenario"] = run.scenario.name
    test_mp_data["version"] = run.version
    test_mp_data = test_mp_data[ret.columns]
    assert_unordered_equality(test_mp_data, ret, check_like=True)

    # Retrieve from database after setting the run to default
    run.set_as_default()
    ret = test_mp.iamc.tabulate()
    ret = ret.drop(columns=["id", "type"])
    assert_unordered_equality(test_mp_data, ret, check_like=True)

    # == Partial Removal ==
    # Remove half the data
    remove_data = arg_data.head(len(ixmp_data) // 2).drop(columns=["value"])
    remaining_data = ixmp_data.tail(len(ixmp_data) // 2).reset_index(drop=True)
    run.iamc.remove(remove_data, type=type)

    # Retrieve from database
    ret = run.iamc.tabulate()
    ret = ret.drop(columns=["id", "type"])
    assert_unordered_equality(remaining_data, ret, check_like=True)

    # == Partial Update / Partial Addition ==
    # Update all data values
    ixmp_data["value"] = -9.9
    arg_data["value"] = -9.9

    # Results in a half insert / half update
    run.iamc.add(arg_data, type=type)

    # Retrieve from database
    ret = run.iamc.tabulate()
    ret = ret.drop(columns=["id", "type"])
    assert_unordered_equality(ixmp_data, ret, check_like=True)

    # == Full Removal ==
    # Remove all data
    remove_data = arg_data.drop(columns=["value"])
    run.iamc.remove(remove_data, type=type)

    # Retrieve from database
    ret = run.iamc.tabulate()
    assert ret.empty
