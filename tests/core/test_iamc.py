import pytest

from ixmp4 import DataPoint
from ixmp4.core.exceptions import SchemaError

from ..utils import add_regions, add_units, all_platforms, assert_unordered_equality


@all_platforms
def test_run_annual_datapoints_raw(test_mp, test_data_annual, request):
    test_mp = request.getfixturevalue(test_mp)
    do_run_datapoints(test_mp, test_data_annual, True, DataPoint.Type.ANNUAL)


@all_platforms
def test_run_annual_datapoints_iamc(test_mp, test_data_annual, request):
    test_mp = request.getfixturevalue(test_mp)
    # convert to test data to standard IAMC format
    df = test_data_annual.rename(columns={"step_year": "year"})
    do_run_datapoints(test_mp, df, False)


@all_platforms
@pytest.mark.parametrize("_type", (DataPoint.Type.CATEGORICAL, DataPoint.Type.DATETIME))
def test_run_inconsistent_annual_raises(test_mp, test_data_annual, _type, request):
    test_mp = request.getfixturevalue(test_mp)
    with pytest.raises(SchemaError):
        do_run_datapoints(test_mp, test_data_annual, True, _type)


@all_platforms
def test_run_categorical_datapoints_raw(test_mp, test_data_categorical, request):
    test_mp = request.getfixturevalue(test_mp)
    do_run_datapoints(test_mp, test_data_categorical, True, DataPoint.Type.CATEGORICAL)


@all_platforms
@pytest.mark.parametrize("_type", (DataPoint.Type.ANNUAL, DataPoint.Type.DATETIME))
def test_run_inconsistent_categorical_raises(
    test_mp, test_data_categorical, _type, request
):
    test_mp = request.getfixturevalue(test_mp)
    with pytest.raises(SchemaError):
        do_run_datapoints(test_mp, test_data_categorical, True, _type)


@all_platforms
def test_run_datetime_datapoints_raw(test_mp, test_data_datetime, request):
    test_mp = request.getfixturevalue(test_mp)
    do_run_datapoints(test_mp, test_data_datetime, True, DataPoint.Type.DATETIME)


@all_platforms
@pytest.mark.parametrize("_type", (DataPoint.Type.ANNUAL, DataPoint.Type.CATEGORICAL))
def test_run_inconsistent_datetime_type_raises(
    test_mp, test_data_datetime, _type, request
):
    test_mp = request.getfixturevalue(test_mp)
    with pytest.raises(SchemaError):
        do_run_datapoints(test_mp, test_data_datetime, True, _type)


@all_platforms
def test_unit_dimensionless_raw(test_mp, test_data_annual, request):
    test_mp = request.getfixturevalue(test_mp)
    test_data_annual.loc[0, "unit"] = ""
    do_run_datapoints(test_mp, test_data_annual, True, DataPoint.Type.ANNUAL)


@all_platforms
def test_unit_as_string_dimensionless_raises(test_mp, test_data_annual, request):
    test_mp = request.getfixturevalue(test_mp)
    test_data_annual.loc[0, "unit"] = "dimensionless"
    with pytest.raises(ValueError, match="Unit name 'dimensionless' is reserved,"):
        do_run_datapoints(test_mp, test_data_annual, DataPoint.Type.ANNUAL)


@all_platforms
@pytest.mark.parametrize(
    "filters",
    (
        dict(variable={"name": "Primary Energy"}),
        dict(variable={"name": "Primary Energy"}, unit={"name": "EJ/yr"}),
        dict(variable={"name__like": "* Energy"}, unit={"name": "EJ/yr"}),
        dict(variable={"name__in": ["Primary Energy", "Some Other Variable"]}),
        dict(variable="Primary Energy"),
        dict(variable="Primary Energy", unit="EJ/yr"),
        dict(variable="* Energy", unit="EJ/yr"),
        dict(variable=["Primary Energy", "Some Other Variable"]),
    ),
)
def test_run_tabulate_with_filter_raw(test_mp, test_data_annual, request, filters):
    test_mp = request.getfixturevalue(test_mp)
    # Filter run directly
    add_regions(test_mp, test_data_annual["region"].unique())
    add_units(test_mp, test_data_annual["unit"].unique())

    run = test_mp.runs.create("Model", "Scenario")
    run.iamc.add(test_data_annual, type=DataPoint.Type.ANNUAL)
    obs = run.iamc.tabulate(raw=True, **filters).drop(["id", "type"], axis=1)
    exp = test_data_annual[test_data_annual.variable == "Primary Energy"]
    assert_unordered_equality(obs, exp, check_like=True)


def do_run_datapoints(test_mp, data, raw=True, _type=None):
    # Test adding, updating, removing data to a run
    # either as ixmp4-database format (columns `step_[year/datetime/categorical]`)
    # or as standard iamc format  (column names 'year' or 'time')

    # Define required regions and units in the database
    add_regions(test_mp, data["region"].unique())
    add_units(test_mp, data["unit"].unique())

    run = test_mp.runs.create("Model", "Scenario")

    # == Full Addition ==
    # Save to database
    run.iamc.add(data, type=_type)

    # Retrieve from database via Run
    ret = run.iamc.tabulate(raw=raw)
    if raw:
        ret = ret.drop(columns=["id", "type"])
    assert_unordered_equality(data, ret, check_like=True)

    # If not set as default, retrieve from database via Platform returns an empty frame
    ret = test_mp.iamc.tabulate(raw=raw)
    assert ret.empty

    # Retrieve from database via Platform (including model, scenario, version columns)
    ret = test_mp.iamc.tabulate(raw=raw, run={"default_only": False})
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
    ret = test_mp.iamc.tabulate(raw=raw)
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

    ts_after_delete = test_mp.backend.iamc.timeseries.tabulate(join_parameters=True)
    all_dp_after_delete = test_mp.backend.iamc.datapoints.tabulate()
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
