import pandas as pd
import pandas.testing as pdt
import pytest

from ixmp4 import DataPoint, Run, IxmpError, InconsistentIamcType

from ..utils import add_regions, add_units, assert_unordered_equality, all_platforms


def _expected_runs_table(*row_default):
    rows = []
    for (i, default) in enumerate(row_default, start=1):
        if default is not None:
            rows.append([i, "Model", "Scenario", i] + [default])

    return pd.DataFrame(
        rows, columns=["id", "model", "scenario", "version", "is_default"]
    )


@all_platforms
def test_run_versions(test_mp):
    run1 = test_mp.Run("Model", "Scenario", version="new")
    run2 = test_mp.Run("Model", "Scenario", version="new")

    assert run1.id != run2.id

    # no default version is assigned, so list & tabulate are empty
    with pytest.raises(Run.NoDefaultVersion):
        run = test_mp.Run("Model", "Scenario")
    assert test_mp.runs.list() == []
    assert test_mp.runs.tabulate().empty

    run_list = test_mp.runs.list(default_only=False)
    assert len(run_list) == 2
    assert run_list[0].id == run1.id
    pdt.assert_frame_equal(
        test_mp.runs.tabulate(default_only=False),
        pd.DataFrame(_expected_runs_table(False, False)),
    )

    # set default, so list & tabulate show default version only
    run1.set_as_default()
    run_list = test_mp.runs.list()
    assert len(run_list) == 1
    assert run_list[0].id == run1.id
    pdt.assert_frame_equal(
        test_mp.runs.tabulate(),
        pd.DataFrame(_expected_runs_table(True)),
    )

    # using default_only=False shows both versions
    pdt.assert_frame_equal(
        test_mp.runs.tabulate(default_only=False),
        pd.DataFrame(_expected_runs_table(True, False)),
    )

    # default version can be retrieved directly
    run = test_mp.Run("Model", "Scenario")
    assert run1.id == run.id

    # default version can be changed
    run2.set_as_default()
    run = test_mp.Run("Model", "Scenario")
    assert run2.id == run.id

    # list shows changed default version only
    run_list = test_mp.runs.list()
    assert len(run_list) == 1
    assert run_list[0].id == run2.id
    pdt.assert_frame_equal(
        test_mp.runs.tabulate(),
        pd.DataFrame(_expected_runs_table(None, True)),
    )

    # unsetting default means run cannot be retrieved directly
    run2.unset_as_default()
    with pytest.raises(Run.NoDefaultVersion):
        test_mp.Run("Model", "Scenario")

    # non-default version cannot be again set as un-default
    with pytest.raises(IxmpError):
        run2.unset_as_default()


@all_platforms
def test_run_annual_datapoints(test_mp, test_data_annual):
    do_run_datapoints(test_mp, test_data_annual, DataPoint.Type.ANNUAL)


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
def test_run_tabulate(test_mp, test_data_annual):
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


def do_run_datapoints(test_mp, test_data, type):
    add_regions(test_mp, test_data["region"].unique())
    add_units(test_mp, test_data["unit"].unique())

    run = test_mp.Run("Model", "Scenario", version="new")

    # == Full Addition ==
    # Save to database
    run.iamc.add(test_data, type=type)

    # Retrieve from database
    ret = run.iamc.tabulate()
    ret = ret.drop(columns=["id"])

    if type is not None:
        ret = ret.drop(columns=["type"])

    assert_unordered_equality(test_data, ret, check_like=True)

    # == Partial Removal ==
    # Remove half the data
    remove_data = test_data.head(len(test_data) // 2).drop(columns=["value"])
    remaining_data = test_data.tail(len(test_data) // 2).reset_index(drop=True)
    run.iamc.remove(remove_data, type=type)

    # Retrieve from database
    ret = run.iamc.tabulate()
    ret = ret.drop(columns=["id"])

    if type is not None:
        ret = ret.drop(columns=["type"])

    assert_unordered_equality(remaining_data, ret, check_like=True)

    # == Partial Update / Partial Addition ==
    # Update all data values
    test_data["value"] = -9.9

    # Results in a half insert / half update
    run.iamc.add(test_data, type=type)

    # Retrieve from database
    ret = run.iamc.tabulate()
    ret = ret.drop(columns=["id"])

    if type is not None:
        ret = ret.drop(columns=["type"])

    assert_unordered_equality(test_data, ret, check_like=True)

    # == Full Removal ==
    # Remove all data
    remove_data = test_data.drop(columns=["value"])
    run.iamc.remove(remove_data, type=type)

    # Retrieve from database
    ret = run.iamc.tabulate()
    assert ret.empty
