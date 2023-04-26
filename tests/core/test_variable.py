# Variable tests are currently disabled

import pytest
import pandas as pd

import ixmp4
from ixmp4 import Variable


from ..utils import assert_unordered_equality, all_platforms


@all_platforms
def test_retrieve_iamc_variable(test_mp):
    iamc_variable1 = test_mp.iamc.variables.create("IAMC Variable")
    test_mp.regions.create("Region", "default")
    test_mp.units.create("Unit")

    variable_data = pd.DataFrame(
        [
            ["Region", "IAMC Variable", "Unit", 2005, 1],
        ],
        columns=["region", "variable", "unit", "step_year", "value"],
    )
    run = test_mp.Run("Model", "Scenario", "new")
    run.iamc.add(variable_data, type=ixmp4.DataPoint.Type.ANNUAL)
    run.set_as_default()

    iamc_variable2 = test_mp.iamc.variables.get("IAMC Variable")

    assert iamc_variable1.id == iamc_variable2.id


@all_platforms
def test_iamc_variable_unqiue(test_mp):
    test_mp.iamc.variables.create("IAMC Variable")

    with pytest.raises(Variable.NotUnique):
        test_mp.iamc.variables.create("IAMC Variable")


def create_testcase_iamc_variables(test_mp):
    test_mp.regions.create("Region", "default")
    test_mp.units.create("Unit")

    iamc_variable = test_mp.iamc.variables.create("IAMC Variable")
    iamc_variable2 = test_mp.iamc.variables.create("IAMC Variable 2")

    variable_data = pd.DataFrame(
        [
            ["Region", "IAMC Variable", "Unit", 2005, 1],
            ["Region", "IAMC Variable 2", "Unit", 2010, 1.0],
        ],
        columns=["region", "variable", "unit", "step_year", "value"],
    )
    run = test_mp.Run("Model", "Scenario", "new")
    run.iamc.add(variable_data, type=ixmp4.DataPoint.Type.ANNUAL)
    run.set_as_default()

    return iamc_variable, iamc_variable2


@all_platforms
def test_list_iamc_variable(test_mp):
    iamc_variables = create_testcase_iamc_variables(test_mp)
    iamc_variable, _ = iamc_variables

    a = [v.id for v in iamc_variables]
    b = [v.id for v in test_mp.iamc.variables.list()]
    assert not (set(a) ^ set(b))

    a = [iamc_variable.id]
    b = [v.id for v in test_mp.iamc.variables.list(name="IAMC Variable")]
    assert not (set(a) ^ set(b))


def df_from_list(iamc_variables):
    return pd.DataFrame(
        [[v.id, v.name, v.created_at, v.created_by] for v in iamc_variables],
        columns=["id", "name", "created_at", "created_by"],
    )


@all_platforms
def test_tabulate_iamc_variable(test_mp):
    iamc_variables = create_testcase_iamc_variables(test_mp)
    iamc_variable, _ = iamc_variables

    a = df_from_list(iamc_variables)
    b = test_mp.iamc.variables.tabulate()
    assert_unordered_equality(a, b, check_dtype=False)

    a = df_from_list([iamc_variable])
    b = test_mp.iamc.variables.tabulate(name="IAMC Variable")
    assert_unordered_equality(a, b, check_dtype=False)


@all_platforms
def test_retrieve_docs(test_mp):
    _, iamc_variable2 = create_testcase_iamc_variables(test_mp)
    docs_iamc_variable1 = test_mp.iamc.variables.set_docs(
        "IAMC Variable", "Description of test IAMC Variable"
    )
    docs_iamc_variable2 = test_mp.iamc.variables.get_docs("IAMC Variable")

    assert docs_iamc_variable1 == docs_iamc_variable2

    assert iamc_variable2.docs is None

    iamc_variable2.docs = "Description of test IAMC Variable 2"

    assert test_mp.iamc.variables.get_docs("IAMC Variable 2") == iamc_variable2.docs


@all_platforms
def test_delete_docs(test_mp):
    iamc_variable, _ = create_testcase_iamc_variables(test_mp)
    iamc_variable.docs = "Description of test IAMC Variable"
    iamc_variable.docs = None

    assert iamc_variable.docs is None

    iamc_variable.docs = "Second description of test IAMC Variable"
    del iamc_variable.docs

    assert iamc_variable.docs is None

    iamc_variable.docs = "Third description of test IAMC Variable"
    test_mp.iamc.variables.delete_docs("IAMC Variable")

    assert iamc_variable.docs is None
