from collections.abc import Iterable

import pandas as pd
import pytest

import ixmp4
from ixmp4.core import Variable

from ..utils import assert_unordered_equality


def create_testcase_iamc_variables(
    platform: ixmp4.Platform,
) -> tuple[Variable, Variable]:
    platform.regions.create("Region", "default")
    platform.units.create("Unit")

    iamc_variable = platform.iamc.variables.create("IAMC Variable")
    iamc_variable2 = platform.iamc.variables.create("IAMC Variable 2")

    variable_data = pd.DataFrame(
        [
            ["Region", "IAMC Variable", "Unit", 2005, 1],
            ["Region", "IAMC Variable 2", "Unit", 2010, 1.0],
        ],
        columns=["region", "variable", "unit", "step_year", "value"],
    )
    run = platform.runs.create("Model", "Scenario")
    run.iamc.add(variable_data, type=ixmp4.DataPoint.Type.ANNUAL)
    run.set_as_default()

    return iamc_variable, iamc_variable2


def df_from_list(iamc_variables: Iterable[Variable]) -> pd.DataFrame:
    return pd.DataFrame(
        [[v.id, v.name, v.created_at, v.created_by] for v in iamc_variables],
        columns=["id", "name", "created_at", "created_by"],
    )


class TestCoreVariable:
    def test_retrieve_iamc_variable(self, platform: ixmp4.Platform) -> None:
        iamc_variable1 = platform.iamc.variables.create("IAMC Variable")
        platform.regions.create("Region", "default")
        platform.units.create("Unit")

        variable_data = pd.DataFrame(
            [
                ["Region", "IAMC Variable", "Unit", 2005, 1],
            ],
            columns=["region", "variable", "unit", "step_year", "value"],
        )
        run = platform.runs.create("Model", "Scenario")
        run.iamc.add(variable_data, type=ixmp4.DataPoint.Type.ANNUAL)
        run.set_as_default()

        iamc_variable2 = platform.iamc.variables.get("IAMC Variable")

        assert iamc_variable1.id == iamc_variable2.id

    def test_iamc_variable_unqiue(self, platform: ixmp4.Platform) -> None:
        platform.iamc.variables.create("IAMC Variable")

        with pytest.raises(Variable.NotUnique):
            platform.iamc.variables.create("IAMC Variable")

    def test_list_iamc_variable(self, platform: ixmp4.Platform) -> None:
        iamc_variables = create_testcase_iamc_variables(platform)
        iamc_variable, _ = iamc_variables

        a = [v.id for v in iamc_variables]
        b = [v.id for v in platform.iamc.variables.list()]
        assert not (set(a) ^ set(b))

        a = [iamc_variable.id]
        b = [v.id for v in platform.iamc.variables.list(name="IAMC Variable")]
        assert not (set(a) ^ set(b))

    def test_tabulate_iamc_variable(self, platform: ixmp4.Platform) -> None:
        iamc_variables = create_testcase_iamc_variables(platform)
        iamc_variable, _ = iamc_variables

        a = df_from_list(iamc_variables)
        b = platform.iamc.variables.tabulate()
        assert_unordered_equality(a, b, check_dtype=False)

        a = df_from_list([iamc_variable])
        b = platform.iamc.variables.tabulate(name="IAMC Variable")
        assert_unordered_equality(a, b, check_dtype=False)

    def test_retrieve_docs(self, platform: ixmp4.Platform) -> None:
        _, iamc_variable2 = create_testcase_iamc_variables(platform)
        docs_iamc_variable1 = platform.iamc.variables.set_docs(
            "IAMC Variable", "Description of test IAMC Variable"
        )
        docs_iamc_variable2 = platform.iamc.variables.get_docs("IAMC Variable")

        assert docs_iamc_variable1 == docs_iamc_variable2

        assert iamc_variable2.docs is None

        iamc_variable2.docs = "Description of test IAMC Variable 2"

        assert (
            platform.iamc.variables.get_docs("IAMC Variable 2") == iamc_variable2.docs
        )

    def test_delete_docs(self, platform: ixmp4.Platform) -> None:
        iamc_variable, _ = create_testcase_iamc_variables(platform)
        iamc_variable.docs = "Description of test IAMC Variable"
        iamc_variable.docs = None

        assert iamc_variable.docs is None

        iamc_variable.docs = "Second description of test IAMC Variable"
        del iamc_variable.docs

        assert iamc_variable.docs is None

        iamc_variable.docs = "Third description of test IAMC Variable"
        platform.iamc.variables.delete_docs("IAMC Variable")

        assert iamc_variable.docs is None
