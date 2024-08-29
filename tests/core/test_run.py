import pandas as pd
import pandas.testing as pdt
import pytest

import ixmp4
from ixmp4 import IxmpError, Run

from ..utils import add_regions, add_units, all_platforms, create_filter_test_data


def _expected_runs_table(*row_default):
    rows = []
    for i, default in enumerate(row_default, start=1):
        if default is not None:
            rows.append([i, "Model", "Scenario", i] + [default])

    return pd.DataFrame(
        rows, columns=["id", "model", "scenario", "version", "is_default"]
    )


def assert_cloned_run(original: Run, clone: Run, kept_solution: bool) -> None:
    pdt.assert_frame_equal(original.iamc.tabulate(), clone.iamc.tabulate())
    for original_indexset, cloned_indexset in zip(
        original.optimization.indexsets.list(), clone.optimization.indexsets.list()
    ):
        assert original_indexset.name == cloned_indexset.name
        assert original_indexset.elements == cloned_indexset.elements
    for original_scalar, cloned_scalar in zip(
        original.optimization.scalars.list(), clone.optimization.scalars.list()
    ):
        assert original_scalar.name == cloned_scalar.name
        assert original_scalar.value == cloned_scalar.value
        assert original_scalar.unit.name == cloned_scalar.unit.name
    for original_table, cloned_table in zip(
        original.optimization.tables.list(), clone.optimization.tables.list()
    ):
        assert original_table.name == cloned_table.name
        assert original_table.data == cloned_table.data
    for original_parameter, cloned_parameter in zip(
        original.optimization.parameters.list(), clone.optimization.parameters.list()
    ):
        assert original_parameter.name == cloned_parameter.name
        assert original_parameter.data == cloned_parameter.data
    for original_equation, cloned_equation in zip(
        original.optimization.equations.list(), clone.optimization.equations.list()
    ):
        assert original_equation.name == cloned_equation.name
        assert cloned_equation.data == (original_equation.data if kept_solution else {})
    for original_variable, cloned_variable in zip(
        original.optimization.variables.list(), clone.optimization.variables.list()
    ):
        assert original_variable.name == cloned_variable.name
        assert cloned_variable.data == (original_variable.data if kept_solution else {})


@all_platforms
class TestCoreRun:
    def test_run_notfound(self, test_mp, request):
        test_mp = request.getfixturevalue(test_mp)
        # no Run with that model and scenario name exists
        with pytest.raises(Run.NotFound):
            _ = test_mp.runs.get("Unknown Model", "Unknown Scenario", version=1)

    def test_run_versions(self, test_mp, request):
        test_mp = request.getfixturevalue(test_mp)
        run1 = test_mp.runs.create("Model", "Scenario")
        run2 = test_mp.runs.create("Model", "Scenario")

        assert run1.id != run2.id

        # no default version is assigned, so list & tabulate are empty
        with pytest.raises(Run.NoDefaultVersion):
            _ = test_mp.runs.get("Model", "Scenario")
        assert test_mp.runs.list() == []
        assert test_mp.runs.tabulate().empty

        # getting a specific version works even if no default version is assigned
        assert run1.id == test_mp.runs.get("Model", "Scenario", version=1).id

        # getting the table and list for all runs works
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
        run = test_mp.runs.get("Model", "Scenario")
        assert run1.id == run.id

        # default version can be changed
        run2.set_as_default()
        run = test_mp.runs.get("Model", "Scenario")
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
            test_mp.runs.get("Model", "Scenario")

        # non-default version cannot be again set as un-default
        with pytest.raises(IxmpError):
            run2.unset_as_default()

    def test_filter_run(self, test_mp, request):
        test_mp = request.getfixturevalue(test_mp)
        run1, run2 = create_filter_test_data(test_mp)

        res = test_mp.runs.tabulate(
            default_only=False,
            iamc={
                "region": {"name": "Region 1"},
            },
        )
        assert sorted(res["model"].tolist()) == ["Model 1"]
        assert sorted(res["scenario"].tolist()) == ["Scenario 1"]

        res = test_mp.runs.tabulate(
            default_only=False,
            iamc={
                "region": {"name": "Region 3"},
            },
        )
        assert sorted(res["model"].tolist()) == ["Model 1", "Model 1", "Model 2"]
        assert sorted(res["scenario"].tolist()) == [
            "Scenario 1",
            "Scenario 1",
            "Scenario 2",
        ]

        run1.set_as_default()
        res = test_mp.runs.tabulate(
            iamc={
                "variable": {"name": "Variable 1"},
                "unit": {"name__in": ["Unit 3", "Unit 4"]},
            }
        )
        assert sorted(res["model"].tolist()) == ["Model 2"]
        assert sorted(res["scenario"].tolist()) == ["Scenario 2"]

        res = test_mp.runs.tabulate(
            default_only=False,
            scenario={"name__in": ["Scenario 2", "Scenario 3"]},
            iamc=None,
        )
        assert sorted(res["model"].tolist()) == ["Model 2", "Model 3"]
        assert sorted(res["scenario"].tolist()) == ["Scenario 2", "Scenario 3"]

        res = test_mp.runs.tabulate(
            default_only=False,
            iamc=False,
        )
        assert sorted(res["model"].tolist()) == ["Model 3"]
        assert sorted(res["scenario"].tolist()) == ["Scenario 3"]

        remove_data = test_mp.iamc.tabulate(
            model={"name": "Model 1"},
            scenario={"name": "Scenario 1"},
            run={"default_only": False},
            region={"name": "Region 3"},
        )
        run1.iamc.remove(remove_data)
        run2.iamc.remove(remove_data)
        res = test_mp.runs.tabulate(
            default_only=False,
            iamc={
                "region": {"name": "Region 3"},
            },
        )
        assert sorted(res["model"].tolist()) == ["Model 2"]
        assert sorted(res["scenario"].tolist()) == ["Scenario 2"]

    def test_run_remove_solution(self, test_mp, request):
        test_mp = request.getfixturevalue(test_mp)
        run = test_mp.runs.create("Model", "Scenario")
        indexset = run.optimization.indexsets.create("Indexset")
        indexset.add(["foo", "bar"])
        test_data = {
            "Indexset": ["bar", "foo"],
            "levels": [2.0, 1],
            "marginals": [0, "test"],
        }
        run.optimization.equations.create(
            "Equation",
            constrained_to_indexsets=[indexset.name],
        ).add(test_data)
        run.optimization.variables.create(
            "Variable",
            constrained_to_indexsets=[indexset.name],
        ).add(test_data)

        run.optimization.remove_solution()
        # Need to fetch them here even if fetched before because API layer might not
        # forward changes automatically
        equation = run.optimization.equations.get("Equation")
        variable = run.optimization.variables.get("Variable")
        assert equation.data == {}
        assert variable.data == {}

    def test_run_clone(self, test_mp, request, test_data_annual):
        # Prepare test data and platform
        test_mp: ixmp4.Platform = request.getfixturevalue(test_mp)  # type: ignore
        # Define required regions and units in the database
        add_regions(test_mp, test_data_annual["region"].unique())
        add_units(test_mp, test_data_annual["unit"].unique())
        unit = test_mp.units.list()[0]  # Test data currently only has one
        test_data = {"Indexset": ["foo"], "values": [3.14], "units": [unit.name]}
        test_solution = {"Indexset": ["foo"], "levels": [4], "marginals": [0.2]}

        # Prepare original run
        run = test_mp.runs.create("Model", "Scenario")
        run.iamc.add(test_data_annual, type=ixmp4.DataPoint.Type.ANNUAL)
        indexset = run.optimization.indexsets.create("Indexset")
        indexset.add(["foo", "bar"])
        run.optimization.scalars.create("Scalar", value=10, unit=unit.name)
        run.optimization.tables.create(
            "Table",
            constrained_to_indexsets=[indexset.name],
        ).add({"Indexset": ["bar"]})
        run.optimization.parameters.create(
            "Parameter", constrained_to_indexsets=[indexset.name]
        ).add(test_data)
        run.optimization.variables.create(
            "Variable", constrained_to_indexsets=[indexset.name]
        ).add(test_solution)
        run.optimization.equations.create(
            "Equation",
            constrained_to_indexsets=[indexset.name],
        ).add(test_solution)

        # Test cloning while keeping the solution
        clone_with_solution = test_mp.runs.clone(run_id=run.id)
        assert_cloned_run(run, clone_with_solution, kept_solution=True)

        # Test cloning without keeping the solution
        clone_without_solution = test_mp.runs.clone(
            run_id=run.id,
            model="new model",
            scenario="new scenario",
            keep_solution=False,
        )
        assert_cloned_run(run, clone_without_solution, kept_solution=False)

        # Test working with cloned run
        cloned_indexset = clone_with_solution.optimization.indexsets.get(indexset.name)
        cloned_indexset.add("baz")
        expected = indexset.elements
        expected.append("baz")
        assert cloned_indexset.elements == expected

        # Test cloning Run without iamc data
        run = test_mp.runs.create("Model", "Scenario")
        clone_without_iamc = test_mp.runs.clone(run.id)
        assert clone_without_iamc.iamc.tabulate().empty
