import pandas as pd
import pandas.testing as pdt
import pytest

# Import this from typing when dropping 3.11
from typing_extensions import Unpack

import ixmp4
from ixmp4.core import Run
from ixmp4.core.exceptions import IxmpError, RunIsLocked

from ..fixtures import FilterIamcDataset, SmallIamcDataset


def _expected_runs_table(*row_default: Unpack[tuple[bool | None, ...]]) -> pd.DataFrame:
    rows = []
    for i, default in enumerate(row_default, start=1):
        if default is not None:
            rows.append(["Model", "Scenario", i] + [default])

    return pd.DataFrame(rows, columns=["model", "scenario", "version", "is_default"])


def assert_cloned_run(original: Run, clone: Run, kept_solution: bool) -> None:
    """Asserts that a Run and its clone contain the same data."""
    # Assert IAMC data are equal
    pdt.assert_frame_equal(original.iamc.tabulate(), clone.iamc.tabulate())

    # Assert indexset names and data are equal
    for original_indexset, cloned_indexset in zip(
        original.optimization.indexsets.list(), clone.optimization.indexsets.list()
    ):
        assert original_indexset.name == cloned_indexset.name
        assert original_indexset.data == cloned_indexset.data

    # Assert scalar names and data are equal
    for original_scalar, cloned_scalar in zip(
        original.optimization.scalars.list(), clone.optimization.scalars.list()
    ):
        assert original_scalar.name == cloned_scalar.name
        assert original_scalar.value == cloned_scalar.value
        assert original_scalar.unit.name == cloned_scalar.unit.name

    # Assert table names and data are equal
    for original_table, cloned_table in zip(
        original.optimization.tables.list(), clone.optimization.tables.list()
    ):
        assert original_table.name == cloned_table.name
        assert original_table.data == cloned_table.data

    # Assert parameter names and data are equal
    for original_parameter, cloned_parameter in zip(
        original.optimization.parameters.list(), clone.optimization.parameters.list()
    ):
        assert original_parameter.name == cloned_parameter.name
        assert original_parameter.data == cloned_parameter.data

    # Assert equation names are equal and the solution is either equal or empty
    for original_equation, cloned_equation in zip(
        original.optimization.equations.list(), clone.optimization.equations.list()
    ):
        assert original_equation.name == cloned_equation.name
        assert cloned_equation.data == (original_equation.data if kept_solution else {})

    # Assert variable names are equal and the solution is either equal or empty
    for original_variable, cloned_variable in zip(
        original.optimization.variables.list(), clone.optimization.variables.list()
    ):
        assert original_variable.name == cloned_variable.name
        assert cloned_variable.data == (original_variable.data if kept_solution else {})


class TestCoreRun:
    filter = FilterIamcDataset()
    small = SmallIamcDataset()

    def test_run_notfound(self, platform: ixmp4.Platform) -> None:
        # no Run with that model and scenario name exists
        with pytest.raises(Run.NotFound):
            _ = platform.runs.get("Unknown Model", "Unknown Scenario", version=1)

    def test_run_versions(self, platform: ixmp4.Platform) -> None:
        run1 = platform.runs.create("Model", "Scenario")
        run2 = platform.runs.create("Model", "Scenario")

        assert run1.id != run2.id

        # no default version is assigned, so list & tabulate are empty
        with pytest.raises(Run.NoDefaultVersion):
            _ = platform.runs.get("Model", "Scenario")
        assert platform.runs.list() == []
        assert platform.runs.tabulate().empty

        # getting a specific version works even if no default version is assigned
        assert run1.id == platform.runs.get("Model", "Scenario", version=1).id

        # getting the table and list for all runs works
        run_list = platform.runs.list(default_only=False)
        assert len(run_list) == 2
        assert run_list[0].id == run1.id
        pdt.assert_frame_equal(
            platform.runs.tabulate(default_only=False),
            pd.DataFrame(_expected_runs_table(False, False)),
        )

        # set default, so list & tabulate show default version only
        run1.set_as_default()
        run_list = platform.runs.list()
        assert len(run_list) == 1
        assert run_list[0].id == run1.id
        pdt.assert_frame_equal(
            platform.runs.tabulate(),
            pd.DataFrame(_expected_runs_table(True)),
        )

        # using default_only=False shows both versions
        pdt.assert_frame_equal(
            platform.runs.tabulate(default_only=False),
            pd.DataFrame(_expected_runs_table(True, False)),
        )

        # using audit_info=True shows additional columns
        audit_info = platform.runs.tabulate(default_only=False, audit_info=True)
        for column in ["updated_at", "updated_by", "created_at", "created_by", "id"]:
            assert column in audit_info.columns
        pdt.assert_series_equal(audit_info.id, pd.Series([run1.id, run2.id], name="id"))

        # default version can be retrieved directly
        run = platform.runs.get("Model", "Scenario")
        assert run1.id == run.id

        # default version can be changed
        run2.set_as_default()
        run = platform.runs.get("Model", "Scenario")
        assert run2.id == run.id

        # list shows changed default version only
        run_list = platform.runs.list()
        assert len(run_list) == 1
        assert run_list[0].id == run2.id
        pdt.assert_frame_equal(
            platform.runs.tabulate(),
            pd.DataFrame(_expected_runs_table(None, True)),
        )

        # unsetting default means run cannot be retrieved directly
        run2.unset_as_default()
        with pytest.raises(Run.NoDefaultVersion):
            platform.runs.get("Model", "Scenario")

        # non-default version cannot be again set as un-default
        with pytest.raises(IxmpError):
            run2.unset_as_default()

        self.filter.load_dataset(platform)

        res = platform.runs.tabulate(
            iamc={
                "region": {"name": "Region 1"},
            },
        )
        assert sorted(res["model"].tolist()) == ["Model 1"]
        assert sorted(res["scenario"].tolist()) == ["Scenario 1"]

        res = platform.runs.tabulate(
            default_only=False,
            iamc={
                "region": {"name": "Region 2"},
            },
        )
        assert sorted(res["model"].tolist()) == ["Model 2"]
        assert sorted(res["scenario"].tolist()) == ["Scenario 2"]

        res = platform.runs.tabulate(
            default_only=True,
            iamc={
                "variable": {"name__like": "Variable *"},
                "unit": {"name__in": ["Unit 2", "Unit 4"]},
            },
        )
        assert sorted(res["model"].tolist()) == ["Model 1"]
        assert sorted(res["scenario"].tolist()) == ["Scenario 1"]

        res = platform.runs.tabulate(
            default_only=False,
            scenario={"name__in": ["Scenario 1", "Scenario 2"]},
            iamc=None,
        )

        assert sorted(res["model"].tolist()) == ["Model 1", "Model 2"]
        assert sorted(res["scenario"].tolist()) == ["Scenario 1", "Scenario 2"]

        res = platform.runs.tabulate(
            default_only=False,
            iamc=False,
        )
        assert sorted(res["model"].tolist()) == ["Model", "Model"]
        assert sorted(res["scenario"].tolist()) == ["Scenario", "Scenario"]

        for run in platform.runs.list(
            default_only=False,
            model={"name": "Model 1"},
            scenario={"name": "Scenario 1"},
        ):
            self.delete_all_datapoints(run)
        res = platform.runs.tabulate(
            default_only=False,
            iamc={
                "region": {"name": "Region 3"},
            },
        )
        assert sorted(res["model"].tolist()) == []
        assert sorted(res["scenario"].tolist()) == []

    def delete_all_datapoints(self, run: ixmp4.Run) -> None:
        remove_data = run.iamc.tabulate(raw=True)
        annual = remove_data[remove_data["type"] == "ANNUAL"].dropna(
            how="all", axis="columns"
        )
        cat = remove_data[remove_data["type"] == "CATEGORICAL"].dropna(
            how="all", axis="columns"
        )

        datetime = remove_data[remove_data["type"] == "DATETIME"].dropna(
            how="all", axis="columns"
        )
        with run.transact("Remove iamc data"):
            if not annual.empty:
                run.iamc.remove(annual, type=ixmp4.DataPoint.Type.ANNUAL)
            if not cat.empty:
                run.iamc.remove(cat, type=ixmp4.DataPoint.Type.CATEGORICAL)
            if not datetime.empty:
                run.iamc.remove(datetime, type=ixmp4.DataPoint.Type.DATETIME)

    def test_run_remove_solution(self, platform: ixmp4.Platform) -> None:
        run = platform.runs.create("Model", "Scenario")
        indexset = run.optimization.indexsets.create("Indexset")
        indexset.add(["foo", "bar"])
        test_data = {
            "Indexset": ["bar", "foo"],
            "levels": [2.5, 1],
            "marginals": [0, 6.9],
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

    def test_run_delete_locked_run(self, platform: ixmp4.Platform) -> None:
        self.small.load_dataset(platform)
        run1_1 = platform.runs.get("Model 1", "Scenario 1")
        run1_2 = platform.runs.get("Model 1", "Scenario 1")

        with run1_1.transact("Erroneously lock run for deletion"):
            with pytest.raises(RunIsLocked):
                run1_1.delete()
            with pytest.raises(RunIsLocked):
                run1_2.delete()

    def test_run_delete_via_object_method(self, platform: ixmp4.Platform) -> None:
        self.small.load_dataset(platform)
        run1 = platform.runs.get("Model 1", "Scenario 1")
        run2 = platform.runs.get("Model 2", "Scenario 2")

        for run in [run1, run2]:
            run.delete()
            self.assert_run_data_deleted(platform, run)

    def test_run_delete_via_repository_id(self, platform: ixmp4.Platform) -> None:
        self.small.load_dataset(platform)
        run1 = platform.runs.get("Model 1", "Scenario 1")
        run2 = platform.runs.get("Model 2", "Scenario 2")

        for run in [run1, run2]:
            platform.runs.delete(run.id)
            self.assert_run_data_deleted(platform, run)

    def test_run_delete_via_repository_object(self, platform: ixmp4.Platform) -> None:
        self.small.load_dataset(platform)
        run1 = platform.runs.get("Model 1", "Scenario 1")
        run2 = platform.runs.get("Model 2", "Scenario 2")

        for run in [run1, run2]:
            platform.runs.delete(run)
            self.assert_run_data_deleted(platform, run)

    def test_run_invalid_argument(self, platform: ixmp4.Platform) -> None:
        with pytest.raises(TypeError):
            platform.runs.delete("Model|Scenario")  # type: ignore[arg-type]

    def assert_run_data_deleted(self, platform: ixmp4.Platform, run: Run) -> None:
        ret_meta = platform.backend.meta.tabulate(
            run={"id": run.id, "default_only": False}
        )
        assert ret_meta.empty

        ret_iamc_dps = platform.backend.iamc.datapoints.tabulate(
            run={"id": run.id, "default_only": True}
        )
        assert ret_iamc_dps.empty

        # TODO: check if optimization data is deleted. @glatterf42

    def test_run_clone(self, platform: ixmp4.Platform) -> None:
        # Prepare test data and platform
        test_data_annual = self.small.annual.copy()
        # Define required regions and units in the database
        self.small.load_regions(platform)
        self.small.load_units(platform)
        unit = platform.units.list()[0]  # Test data currently only has one
        test_data = {"Indexset": ["foo"], "values": [3.14], "units": [unit.name]}
        test_solution = {"Indexset": ["foo"], "levels": [4], "marginals": [0.2]}

        # Prepare original run
        run = platform.runs.create("Model", "Scenario")
        # Add IAMC data
        with run.transact("Add IAMC data"):
            run.iamc.add(test_data_annual, type=ixmp4.DataPoint.Type.ANNUAL)

        # Create optimization items and add some data
        indexset = run.optimization.indexsets.create("Indexset")
        indexset.add(["foo", "bar"])

        run.optimization.scalars.create("Scalar", value=10, unit=unit.name)

        run.optimization.tables.create(
            "Table", constrained_to_indexsets=[indexset.name]
        ).add({"Indexset": ["bar"]})

        run.optimization.parameters.create(
            "Parameter", constrained_to_indexsets=[indexset.name]
        ).add(test_data)

        run.optimization.variables.create(
            "Variable", constrained_to_indexsets=[indexset.name]
        ).add(test_solution)

        run.optimization.equations.create(
            "Equation", constrained_to_indexsets=[indexset.name]
        ).add(test_solution)

        # Test cloning while keeping the solution
        clone_with_solution = run.clone()
        assert_cloned_run(run, clone_with_solution, kept_solution=True)

        # Test cloning without keeping the solution
        clone_without_solution = run.clone(
            model="new model", scenario="new scenario", keep_solution=False
        )
        assert_cloned_run(run, clone_without_solution, kept_solution=False)

        # Test working with cloned run
        cloned_indexset = clone_with_solution.optimization.indexsets.get(indexset.name)
        cloned_indexset.add("baz")
        expected = indexset.data
        # TODO If possible, it would be great to type hint data according to what it is
        # so that something like this works (not just a generic union of lists):
        expected.append("baz")  # type: ignore[arg-type]
        assert cloned_indexset.data == expected

        # Test cloning Run without iamc data
        run = platform.runs.create("Model", "Scenario")
        clone_without_iamc = run.clone()
        assert clone_without_iamc.iamc.tabulate().empty

    def test_run_is_default(self, platform: ixmp4.Platform) -> None:
        run = platform.runs.create("Model", "Scenario")
        assert run.is_default is False

        run.set_as_default()
        assert run.is_default

        # Mypy doesn't know that set_as_default() reloads the underlying run._model
        run.unset_as_default()  # type: ignore[unreachable]
        assert not run.is_default

    def test_run_has_solution(self, platform: ixmp4.Platform) -> None:
        run = platform.runs.create("Model", "Scenario")

        # Set up an equation and a variable
        indexset = run.optimization.indexsets.create("Indexset")
        indexset.add(["foo"])
        equation = run.optimization.equations.create(
            name="Equation",
            constrained_to_indexsets=[indexset.name],
        )
        variable = run.optimization.variables.create("Variable")

        # Without data in them, the run has no solution
        assert not run.optimization.has_solution()

        # Add data to equation to simulate having a solution
        equation.add({indexset.name: ["foo"], "levels": [1], "marginals": [0]})
        assert run.optimization.has_solution()

        # Check this also works for having data just in the variable
        equation.remove_data()
        variable.add({"levels": [2025], "marginals": [1.5]})
        assert run.optimization.has_solution()
