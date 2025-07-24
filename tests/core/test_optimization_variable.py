import pandas as pd
import pytest

import ixmp4
from ixmp4.core import IndexSet, OptimizationVariable
from ixmp4.core.exceptions import (
    OptimizationDataValidationError,
    OptimizationItemUsageError,
    RunLockRequired,
)
from ixmp4.data.backend.api import RestBackend
from ixmp4.data.db.optimization.variable.repository import logger

from ..utils import assert_unordered_equality, create_indexsets_for_run


def df_from_list(variables: list[OptimizationVariable]) -> pd.DataFrame:
    return pd.DataFrame(
        [
            [
                variable.run_id,
                variable.data,
                variable.name,
                variable.id,
                variable.created_at,
                variable.created_by,
            ]
            for variable in variables
        ],
        columns=[
            "run__id",
            "data",
            "name",
            "id",
            "created_at",
            "created_by",
        ],
    )


class TestCoreVariable:
    def test_create_variable(self, platform: ixmp4.Platform) -> None:
        run = platform.runs.create("Model", "Scenario")

        # Test creation without indexset
        with run.transact("Test creating scalar Variable"):
            variable = run.optimization.variables.create("Variable")
        assert variable.run_id == run.id
        assert variable.name == "Variable"
        assert variable.data == {}
        assert variable.indexset_names is None
        assert variable.column_names is None
        assert variable.levels == []
        assert variable.marginals == []

        # Test create without run lock raises
        with pytest.raises(RunLockRequired):
            run.optimization.variables.create("Variable 2")

        # Test creation with indexset
        indexset_1, _ = tuple(
            IndexSet(_run=run, _backend=platform.backend, _model=model)
            for model in create_indexsets_for_run(platform=platform, run_id=run.id)
        )
        with run.transact("Test creating indexed Variable"):
            variable_2 = run.optimization.variables.create(
                name="Variable 2",
                constrained_to_indexsets=[indexset_1.name],
            )

        assert variable_2.run_id == run.id
        assert variable_2.name == "Variable 2"
        assert variable_2.data == {}  # JsonDict type currently requires dict, not None
        assert variable_2.column_names is None
        assert variable_2.indexset_names == [indexset_1.name]
        assert variable_2.levels == []
        assert variable_2.marginals == []

        with run.transact("Test raising various errors for optimization Variable"):
            # Test duplicate name raises
            with pytest.raises(OptimizationVariable.NotUnique):
                _ = run.optimization.variables.create(
                    "Variable", constrained_to_indexsets=[indexset_1.name]
                )
            with pytest.raises(OptimizationVariable.NotUnique):
                _ = run.optimization.variables.create(
                    "Variable",
                    constrained_to_indexsets=[indexset_1.name],
                    column_names=["Column 1"],
                )

            # Test that giving column_names, but not constrained_to_indexsets raises
            with pytest.raises(
                OptimizationItemUsageError,
                match="Received `column_names` to name columns, but no "
                "`constrained_to_indexsets`",
            ):
                _ = run.optimization.variables.create(
                    "Variable 0",
                    column_names=["Dimension 1"],
                )

            # Test mismatch in constrained_to_indexsets and column_names raises
            with pytest.raises(OptimizationItemUsageError, match="not equal in length"):
                _ = run.optimization.variables.create(
                    "Variable 0",
                    constrained_to_indexsets=[indexset_1.name],
                    column_names=["Dimension 1", "Dimension 2"],
                )

        with run.transact("Test creating Variable with column_names"):
            # Test columns_names are used for names if given
            variable_3 = run.optimization.variables.create(
                "Variable 3",
                constrained_to_indexsets=[indexset_1.name],
                column_names=["Column 1"],
            )
        assert variable_3.column_names == ["Column 1"]

        with run.transact("Test duplicate column_names for opt.Var"):
            # Test duplicate column_names raise
            with pytest.raises(
                OptimizationItemUsageError, match="`column_names` are not unique"
            ):
                _ = run.optimization.variables.create(
                    name="Variable 0",
                    constrained_to_indexsets=[indexset_1.name, indexset_1.name],
                    column_names=["Column 1", "Column 1"],
                )

        with run.transact("Test opt.Var with different column_names for same indexset"):
            # Test using different column names for same indexset
            variable_4 = run.optimization.variables.create(
                name="Variable 4",
                constrained_to_indexsets=[indexset_1.name, indexset_1.name],
                column_names=["Column 1", "Column 2"],
            )

        assert variable_4.column_names == ["Column 1", "Column 2"]
        assert variable_4.indexset_names == [indexset_1.name, indexset_1.name]

    def test_delete_variable(self, platform: ixmp4.Platform) -> None:
        run = platform.runs.create("Model", "Scenario")

        with run.transact("Test deletion of scalar Variable"):
            variable_1 = run.optimization.variables.create(name="Variable 1")

            # Test deletion without linked IndexSets
            run.optimization.variables.delete(item=variable_1.name)

        assert run.optimization.variables.tabulate().empty

        (indexset_1,) = create_indexsets_for_run(
            platform=platform, run_id=run.id, amount=1
        )
        with run.transact("Test deletion of indexed Variable"):
            variable_2 = run.optimization.variables.create(
                name="Variable 2", constrained_to_indexsets=[indexset_1.name]
            )

            # TODO How to check that DeletionPrevented is raised? No other object uses
            # Variable.id, so nothing could prevent the deletion.

            # Test unknown name raises
            with pytest.raises(OptimizationVariable.NotFound):
                run.optimization.variables.delete(item="does not exist")

            # Test normal deletion
            run.optimization.variables.delete(item=variable_2.name)

        assert run.optimization.variables.tabulate().empty

        # Confirm that IndexSet has not been deleted
        assert not run.optimization.indexsets.tabulate().empty

        # Test that association table rows are deleted
        # If they haven't, this would raise DeletionPrevented
        with run.transact("Test indexsets.delete() in variables.delete()"):
            run.optimization.indexsets.delete(item=indexset_1.id)

        # Test delete without run lock raises
        with pytest.raises(RunLockRequired):
            run.optimization.variables.delete(item="Variable 2")

    def test_get_variable(self, platform: ixmp4.Platform) -> None:
        run = platform.runs.create("Model", "Scenario")
        (indexset,) = create_indexsets_for_run(
            platform=platform, run_id=run.id, amount=1
        )
        with run.transact("Test get Variable"):
            _ = run.optimization.variables.create(
                name="Variable", constrained_to_indexsets=[indexset.name]
            )
        variable = run.optimization.variables.get(name="Variable")
        assert variable.run_id == run.id
        assert variable.id == 1
        assert variable.name == "Variable"
        assert variable.data == {}
        assert variable.levels == []
        assert variable.marginals == []
        assert variable.column_names is None
        assert variable.indexset_names == [indexset.name]

        with pytest.raises(OptimizationVariable.NotFound):
            _ = run.optimization.variables.get("Variable 2")

    def test_variable_add_data(self, platform: ixmp4.Platform) -> None:
        run = platform.runs.create("Model", "Scenario")
        indexset, indexset_2 = tuple(
            IndexSet(_backend=platform.backend, _model=model, _run=run)
            for model in create_indexsets_for_run(platform=platform, run_id=run.id)
        )
        # pandas can only convert dicts to dataframes if the values are lists
        # or if index is given. But maybe using read_json instead of from_dict
        # can remedy this. Or maybe we want to catch the resulting
        # "ValueError: If using all scalar values, you must pass an index" and
        # reraise a custom informative error?
        test_data_1 = {
            indexset.name: ["foo"],
            indexset_2.name: [1],
            "levels": [3.14],
            "marginals": [0.000314],
        }

        with run.transact("Test Variable.add()"):
            indexset.add(data=["foo", "bar", ""])
            indexset_2.add(data=[1, 2, 3])
            variable = run.optimization.variables.create(
                "Variable",
                constrained_to_indexsets=[indexset.name, indexset_2.name],
            )
            variable.add(data=test_data_1)
        assert variable.data == test_data_1
        assert variable.levels == test_data_1["levels"]
        assert variable.marginals == test_data_1["marginals"]

        test_data_2 = {
            indexset.name: ["", "", "foo", "foo", "bar", "bar"],
            indexset_2.name: [3, 1, 2, 1, 2, 3],
            "levels": [6, 5, 4, 3, 2, 1],
            "marginals": [1, 3, 5, 6, 4, 2],
        }

        # Test add without run lock raises
        with pytest.raises(RunLockRequired):
            variable.add(data=test_data_2)

        with run.transact("Test Variable.add() errors and order"):
            variable_2 = run.optimization.variables.create(
                name="Variable 2",
                constrained_to_indexsets=[indexset.name, indexset_2.name],
            )

            with pytest.raises(
                OptimizationItemUsageError,
                match=r"must include the column\(s\): marginals!",
            ):
                variable_2.add(
                    pd.DataFrame(
                        {
                            indexset.name: ["foo"],
                            indexset_2.name: [2],
                            "levels": [1],
                        }
                    ),
                )

            with pytest.raises(
                OptimizationItemUsageError,
                match=r"must include the column\(s\): levels!",
            ):
                variable_2.add(
                    data=pd.DataFrame(
                        {
                            indexset.name: ["foo"],
                            indexset_2.name: [2],
                            "marginals": [0],
                        }
                    ),
                )

            # By converting data to pd.DataFrame, we automatically enforce equal length
            # of new columns, raises All arrays must be of the same length otherwise:
            with pytest.raises(
                OptimizationDataValidationError,
                match="All arrays must be of the same length",
            ):
                variable_2.add(
                    data={
                        indexset.name: ["foo", "foo"],
                        indexset_2.name: [2, 2],
                        "levels": [1, 2],
                        "marginals": [3],
                    },
                )

            with pytest.raises(
                OptimizationDataValidationError, match="contains duplicate rows"
            ):
                variable_2.add(
                    data={
                        indexset.name: ["foo", "foo"],
                        indexset_2.name: [2, 2],
                        "levels": [1, 2],
                        "marginals": [3.4, 5.6],
                    },
                )

            # Test that order is conserved
            variable_2.add(test_data_2)
        assert variable_2.data == test_data_2
        assert variable_2.levels == test_data_2["levels"]
        assert variable_2.marginals == test_data_2["marginals"]

        # NOTE entries for levels and marginals must be convertible to one of
        # (float, int, str)
        test_data_6 = {
            indexset.name: ["foo", "foo", "bar", "bar"],
            indexset_2.name: [1, 3, 1, 2],
            "levels": [0.00001, "2", 2.3, 400000],
            "marginals": [6, 7.8, 9, 0],
        }
        test_data_7 = {
            indexset.name: ["foo", "foo", "bar", "bar", "bar"],
            indexset_2.name: [1, 2, 3, 2, 1],
            "levels": [0.00001, 2.3, 3, "400000", "5"],
            "marginals": [6, 7.8, 9, "0", 3],
        }

        # Test updating of existing keys
        with run.transact("Test Variable upsert"):
            variable_4 = run.optimization.variables.create(
                name="Variable 4",
                constrained_to_indexsets=[indexset.name, indexset_2.name],
            )
            variable_4.add(data=test_data_6)
            variable_4.add(data=test_data_7)
        expected = (
            pd.DataFrame(test_data_7)
            .set_index([indexset.name, indexset_2.name])
            .combine_first(
                pd.DataFrame(test_data_6).set_index([indexset.name, indexset_2.name])
            )
            .reset_index()
        )
        # NOTE Something along the API route converts all levels and marginals to float,
        # while the direct pandas call respects the different dtypes. However, anyone
        # accessing .levels and .marginals will always be served float, so that's fine.
        if isinstance(platform.backend, RestBackend):
            expected = expected.astype({"levels": float, "marginals": float})
        assert_unordered_equality(
            expected, pd.DataFrame(variable_4.data), check_dtype=False
        )

        # Test adding to scalar variable raises
        with run.transact("Test raising on adding to scalar Variable"):
            with pytest.raises(
                OptimizationDataValidationError,
                match="Trying to add data to unknown columns!",
            ):
                variable_5 = run.optimization.variables.create("Variable 5")
                variable_5.add(data={"foo": ["bar"], "levels": [1], "marginals": [0]})

        test_data_8 = {
            "Column 1": ["", "", "foo", "foo", "bar", "bar"],
            "Column 2": [3, 1, 2, 1, 2, 3],
            "levels": [6, 5, 4, 3, 2, 1],
            "marginals": [0.5] * 6,
        }

        # Test adding with column_names
        with run.transact("Test Variable.add() with column_names"):
            variable_6 = run.optimization.variables.create(
                name="Variable 6",
                constrained_to_indexsets=[indexset.name, indexset_2.name],
                column_names=["Column 1", "Column 2"],
            )
            variable_6.add(data=test_data_8)

        assert variable_6.data == test_data_8

        # Test adding empty data works
        with run.transact("Test Variable.add() with empty data"):
            variable_6.add(pd.DataFrame())

        assert variable_6.data == test_data_8

    def test_variable_remove_data(
        self, platform: ixmp4.Platform, caplog: pytest.LogCaptureFixture
    ) -> None:
        run = platform.runs.create("Model", "Scenario")
        test_data: dict[str, list[float | int | str]] = {
            "Indexset": ["bar", "foo"],
            "levels": [2.3, 1],
            "marginals": [0, 4.2],
        }

        with run.transact("Test Variable.remove_data() -- preparation"):
            indexset = run.optimization.indexsets.create("Indexset")
            indexset.add(data=["foo", "bar"])
            variable = run.optimization.variables.create(
                "Variable",
                constrained_to_indexsets=[indexset.name],
            )
            variable.add(test_data)
        assert variable.data == test_data

        # Test removing empty data removes nothing
        with run.transact("Test Variable.remove_data()"):
            variable.remove_data(data={})

        assert variable.data == test_data

        # Test remove without run lock raises
        with pytest.raises(RunLockRequired):
            variable.remove_data(data={})

        with run.transact("Test Variable.remove_data() errors"):
            # Test incomplete index raises...
            with pytest.raises(
                OptimizationItemUsageError, match="data to be removed must specify"
            ):
                variable.remove_data(data={"foo": ["bar"]})

            # ...even when removing a column that's known in principle
            with pytest.raises(
                OptimizationItemUsageError, match="data to be removed must specify"
            ):
                variable.remove_data(data={"levels": [2.3]})

            # Test removing one row
            remove_data = {indexset.name: [test_data[indexset.name][0]]}
            test_data_2 = {k: [v[1]] for k, v in test_data.items()}
            variable.remove_data(data=remove_data)
        assert variable.data == test_data_2

        # Test removing non-existing (but correctly formatted) data works, even with
        # additional/unused columns
        remove_data["levels"] = [1]
        with run.transact("Test Variable.remove_data() for non-existing data"):
            variable.remove_data(data=remove_data)

        assert variable.data == test_data_2

        # Test removing all rows
        with run.transact("Test Variable.remove_data() for all data"):
            variable.remove_data()
        assert variable.data == {}

        # Test removing specific data from unindexed Equation warns
        with run.transact("Test Variable.remove_data() warns on scalar Variable"):
            variable_2 = run.optimization.variables.create("Variable 2")

            caplog.clear()
            with caplog.at_level("WARNING", logger=logger.name):
                variable_2.remove_data(data=test_data_2)

        expected = [
            f"Trying to remove {test_data_2} from Variable '{variable_2.name}', but "
            "that is not indexed; not removing anything!"
        ]
        assert caplog.messages == expected

    def test_list_variable(self, platform: ixmp4.Platform) -> None:
        run = platform.runs.create("Model", "Scenario")
        indexset, indexset_2 = create_indexsets_for_run(
            platform=platform, run_id=run.id
        )
        with run.transact("Test variables.list()"):
            variable = run.optimization.variables.create(
                "Variable", constrained_to_indexsets=[indexset.name]
            )
            variable_2 = run.optimization.variables.create(
                "Variable 2", constrained_to_indexsets=[indexset_2.name]
            )

        # Create new run to test listing variables for specific run
        run_2 = platform.runs.create("Model", "Scenario")
        (indexset,) = create_indexsets_for_run(
            platform=platform, run_id=run_2.id, amount=1
        )
        with run_2.transact("Test variables.list() for specific run"):
            run_2.optimization.variables.create(
                "Variable", constrained_to_indexsets=[indexset.name]
            )
        expected_ids = [variable.id, variable_2.id]
        list_ids = [variable.id for variable in run.optimization.variables.list()]
        assert not (set(expected_ids) ^ set(list_ids))

        # Test retrieving just one result by providing a name
        expected_id = [variable.id]
        list_id = [
            variable.id for variable in run.optimization.variables.list(name="Variable")
        ]
        assert not (set(expected_id) ^ set(list_id))

    def test_tabulate_variable(self, platform: ixmp4.Platform) -> None:
        run = platform.runs.create("Model", "Scenario")
        indexset, indexset_2 = tuple(
            IndexSet(_backend=platform.backend, _model=model, _run=run)
            for model in create_indexsets_for_run(platform=platform, run_id=run.id)
        )
        with run.transact("Test variables.tabulate()"):
            variable = run.optimization.variables.create(
                name="Variable",
                constrained_to_indexsets=[indexset.name, indexset_2.name],
            )
            variable_2 = run.optimization.variables.create(
                name="Variable 2",
                constrained_to_indexsets=[indexset.name, indexset_2.name],
            )

        # Create new run to test tabulating variables for specific run
        run_2 = platform.runs.create("Model", "Scenario")
        (indexset_3,) = create_indexsets_for_run(
            platform=platform, run_id=run_2.id, amount=1
        )
        with run_2.transact("Test variables.tabulate() for specific run"):
            run_2.optimization.variables.create(
                "Variable", constrained_to_indexsets=[indexset_3.name]
            )
        pd.testing.assert_frame_equal(
            df_from_list([variable_2]),
            run.optimization.variables.tabulate(name="Variable 2"),
        )

        test_data_1 = {
            indexset.name: ["foo"],
            indexset_2.name: [1],
            "levels": [314],
            "marginals": [2.0],
        }
        test_data_2 = {
            indexset_2.name: [2, 3],
            indexset.name: ["foo", "bar"],
            "levels": [1, -2.0],
            "marginals": [0, 10],
        }
        with run.transact("Test variables.tabulate() with data"):
            indexset.add(data=["foo", "bar"])
            indexset_2.add(data=[1, 2, 3])
            variable.add(data=test_data_1)
            variable_2.add(data=test_data_2)

        pd.testing.assert_frame_equal(
            df_from_list([variable, variable_2]),
            run.optimization.variables.tabulate(),
        )

    def test_variable_docs(self, platform: ixmp4.Platform) -> None:
        run = platform.runs.create("Model", "Scenario")
        (indexset,) = create_indexsets_for_run(
            platform=platform, run_id=run.id, amount=1
        )
        with run.transact("Test Variable.docs"):
            variable_1 = run.optimization.variables.create(
                "Variable 1", constrained_to_indexsets=[indexset.name]
            )
        docs = "Documentation of Variable 1"
        variable_1.docs = docs
        assert variable_1.docs == docs

        variable_1.docs = None
        assert variable_1.docs is None
