import pandas as pd
import pytest

import ixmp4
from ixmp4.core import Equation, IndexSet
from ixmp4.core.exceptions import (
    OptimizationDataValidationError,
    OptimizationItemUsageError,
)
from ixmp4.data.backend.api import RestBackend
from ixmp4.data.db.optimization.equation.repository import logger

from ..utils import assert_unordered_equality, create_indexsets_for_run


def df_from_list(equations: list[Equation]) -> pd.DataFrame:
    return pd.DataFrame(
        [
            [
                equation.run_id,
                equation.data,
                equation.name,
                equation.id,
                equation.created_at,
                equation.created_by,
            ]
            for equation in equations
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


class TestCoreEquation:
    def test_create_equation(self, platform: ixmp4.Platform) -> None:
        run = platform.runs.create("Model", "Scenario")

        # Test creation without indexset
        with run.transact("Test equations.create() scalar"):
            equation_1 = run.optimization.equations.create("Equation 1")
        assert equation_1.run_id == run.id
        assert equation_1.name == "Equation 1"
        assert equation_1.data == {}
        assert equation_1.indexset_names is None
        assert equation_1.column_names is None
        assert equation_1.levels == []
        assert equation_1.marginals == []

        # Test creation with indexset
        indexset_1, _ = tuple(
            IndexSet(_backend=platform.backend, _model=model, _run=run)
            for model in create_indexsets_for_run(platform=platform, run_id=run.id)
        )
        with run.transact("Test equations.create() linked"):
            equation_2 = run.optimization.equations.create(
                name="Equation 2",
                constrained_to_indexsets=[indexset_1.name],
            )

        assert equation_2.run_id == run.id
        assert equation_2.name == "Equation 2"
        assert equation_2.data == {}  # JsonDict type requires a dict, not None
        assert equation_2.column_names is None
        assert equation_2.indexset_names == [indexset_1.name]
        assert equation_2.levels == []
        assert equation_2.marginals == []

        with run.transact("Test equations.create() errors and column_names"):
            # Test duplicate name raises
            with pytest.raises(Equation.NotUnique):
                _ = run.optimization.equations.create(
                    "Equation 1", constrained_to_indexsets=[indexset_1.name]
                )
            with pytest.raises(Equation.NotUnique):
                _ = run.optimization.equations.create(
                    "Equation 1",
                    constrained_to_indexsets=[indexset_1.name],
                    column_names=["Column 1"],
                )

            # Test that giving column_names, but not constrained_to_indexsets raises
            with pytest.raises(
                OptimizationItemUsageError,
                match="Received `column_names` to name columns, but no "
                "`constrained_to_indexsets`",
            ):
                _ = run.optimization.equations.create(
                    "Equation 0",
                    column_names=["Dimension 1"],
                )

            # Test mismatch in constrained_to_indexsets and column_names raises
            with pytest.raises(OptimizationItemUsageError, match="not equal in length"):
                _ = run.optimization.equations.create(
                    "Equation 2",
                    constrained_to_indexsets=[indexset_1.name],
                    column_names=["Dimension 1", "Dimension 2"],
                )

            # Test columns_names are used for names if given
            equation_3 = run.optimization.equations.create(
                "Equation 3",
                constrained_to_indexsets=[indexset_1.name],
                column_names=["Column 1"],
            )
        assert equation_3.column_names == ["Column 1"]

        with run.transact("Test equations.create() multiple column_names"):
            # Test duplicate column_names raise
            with pytest.raises(
                OptimizationItemUsageError, match="`column_names` are not unique"
            ):
                _ = run.optimization.equations.create(
                    name="Equation 4",
                    constrained_to_indexsets=[indexset_1.name, indexset_1.name],
                    column_names=["Column 1", "Column 1"],
                )

            # Test using different column names for same indexset
            equation_4 = run.optimization.equations.create(
                name="Equation 4",
                constrained_to_indexsets=[indexset_1.name, indexset_1.name],
                column_names=["Column 1", "Column 2"],
            )

        assert equation_4.column_names == ["Column 1", "Column 2"]
        assert equation_4.indexset_names == [indexset_1.name, indexset_1.name]

    def test_delete_equation(self, platform: ixmp4.Platform) -> None:
        run = platform.runs.create("Model", "Scenario")

        with run.transact("Test equations.delete() scalar"):
            equation_1 = run.optimization.equations.create(name="Equation 1")

            # Test deletion without linked IndexSets
            run.optimization.equations.delete(item=equation_1.name)

        assert run.optimization.equations.tabulate().empty

        (indexset_1,) = create_indexsets_for_run(
            platform=platform, run_id=run.id, amount=1
        )
        with run.transact("Test equations.delete() linked"):
            equation_2 = run.optimization.equations.create(
                name="Equation 2", constrained_to_indexsets=[indexset_1.name]
            )

            # TODO How to check that DeletionPrevented is raised? No other object uses
            # Equation.id, so nothing could prevent the deletion.

            # Test unknown name raises
            with pytest.raises(Equation.NotFound):
                run.optimization.equations.delete(item="does not exist")

            # Test normal deletion
            run.optimization.equations.delete(item=equation_2.name)

        assert run.optimization.equations.tabulate().empty

        # Confirm that IndexSet has not been deleted
        assert not run.optimization.indexsets.tabulate().empty

        with run.transact("Test equations.delete() indexset linkage"):
            # Test that association table rows are deleted
            # If they haven't, this would raise DeletionPrevented
            run.optimization.indexsets.delete(item=indexset_1.id)

    def test_get_equation(self, platform: ixmp4.Platform) -> None:
        run = platform.runs.create("Model", "Scenario")
        (indexset,) = create_indexsets_for_run(
            platform=platform, run_id=run.id, amount=1
        )
        with run.transact("Test equations.get()"):
            _ = run.optimization.equations.create(
                name="Equation", constrained_to_indexsets=[indexset.name]
            )
        equation = run.optimization.equations.get(name="Equation")
        assert equation.run_id == run.id
        assert equation.id == 1
        assert equation.name == "Equation"
        assert equation.data == {}
        assert equation.levels == []
        assert equation.marginals == []
        assert equation.column_names is None
        assert equation.indexset_names == [indexset.name]

        with pytest.raises(Equation.NotFound):
            _ = run.optimization.equations.get("Equation 2")

    def test_equation_add_data(self, platform: ixmp4.Platform) -> None:
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
        with run.transact("Test Equation.add()"):
            indexset.add(data=["foo", "bar", ""])
            indexset_2.add(data=[1, 2, 3])
            equation = run.optimization.equations.create(
                "Equation",
                constrained_to_indexsets=[indexset.name, indexset_2.name],
            )
            equation.add(data=test_data_1)
        assert equation.data == test_data_1
        assert equation.levels == test_data_1["levels"]
        assert equation.marginals == test_data_1["marginals"]

        test_data_2 = {
            indexset.name: ["", "", "foo", "foo", "bar", "bar"],
            indexset_2.name: [3, 1, 2, 1, 2, 3],
            "levels": [6, 5, 4, 3, 2, 1],
            "marginals": [1, 3, 5, 6, 4, 2],
        }

        with run.transact("Test Equation.add() errors and order"):
            equation_2 = run.optimization.equations.create(
                name="Equation 2",
                constrained_to_indexsets=[indexset.name, indexset_2.name],
            )

            with pytest.raises(
                OptimizationItemUsageError,
                match=r"must include the column\(s\): marginals!",
            ):
                equation_2.add(
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
                equation_2.add(
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
                equation_2.add(
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
                equation_2.add(
                    data={
                        indexset.name: ["foo", "foo"],
                        indexset_2.name: [2, 2],
                        "levels": [1, 2],
                        "marginals": [3.4, 5.6],
                    },
                )

            # Test adding to scalar equation raises
            with pytest.raises(
                OptimizationDataValidationError,
                match="Trying to add data to unknown columns!",
            ):
                equation_5 = run.optimization.equations.create("Equation 5")
                equation_5.add(data={"foo": ["bar"], "levels": [1], "marginals": [0]})

            # Test that order is conserved
            equation_2.add(test_data_2)

        assert equation_2.data == test_data_2
        assert equation_2.levels == test_data_2["levels"]
        assert equation_2.marginals == test_data_2["marginals"]

        # Test updating of existing keys
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

        with run.transact("Test Equation.add() update"):
            equation_4 = run.optimization.equations.create(
                name="Equation 4",
                constrained_to_indexsets=[indexset.name, indexset_2.name],
            )
            equation_4.add(data=test_data_6)
            equation_4.add(data=test_data_7)

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
            expected, pd.DataFrame(equation_4.data), check_dtype=False
        )

        # Test adding with column_names
        test_data_8 = {
            "Column 1": ["", "", "foo", "foo", "bar", "bar"],
            "Column 2": [3, 1, 2, 1, 2, 3],
            "levels": [6, 5, 4, 3, 2, 1],
            "marginals": [0.5] * 6,
        }

        with run.transact("Test Equation.add() column_names"):
            equation_6 = run.optimization.equations.create(
                name="Equation 6",
                constrained_to_indexsets=[indexset.name, indexset_2.name],
                column_names=["Column 1", "Column 2"],
            )
            equation_6.add(data=test_data_8)

        assert equation_6.data == test_data_8

        # Test adding empty data works
        with run.transact("Test Equation.add() empty data"):
            equation_6.add(pd.DataFrame())

        assert equation_6.data == test_data_8

    def test_equation_remove_data(
        self, platform: ixmp4.Platform, caplog: pytest.LogCaptureFixture
    ) -> None:
        run = platform.runs.create("Model", "Scenario")
        with run.transact("Test Equation.remove_data() -- preparation"):
            indexset = run.optimization.indexsets.create("Indexset")
            indexset.add(data=["foo", "bar"])
            test_data: dict[str, list[float | int | str]] = {
                indexset.name: ["bar", "foo"],
                "levels": [2.3, 1],
                "marginals": [0, 4.2],
            }
            equation = run.optimization.equations.create(
                "Equation",
                constrained_to_indexsets=[indexset.name],
            )
            equation.add(test_data)
        assert equation.data == test_data

        # Test removing empty data removes nothing
        with run.transact("Test Equation.remove_data() empty"):
            equation.remove_data(data={})

        assert equation.data == test_data

        remove_data = {indexset.name: [test_data[indexset.name][0]]}
        test_data_2 = {k: [v[1]] for k, v in test_data.items()}

        with run.transact("Test Equation.remove_data() errors and single"):
            # Test incomplete index raises...
            with pytest.raises(
                OptimizationItemUsageError, match="data to be removed must specify"
            ):
                equation.remove_data(data={"foo": ["bar"]})

            # ...even when removing a column that's known in principle
            with pytest.raises(
                OptimizationItemUsageError, match="data to be removed must specify"
            ):
                equation.remove_data(data={"levels": [2.3]})

            # Test removing one row
            equation.remove_data(data=remove_data)

        assert equation.data == test_data_2

        # Test removing non-existing (but correctly formatted) data works, even with
        # additional/unused columns
        remove_data["levels"] = [1]
        with run.transact("Test Equation.remove_data() non-existing"):
            equation.remove_data(data=remove_data)

        assert equation.data == test_data_2

        # Test removing all rows
        with run.transact("Test Equation.remove_data() all data"):
            equation.remove_data()

        assert equation.data == {}

        # Test removing specific data from unindexed Equation warns
        with run.transact("Test Equation.remove_data() warn unindexed"):
            equation_2 = run.optimization.equations.create("Equation 2")

            caplog.clear()
            with caplog.at_level("WARNING", logger=logger.name):
                equation_2.remove_data(data=test_data_2)

        expected = [
            f"Trying to remove {test_data_2} from Equation '{equation_2.name}', but "
            "that is not indexed; not removing anything!"
        ]
        assert caplog.messages == expected

    def test_list_equation(self, platform: ixmp4.Platform) -> None:
        run = platform.runs.create("Model", "Scenario")
        indexset, indexset_2 = create_indexsets_for_run(
            platform=platform, run_id=run.id
        )
        with run.transact("Test equations.list()"):
            equation = run.optimization.equations.create(
                "Equation", constrained_to_indexsets=[indexset.name]
            )
            equation_2 = run.optimization.equations.create(
                "Equation 2", constrained_to_indexsets=[indexset_2.name]
            )

        # Create new run to test listing equations of specific run
        run_2 = platform.runs.create("Model", "Scenario")
        (indexset,) = create_indexsets_for_run(
            platform=platform, run_id=run_2.id, amount=1
        )
        with run_2.transact("Test equations.list() for specific run"):
            run_2.optimization.equations.create(
                "Equation", constrained_to_indexsets=[indexset.name]
            )
        expected_ids = [equation.id, equation_2.id]
        list_ids = [equation.id for equation in run.optimization.equations.list()]
        assert not (set(expected_ids) ^ set(list_ids))

        # Test retrieving just one result by providing a name
        expected_id = [equation.id]
        list_id = [
            equation.id for equation in run.optimization.equations.list(name="Equation")
        ]
        assert not (set(expected_id) ^ set(list_id))

    def test_tabulate_equation(self, platform: ixmp4.Platform) -> None:
        run = platform.runs.create("Model", "Scenario")
        indexset, indexset_2 = tuple(
            IndexSet(_backend=platform.backend, _model=model, _run=run)
            for model in create_indexsets_for_run(platform=platform, run_id=run.id)
        )
        with run.transact("Test equations.tabulate()"):
            equation = run.optimization.equations.create(
                name="Equation",
                constrained_to_indexsets=[indexset.name, indexset_2.name],
            )
            equation_2 = run.optimization.equations.create(
                name="Equation 2",
                constrained_to_indexsets=[indexset.name, indexset_2.name],
            )

        # Create new run to test tabulating equations of specific run
        run_2 = platform.runs.create("Model", "Scenario")
        (indexset_3,) = create_indexsets_for_run(
            platform=platform, run_id=run_2.id, amount=1
        )
        with run_2.transact("Test equations.tabulate() for specific run"):
            run_2.optimization.equations.create(
                "Equation", constrained_to_indexsets=[indexset_3.name]
            )
        pd.testing.assert_frame_equal(
            df_from_list([equation_2]),
            run.optimization.equations.tabulate(name="Equation 2"),
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

        with run.transact("Test equations.tabulate() with data"):
            indexset.add(data=["foo", "bar"])
            indexset_2.add(data=[1, 2, 3])
            equation.add(data=test_data_1)
            equation_2.add(data=test_data_2)

        pd.testing.assert_frame_equal(
            df_from_list([equation, equation_2]),
            run.optimization.equations.tabulate(),
        )

    def test_equation_docs(self, platform: ixmp4.Platform) -> None:
        run = platform.runs.create("Model", "Scenario")
        (indexset,) = tuple(
            IndexSet(_backend=platform.backend, _model=model, _run=run)
            for model in create_indexsets_for_run(
                platform=platform, run_id=run.id, amount=1
            )
        )
        with run.transact("Test Equation.docs"):
            equation_1 = run.optimization.equations.create(
                "Equation 1", constrained_to_indexsets=[indexset.name]
            )
        docs = "Documentation of Equation 1"
        equation_1.docs = docs
        assert equation_1.docs == docs

        equation_1.docs = None
        assert equation_1.docs is None
