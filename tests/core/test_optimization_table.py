import pandas as pd
import pytest

import ixmp4
from ixmp4.core import IndexSet, Table
from ixmp4.core.exceptions import (
    OptimizationDataValidationError,
    OptimizationItemUsageError,
)

from ..utils import create_indexsets_for_run


def df_from_list(tables: list[Table]) -> pd.DataFrame:
    return pd.DataFrame(
        # Order is important here to avoid utils.assert_unordered_equality,
        # which doesn't like lists
        [
            [
                table.run_id,
                table.data,
                table.name,
                table.id,
                table.created_at,
                table.created_by,
            ]
            for table in tables
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


class TestCoreTable:
    def test_create_table(self, platform: ixmp4.Platform) -> None:
        run = platform.runs.create("Model", "Scenario")

        # Test normal creation
        indexset_1, indexset_2 = tuple(
            IndexSet(_backend=platform.backend, _model=model, _run=run)
            for model in create_indexsets_for_run(platform=platform, run_id=run.id)
        )
        with run.transact("Test tables.create()"):
            table = run.optimization.tables.create(
                "Table 1",
                constrained_to_indexsets=[indexset_1.name],
            )
        assert table.run_id == run.id
        assert table.id == 1
        assert table.name == "Table 1"
        assert table.data == {}
        assert table.indexset_names == [indexset_1.name]
        assert table.column_names is None

        with run.transact("Test tables.create() errors and column_names"):
            # Test duplicate name raises
            with pytest.raises(Table.NotUnique):
                _ = run.optimization.tables.create(
                    "Table 1", constrained_to_indexsets=[indexset_1.name]
                )

            # Test mismatch in constrained_to_indexsets and column_names raises
            with pytest.raises(OptimizationItemUsageError, match="not equal in length"):
                _ = run.optimization.tables.create(
                    name="Table 2",
                    constrained_to_indexsets=[indexset_1.name],
                    column_names=["Dimension 1", "Dimension 2"],
                )

            # Test columns_names are used for names if given
            table_2 = run.optimization.tables.create(
                name="Table 2",
                constrained_to_indexsets=[indexset_1.name],
                column_names=["Column 1"],
            )
        assert table_2.column_names == ["Column 1"]

        with run.transact("Test tables.create() multiple column_names"):
            # Test duplicate column_names raise
            with pytest.raises(
                OptimizationItemUsageError, match="`column_names` are not unique"
            ):
                _ = run.optimization.tables.create(
                    name="Table 3",
                    constrained_to_indexsets=[indexset_1.name, indexset_1.name],
                    column_names=["Column 1", "Column 1"],
                )

            # Test using different column names for same indexset
            table_3 = run.optimization.tables.create(
                name="Table 3",
                constrained_to_indexsets=[indexset_1.name, indexset_1.name],
                column_names=["Column 1", "Column 2"],
            )

        assert table_3.column_names == ["Column 1", "Column 2"]
        assert table_3.indexset_names == [indexset_1.name, indexset_1.name]

    def test_delete_table(self, platform: ixmp4.Platform) -> None:
        run = platform.runs.create("Model", "Scenario")
        (indexset_1,) = create_indexsets_for_run(
            platform=platform, run_id=run.id, amount=1
        )
        with run.transact("Test tables.delete()"):
            table = run.optimization.tables.create(
                name="Table", constrained_to_indexsets=[indexset_1.name]
            )

            # TODO How to check that DeletionPrevented is raised? No other object uses
            # Table.id, so nothing could prevent the deletion.

            # Test unknown name raises
            with pytest.raises(Table.NotFound):
                run.optimization.tables.delete(item="does not exist")

            # Test normal deletion
            run.optimization.tables.delete(item=table.name)

        assert run.optimization.tables.tabulate().empty

        # Confirm that IndexSet has not been deleted
        assert not run.optimization.indexsets.tabulate().empty

        with run.transact("Test tables.delete() indexset linkage"):
            # Test that association table rows are deleted
            # If they haven't, this would raise DeletionPrevented
            run.optimization.indexsets.delete(item=indexset_1.id)

    def test_get_table(self, platform: ixmp4.Platform) -> None:
        run = platform.runs.create("Model", "Scenario")
        (indexset,) = create_indexsets_for_run(
            platform=platform, run_id=run.id, amount=1
        )
        with run.transact("Test tables.get()"):
            _ = run.optimization.tables.create(
                name="Table", constrained_to_indexsets=[indexset.name]
            )
        table = run.optimization.tables.get("Table")
        assert table.run_id == run.id
        assert table.id == 1
        assert table.name == "Table"
        assert table.data == {}
        assert table.indexset_names == [indexset.name]

        with pytest.raises(Table.NotFound):
            _ = run.optimization.tables.get(name="Table 2")

    def test_table_add_data(self, platform: ixmp4.Platform) -> None:
        run = platform.runs.create("Model", "Scenario")
        indexset, indexset_2 = tuple(
            IndexSet(_backend=platform.backend, _model=model, _run=run)
            for model in create_indexsets_for_run(platform=platform, run_id=run.id)
        )
        with run.transact("Test Table.add()"):
            indexset.add(data=["foo", "bar", ""])
            indexset_2.add([1, 2, 3])
            # pandas can only convert dicts to dataframes if the values are lists
            # or if index is given. But maybe using read_json instead of from_dict
            # can remedy this. Or maybe we want to catch the resulting
            # "ValueError: If using all scalar values, you must pass an index" and
            # reraise a custom informative error?
            test_data_1 = {indexset.name: ["foo"], indexset_2.name: [1]}
            table = run.optimization.tables.create(
                "Table",
                constrained_to_indexsets=[indexset.name, indexset_2.name],
            )
            table.add(data=test_data_1)
        assert table.data == test_data_1

        test_data_2 = {indexset.name: [""], indexset_2.name: [3]}

        with run.transact("Test Table.add() errors"):
            table_2 = run.optimization.tables.create(
                name="Table 2",
                constrained_to_indexsets=[indexset.name, indexset_2.name],
            )

            with pytest.raises(OptimizationDataValidationError, match="missing values"):
                table_2.add(
                    pd.DataFrame({indexset.name: [None], indexset_2.name: [2]}),
                    # empty string is allowed for now, but None or NaN raise
                )

            with pytest.raises(
                OptimizationDataValidationError, match="contains duplicate rows"
            ):
                table_2.add(
                    data={indexset.name: ["foo", "foo"], indexset_2.name: [2, 2]},
                )

            # Test raising on unrecognised data.values()
            with pytest.raises(
                OptimizationDataValidationError,
                match="contains values that are not allowed",
            ):
                table_2.add(
                    data={indexset.name: ["foo"], indexset_2.name: [0]},
                )

            table_2.add(data=test_data_2)
        assert table_2.data == test_data_2

        # Test overwriting column names
        test_data_3 = {"Column 1": ["bar"], "Column 2": [2]}
        with run.transact("Test Table.add() column_names"):
            table_3 = run.optimization.tables.create(
                name="Table 3",
                constrained_to_indexsets=[indexset.name, indexset_2.name],
                column_names=["Column 1", "Column 2"],
            )
            with pytest.raises(
                OptimizationDataValidationError,
                match="Data is missing for some columns!",
            ):
                table_3.add(data={"Column 1": ["bar"]})

            table_3.add(data=test_data_3)
        assert table_3.data == test_data_3

        with run.transact("Test Table.add() column_names insert"):
            # Test raising on non-existing Column.name
            with pytest.raises(
                OptimizationDataValidationError,
                match="Trying to add data to unknown columns!",
            ):
                table_3.add(
                    {"Column 1": ["not there"], "Column 2": [2], "Column 3": [1]}
                )

            # Test data is expanded when Column.name is already present
            table_3.add(
                data=pd.DataFrame({"Column 1": ["foo"], "Column 2": [3]}),
            )
        assert table_3.data == {"Column 1": ["bar", "foo"], "Column 2": [2, 3]}

        test_data_4 = {"Column 2": [2], "Column 1": ["bar"]}

        with run.transact("Test Table.add() order"):
            # Test that order is not important...
            table_4 = run.optimization.tables.create(
                name="Table 4",
                constrained_to_indexsets=[indexset.name, indexset_2.name],
                column_names=["Column 1", "Column 2"],
            )
            table_4.add(data=test_data_4)
        assert table_4.data == test_data_4

        with run.transact("Test Table.add() order expanding"):
            # ...even for expanding
            table_4.add(data={"Column 1": ["foo"], "Column 2": [1]})
        assert table_4.data == {"Column 2": [2, 1], "Column 1": ["bar", "foo"]}

        with run.transact("Test Table.add() another error"):
            # This doesn't seem to test a distinct case compared to the above
            with pytest.raises(
                OptimizationDataValidationError,
                match="Trying to add data to unknown columns!",
            ):
                table_4.add(
                    data={
                        "Column 1": ["bar"],
                        "Column 2": [3],
                        "Indexset": ["foo"],
                    },
                )

        with run.transact("Test Table.add() order"):
            # Test various data types
            indexset_3 = run.optimization.indexsets.create(name="Indexset 3")

            indexset_3.add(data=[1.0, 2.2, 3.14])
            table_5 = run.optimization.tables.create(
                name="Table 5",
                constrained_to_indexsets=[indexset.name, indexset_3.name],
            )
            test_data_5 = {
                indexset.name: ["foo", "foo", "bar"],
                indexset_3.name: [1.0, 2.2, 3.14],
            }
            table_5.add(test_data_5)
        assert table_5.data == test_data_5

        with run.transact("Test Table.add() empty"):
            # Test adding nothing is a no-op
            table_5.add(data={})
        assert table_5.data == test_data_5

    def test_table_remove_data(self, platform: ixmp4.Platform) -> None:
        run = platform.runs.create("Model", "Scenario")

        # Prepare a table containing some test data
        indexset_1, indexset_2 = tuple(
            IndexSet(_backend=platform.backend, _model=model, _run=run)
            for model in create_indexsets_for_run(platform=platform, run_id=run.id)
        )
        initial_data: dict[str, list[int] | list[str]] = {
            indexset_1.name: ["foo", "foo", "foo", "bar", "bar", "bar"],
            indexset_2.name: [1, 2, 3, 1, 2, 3],
        }
        with run.transact("Test Table.remove()"):
            indexset_1.add(data=["foo", "bar", ""])
            indexset_2.add(data=[1, 2, 3])
            table = run.optimization.tables.create(
                name="Table",
                constrained_to_indexsets=[indexset_1.name, indexset_2.name],
            )
            table.add(data=initial_data)

            # Test removing empty data removes nothing
            table.remove(data={})

        assert table.data == initial_data

        remove_data_1: dict[str, list[int] | list[str]] = {
            indexset_1.name: ["foo"],
            indexset_2.name: [1],
        }

        with run.transact("Test Table.remove() errors and single"):
            # Test incomplete index raises
            with pytest.raises(
                OptimizationItemUsageError, match="data to be removed must specify"
            ):
                table.remove(data={indexset_1.name: ["foo"]})

            # Test unknown keys without indexed columns raises
            with pytest.raises(
                OptimizationItemUsageError, match="data to be removed must specify"
            ):
                table.remove(data={"foo": ["bar"]})

            # Test removing one row
            table.remove(data=remove_data_1)

        # Prepare the expectation from the original test data
        # You can confirm manually that only the correct types are removed
        for key in remove_data_1.keys():
            initial_data[key].remove(remove_data_1[key][0])  # type: ignore[arg-type]

        assert table.data == initial_data

        # Test removing non-existing (but correctly formatted) data works, even with
        # additional/unused columns
        remove_data_1["foo"] = ["bar"]
        with run.transact("Test Table.remove() non-existing"):
            table.remove(data=remove_data_1)

        assert table.data == initial_data

        # Test removing multiple rows
        remove_data_2 = pd.DataFrame(
            {indexset_1.name: ["foo", "bar", "bar"], indexset_2.name: [3, 1, 3]}
        )
        with run.transact("Test Table.remove() multiple"):
            table.remove(data=remove_data_2)

        # Prepare the expectation
        expected = {indexset_1.name: ["foo", "bar"], indexset_2.name: [2, 2]}

        assert table.data == expected

        # Test removing all remaining data
        remove_data_3 = {indexset_1.name: ["foo", "bar"], indexset_2.name: [2, 2]}
        with run.transact("Test Table.remove() all data"):
            table.remove(data=remove_data_3)

        assert table.data == {}

    def test_list_tables(self, platform: ixmp4.Platform) -> None:
        run = platform.runs.create("Model", "Scenario")
        create_indexsets_for_run(platform=platform, run_id=run.id)
        with run.transact("Test tables.list()"):
            table = run.optimization.tables.create(
                "Table", constrained_to_indexsets=["Indexset 1"]
            )
            table_2 = run.optimization.tables.create(
                "Table 2", constrained_to_indexsets=["Indexset 2"]
            )

        # Create table in another run to test listing tables for specific run
        run_2 = platform.runs.create("Model", "Scenario")
        with run_2.transact("Test tables.list() 2"):
            indexset_3 = run_2.optimization.indexsets.create("Indexset 3")
            run_2.optimization.tables.create(
                "Table 1", constrained_to_indexsets=[indexset_3.name]
            )

        expected_ids = [table.id, table_2.id]
        list_ids = [table.id for table in run.optimization.tables.list()]
        assert not (set(expected_ids) ^ set(list_ids))

        # Test retrieving just one result by providing a name
        expected_id = [table.id]
        list_id = [table.id for table in run.optimization.tables.list(name="Table")]
        assert not (set(expected_id) ^ set(list_id))

    def test_tabulate_table(self, platform: ixmp4.Platform) -> None:
        run = platform.runs.create("Model", "Scenario")
        indexset, indexset_2 = tuple(
            IndexSet(_backend=platform.backend, _model=model, _run=run)
            for model in create_indexsets_for_run(platform=platform, run_id=run.id)
        )
        with run.transact("Test tables.tabulate()"):
            table = run.optimization.tables.create(
                name="Table",
                constrained_to_indexsets=[indexset.name, indexset_2.name],
            )
            table_2 = run.optimization.tables.create(
                name="Table 2",
                constrained_to_indexsets=[indexset.name, indexset_2.name],
            )

        # Create table in another run to test listing tables for specific run
        run_2 = platform.runs.create("Model", "Scenario")
        with run_2.transact("Test tables.tabulate() 2"):
            indexset_3 = run_2.optimization.indexsets.create("Indexset 3")
            run_2.optimization.tables.create(
                "Table 1", constrained_to_indexsets=[indexset_3.name]
            )

        pd.testing.assert_frame_equal(
            df_from_list([table_2]),
            run.optimization.tables.tabulate(name="Table 2"),
        )

        with run.transact("Test tables.tabulate() with data"):
            indexset.add(["foo", "bar"])
            indexset_2.add([1, 2, 3])
            test_data_1 = {indexset.name: ["foo"], indexset_2.name: [1]}
            table.add(test_data_1)
            test_data_2 = {indexset_2.name: [2, 3], indexset.name: ["foo", "bar"]}
            table_2.add(test_data_2)

        pd.testing.assert_frame_equal(
            df_from_list([table, table_2]),
            run.optimization.tables.tabulate(),
        )

    def test_table_docs(self, platform: ixmp4.Platform) -> None:
        run = platform.runs.create("Model", "Scenario")
        (indexset,) = create_indexsets_for_run(
            platform=platform, run_id=run.id, amount=1
        )
        with run.transact("Test Table.docs"):
            table_1 = run.optimization.tables.create(
                "Table 1", constrained_to_indexsets=[indexset.name]
            )
        docs = "Documentation of Table 1"
        table_1.docs = docs
        assert table_1.docs == docs

        table_1.docs = None
        assert table_1.docs is None
