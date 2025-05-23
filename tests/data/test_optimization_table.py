import pandas as pd
import pytest

import ixmp4
from ixmp4.core.exceptions import (
    OptimizationDataValidationError,
    OptimizationItemUsageError,
)
from ixmp4.data.abstract import Table

from ..utils import create_indexsets_for_run


def df_from_list(tables: list[Table]) -> pd.DataFrame:
    return pd.DataFrame(
        [
            [
                table.run__id,
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


class TestDataOptimizationTable:
    def test_create_table(self, platform: ixmp4.Platform) -> None:
        run = platform.backend.runs.create("Model", "Scenario")

        # Test normal creation
        indexset_1, indexset_2 = create_indexsets_for_run(
            platform=platform, run_id=run.id
        )
        table = platform.backend.optimization.tables.create(
            run_id=run.id, name="Table", constrained_to_indexsets=[indexset_1.name]
        )

        assert table.run__id == run.id
        assert table.name == "Table"
        assert table.data == {}  # JsonDict type currently requires a dict, not None
        assert table.indexset_names == [indexset_1.name]
        assert table.column_names is None

        # Test duplicate name raises
        with pytest.raises(Table.NotUnique):
            _ = platform.backend.optimization.tables.create(
                run_id=run.id, name="Table", constrained_to_indexsets=[indexset_1.name]
            )

        # Test mismatch in constrained_to_indexsets and column_names raises
        with pytest.raises(OptimizationItemUsageError, match="not equal in length"):
            _ = platform.backend.optimization.tables.create(
                run_id=run.id,
                name="Table 2",
                constrained_to_indexsets=[indexset_1.name],
                column_names=["Dimension 1", "Dimension 2"],
            )

        # Test columns_names are used for names if given
        table_2 = platform.backend.optimization.tables.create(
            run_id=run.id,
            name="Table 2",
            constrained_to_indexsets=[indexset_1.name],
            column_names=["Column 1"],
        )
        table_2.column_names == ["Column 1"]

        # Test duplicate column_names raise
        with pytest.raises(
            OptimizationItemUsageError, match="`column_names` are not unique"
        ):
            _ = platform.backend.optimization.tables.create(
                run_id=run.id,
                name="Table 3",
                constrained_to_indexsets=[indexset_1.name, indexset_1.name],
                column_names=["Column 1", "Column 1"],
            )

        # Test using different column names for same indexset
        table_3 = platform.backend.optimization.tables.create(
            run_id=run.id,
            name="Table 3",
            constrained_to_indexsets=[indexset_1.name, indexset_1.name],
            column_names=["Column 1", "Column 2"],
        )

        assert table_3.column_names == ["Column 1", "Column 2"]
        assert table_3.indexset_names == [indexset_1.name, indexset_1.name]

    def test_delete_table(self, platform: ixmp4.Platform) -> None:
        run = platform.backend.runs.create("Model", "Scenario")
        indexset_1, _ = create_indexsets_for_run(platform=platform, run_id=run.id)
        table = platform.backend.optimization.tables.create(
            run_id=run.id, name="Table", constrained_to_indexsets=[indexset_1.name]
        )

        # TODO How to check that DeletionPrevented is raised? No other object uses
        # Table.id, so nothing could prevent the deletion.

        # Test unknown id raises
        with pytest.raises(Table.NotFound):
            platform.backend.optimization.tables.delete(id=(table.id + 1))

        # Test normal deletion
        platform.backend.optimization.tables.delete(id=table.id)

        assert platform.backend.optimization.tables.tabulate().empty

        # Confirm that IndexSet has not been deleted
        assert not platform.backend.optimization.indexsets.tabulate().empty

        # Test that association table rows are deleted
        # If they haven't, this would raise DeletionPrevented
        platform.backend.optimization.indexsets.delete(id=indexset_1.id)

    def test_get_table(self, platform: ixmp4.Platform) -> None:
        run = platform.backend.runs.create("Model", "Scenario")
        _, _ = create_indexsets_for_run(platform=platform, run_id=run.id)
        table = platform.backend.optimization.tables.create(
            run_id=run.id, name="Table", constrained_to_indexsets=["Indexset 1"]
        )
        assert table == platform.backend.optimization.tables.get(
            run_id=run.id, name="Table"
        )

        with pytest.raises(Table.NotFound):
            _ = platform.backend.optimization.tables.get(run_id=run.id, name="Table 2")

    def test_table_add_data(self, platform: ixmp4.Platform) -> None:
        run = platform.backend.runs.create("Model", "Scenario")
        indexset_1, indexset_2 = create_indexsets_for_run(
            platform=platform, run_id=run.id
        )
        platform.backend.optimization.indexsets.add_data(
            id=indexset_1.id, data=["foo", "bar", ""]
        )
        platform.backend.optimization.indexsets.add_data(
            id=indexset_2.id, data=[1, 2, 3]
        )
        # pandas can only convert dicts to dataframes if the values are lists
        # or if index is given. But maybe using read_json instead of from_dict
        # can remedy this. Or maybe we want to catch the resulting
        # "ValueError: If using all scalar values, you must pass an index" and
        # reraise a custom informative error?
        test_data_1 = {indexset_1.name: ["foo"], indexset_2.name: [1]}
        table = platform.backend.optimization.tables.create(
            run_id=run.id,
            name="Table",
            constrained_to_indexsets=[indexset_1.name, indexset_2.name],
        )
        platform.backend.optimization.tables.add_data(id=table.id, data=test_data_1)

        table = platform.backend.optimization.tables.get(run_id=run.id, name="Table")
        assert table.data == test_data_1

        table_2 = platform.backend.optimization.tables.create(
            run_id=run.id,
            name="Table 2",
            constrained_to_indexsets=[indexset_1.name, indexset_2.name],
        )

        with pytest.raises(OptimizationDataValidationError, match="missing values"):
            platform.backend.optimization.tables.add_data(
                id=table_2.id,
                data=pd.DataFrame({indexset_1.name: [None], indexset_2.name: [2]}),
                # empty string is allowed for now (see below), but None or NaN raise
            )

        with pytest.raises(
            OptimizationDataValidationError, match="contains duplicate rows"
        ):
            platform.backend.optimization.tables.add_data(
                id=table_2.id,
                data={indexset_1.name: ["foo", "foo"], indexset_2.name: [2, 2]},
            )

        # Test raising on unrecognised data.values()
        with pytest.raises(
            OptimizationDataValidationError,
            match="contains values that are not allowed",
        ):
            platform.backend.optimization.tables.add_data(
                id=table_2.id,
                data={indexset_1.name: ["foo"], indexset_2.name: [0]},
            )

        test_data_2 = {indexset_1.name: [""], indexset_2.name: [3]}
        platform.backend.optimization.tables.add_data(id=table_2.id, data=test_data_2)
        table_2 = platform.backend.optimization.tables.get(
            run_id=run.id, name="Table 2"
        )
        assert table_2.data == test_data_2

        table_3 = platform.backend.optimization.tables.create(
            run_id=run.id,
            name="Table 3",
            constrained_to_indexsets=[indexset_1.name, indexset_2.name],
            column_names=["Column 1", "Column 2"],
        )
        with pytest.raises(
            OptimizationDataValidationError, match="Data is missing for some columns!"
        ):
            platform.backend.optimization.tables.add_data(
                id=table_3.id, data={"Column 1": ["bar"]}
            )

        test_data_3 = {"Column 1": ["bar"], "Column 2": [2]}
        platform.backend.optimization.tables.add_data(id=table_3.id, data=test_data_3)
        table_3 = platform.backend.optimization.tables.get(
            run_id=run.id, name="Table 3"
        )
        assert table_3.data == test_data_3

        # Test data is expanded when Column.name is already present
        platform.backend.optimization.tables.add_data(
            id=table_3.id, data=pd.DataFrame({"Column 1": ["foo"], "Column 2": [3]})
        )
        table_3 = platform.backend.optimization.tables.get(
            run_id=run.id, name="Table 3"
        )
        assert table_3.data == {"Column 1": ["bar", "foo"], "Column 2": [2, 3]}

        # Test raising on non-existing Column.name
        with pytest.raises(
            OptimizationDataValidationError,
            match="Trying to add data to unknown columns!",
        ):
            platform.backend.optimization.tables.add_data(
                id=table_3.id, data={"Column 3": [1]}
            )

        # Test that order is not important...
        table_4 = platform.backend.optimization.tables.create(
            run_id=run.id,
            name="Table 4",
            constrained_to_indexsets=[indexset_1.name, indexset_2.name],
            column_names=["Column 1", "Column 2"],
        )
        test_data_4 = {"Column 2": [2], "Column 1": ["bar"]}
        platform.backend.optimization.tables.add_data(id=table_4.id, data=test_data_4)
        table_4 = platform.backend.optimization.tables.get(
            run_id=run.id, name="Table 4"
        )
        assert table_4.data == test_data_4

        # ...even for expanding
        platform.backend.optimization.tables.add_data(
            id=table_4.id, data={"Column 1": ["foo"], "Column 2": [1]}
        )
        table_4 = platform.backend.optimization.tables.get(
            run_id=run.id, name="Table 4"
        )
        assert table_4.data == {"Column 2": [2, 1], "Column 1": ["bar", "foo"]}

        # This doesn't seem to test a distinct case compared to the above
        with pytest.raises(
            OptimizationDataValidationError,
            match="Trying to add data to unknown columns!",
        ):
            platform.backend.optimization.tables.add_data(
                id=table_4.id,
                data={"Column 1": ["bar"], "Column 2": [3], "Indexset": ["foo"]},
            )

        # Test various data types
        indexset_3 = platform.backend.optimization.indexsets.create(
            run_id=run.id, name="Indexset 3"
        )
        test_data_5 = {
            indexset_1.name: ["foo", "foo", "bar"],
            indexset_3.name: [1.0, 2.2, 3.14],
        }

        platform.backend.optimization.indexsets.add_data(
            id=indexset_3.id, data=[1.0, 2.2, 3.14]
        )
        table_5 = platform.backend.optimization.tables.create(
            run_id=run.id,
            name="Table 5",
            constrained_to_indexsets=[indexset_1.name, indexset_3.name],
        )
        platform.backend.optimization.tables.add_data(id=table_5.id, data=test_data_5)
        table_5 = platform.backend.optimization.tables.get(
            run_id=run.id, name="Table 5"
        )
        assert table_5.data == test_data_5

        # This doesn't raise since the union of existing and new data is validated
        platform.backend.optimization.tables.add_data(id=table_5.id, data={})
        table_5 = platform.backend.optimization.tables.get(
            run_id=run.id, name="Table 5"
        )
        assert table_5.data == test_data_5

    def test_table_remove_data(self, platform: ixmp4.Platform) -> None:
        run = platform.backend.runs.create("Model", "Scenario")
        indexset_1, indexset_2 = create_indexsets_for_run(
            platform=platform, run_id=run.id
        )
        platform.backend.optimization.indexsets.add_data(
            id=indexset_1.id, data=["foo", "bar", ""]
        )
        platform.backend.optimization.indexsets.add_data(
            id=indexset_2.id, data=[1, 2, 3]
        )
        initial_data: dict[str, list[int] | list[str]] = {
            indexset_1.name: ["foo", "foo", "foo", "bar", "bar", "bar"],
            indexset_2.name: [1, 2, 3, 1, 2, 3],
        }
        table = platform.backend.optimization.tables.create(
            run_id=run.id,
            name="Table",
            constrained_to_indexsets=[indexset_1.name, indexset_2.name],
        )
        platform.backend.optimization.tables.add_data(id=table.id, data=initial_data)

        # Test removing empty data removes nothing
        platform.backend.optimization.tables.remove_data(id=table.id, data={})
        table = platform.backend.optimization.tables.get(run_id=run.id, name="Table")

        assert table.data == initial_data

        # Test incomplete index raises
        with pytest.raises(
            OptimizationItemUsageError, match="data to be removed must specify"
        ):
            platform.backend.optimization.tables.remove_data(
                id=table.id, data={indexset_1.name: ["foo"]}
            )

        # Test unknown keys without indexed columns raises
        with pytest.raises(
            OptimizationItemUsageError, match="data to be removed must specify"
        ):
            platform.backend.optimization.tables.remove_data(
                id=table.id, data={"foo": ["bar"]}
            )

        # Test removing one row
        remove_data_1: dict[str, list[int] | list[str]] = {
            indexset_1.name: ["foo"],
            indexset_2.name: [1],
        }
        platform.backend.optimization.tables.remove_data(
            id=table.id, data=remove_data_1
        )
        table = platform.backend.optimization.tables.get(run_id=run.id, name="Table")

        # Prepare the expectation from the original test data
        # You can confirm manually that only the correct types are removed
        for key in remove_data_1.keys():
            initial_data[key].remove(remove_data_1[key][0])  # type: ignore[arg-type]

        assert table.data == initial_data

        # Test removing non-existing (but correctly formatted) data works, even with
        # additional/unused columns
        remove_data_1["foo"] = ["bar"]
        platform.backend.optimization.tables.remove_data(
            id=table.id, data=remove_data_1
        )
        table = platform.backend.optimization.tables.get(run_id=run.id, name="Table")

        assert table.data == initial_data

        # Test removing multiple rows
        remove_data_2 = pd.DataFrame(
            {indexset_1.name: ["foo", "bar", "bar"], indexset_2.name: [3, 1, 3]}
        )
        platform.backend.optimization.tables.remove_data(
            id=table.id, data=remove_data_2
        )
        table = platform.backend.optimization.tables.get(run_id=run.id, name="Table")

        # Prepare the expectation
        expected = {indexset_1.name: ["foo", "bar"], indexset_2.name: [2, 2]}

        assert table.data == expected

        # Test removing all remaining data
        remove_data_3 = {indexset_1.name: ["foo", "bar"], indexset_2.name: [2, 2]}
        platform.backend.optimization.tables.remove_data(
            id=table.id, data=remove_data_3
        )
        table = platform.backend.optimization.tables.get(run_id=run.id, name="Table")

        assert table.data == {}

    def test_list_table(self, platform: ixmp4.Platform) -> None:
        run = platform.backend.runs.create("Model", "Scenario")
        create_indexsets_for_run(platform=platform, run_id=run.id)
        table = platform.backend.optimization.tables.create(
            run_id=run.id, name="Table", constrained_to_indexsets=["Indexset 1"]
        )
        table_2 = platform.backend.optimization.tables.create(
            run_id=run.id, name="Table 2", constrained_to_indexsets=["Indexset 2"]
        )
        assert [table, table_2] == platform.backend.optimization.tables.list()

        assert [table] == platform.backend.optimization.tables.list(name="Table")
        # Test listing of Tables when specifying a Run
        run_2 = platform.backend.runs.create("Model", "Scenario")
        indexset_3, indexset_4 = create_indexsets_for_run(
            platform=platform, run_id=run_2.id, offset=2
        )
        table_3 = platform.backend.optimization.tables.create(
            run_id=run_2.id, name="Table", constrained_to_indexsets=[indexset_3.name]
        )
        table_4 = platform.backend.optimization.tables.create(
            run_id=run_2.id, name="Table 2", constrained_to_indexsets=[indexset_4.name]
        )
        assert [table_3, table_4] == platform.backend.optimization.tables.list(
            run_id=run_2.id
        )

    def test_tabulate_table(self, platform: ixmp4.Platform) -> None:
        run = platform.backend.runs.create("Model", "Scenario")
        indexset_1, indexset_2 = create_indexsets_for_run(
            platform=platform, run_id=run.id, offset=2
        )
        table = platform.backend.optimization.tables.create(
            run_id=run.id,
            name="Table",
            constrained_to_indexsets=[indexset_1.name, indexset_2.name],
        )
        table_2 = platform.backend.optimization.tables.create(
            run_id=run.id,
            name="Table 2",
            constrained_to_indexsets=[indexset_1.name, indexset_2.name],
        )
        pd.testing.assert_frame_equal(
            df_from_list([table_2]),
            platform.backend.optimization.tables.tabulate(name="Table 2"),
        )

        platform.backend.optimization.indexsets.add_data(
            id=indexset_1.id, data=["foo", "bar"]
        )
        platform.backend.optimization.indexsets.add_data(
            id=indexset_2.id, data=[1, 2, 3]
        )
        test_data_1 = {indexset_1.name: ["foo"], indexset_2.name: [1]}
        platform.backend.optimization.tables.add_data(id=table.id, data=test_data_1)
        table = platform.backend.optimization.tables.get(run_id=run.id, name="Table")

        test_data_2 = {indexset_2.name: [2, 3], indexset_1.name: ["foo", "bar"]}
        platform.backend.optimization.tables.add_data(id=table_2.id, data=test_data_2)
        table_2 = platform.backend.optimization.tables.get(
            run_id=run.id, name="Table 2"
        )
        pd.testing.assert_frame_equal(
            df_from_list([table, table_2]),
            platform.backend.optimization.tables.tabulate(),
        )

        # Test tabulation of Tables when specifying a Run
        run_2 = platform.backend.runs.create("Model", "Scenario")
        indexset_3, indexset_4 = create_indexsets_for_run(
            platform=platform, run_id=run_2.id, offset=2
        )
        table_3 = platform.backend.optimization.tables.create(
            run_id=run_2.id, name="Table", constrained_to_indexsets=[indexset_3.name]
        )
        table_4 = platform.backend.optimization.tables.create(
            run_id=run_2.id, name="Table 2", constrained_to_indexsets=[indexset_4.name]
        )
        pd.testing.assert_frame_equal(
            df_from_list([table_3, table_4]),
            platform.backend.optimization.tables.tabulate(run_id=run_2.id),
        )
