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
                table.name,
                table.id,
                table.created_at,
                table.created_by,
            ]
            for table in tables
        ],
        columns=[
            "run__id",
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
        indexset_1, _ = create_indexsets_for_run(platform=platform, run_id=run.id)
        table = platform.backend.optimization.tables.create(
            run_id=run.id, name="Table", constrained_to_indexsets=[indexset_1.name]
        )

        assert table.run__id == run.id
        assert table.name == "Table"
        assert table.data == {}
        assert table.indexsets == [indexset_1.name]
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
        assert table_2.column_names == ["Column 1"]

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
            indexset_id=indexset_1.id, data=["foo", "bar", ""]
        )
        platform.backend.optimization.indexsets.add_data(
            indexset_id=indexset_2.id, data=[1, 2, 3]
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
        platform.backend.optimization.tables.add_data(
            table_id=table.id, data=test_data_1
        )

        table = platform.backend.optimization.tables.get(run_id=run.id, name="Table")
        assert table.data == test_data_1

        table_2 = platform.backend.optimization.tables.create(
            run_id=run.id,
            name="Table 2",
            constrained_to_indexsets=[indexset_1.name, indexset_2.name],
        )

        with pytest.raises(OptimizationDataValidationError, match="missing values"):
            platform.backend.optimization.tables.add_data(
                table_id=table_2.id,
                data=pd.DataFrame({indexset_1.name: [None], indexset_2.name: [2]}),
                # empty string is allowed for now (see below), but None or NaN raise
            )

        with pytest.raises(
            OptimizationDataValidationError, match="contains duplicate rows"
        ):
            platform.backend.optimization.tables.add_data(
                table_id=table_2.id,
                data={indexset_1.name: ["foo", "foo"], indexset_2.name: [2, 2]},
            )

        # Test raising on unrecognised data.values()
        with pytest.raises(
            OptimizationDataValidationError,
            match="contains values that are not allowed",
        ):
            platform.backend.optimization.tables.add_data(
                table_id=table_2.id,
                data={indexset_1.name: ["foo"], indexset_2.name: [0]},
            )

        test_data_2 = {indexset_1.name: [""], indexset_2.name: [3]}
        platform.backend.optimization.tables.add_data(
            table_id=table_2.id, data=test_data_2
        )
        table_2 = platform.backend.optimization.tables.get(
            run_id=run.id, name="Table 2"
        )
        assert table_2.data == test_data_2

        # Test overwriting column names
        table_3 = platform.backend.optimization.tables.create(
            run_id=run.id,
            name="Table 3",
            constrained_to_indexsets=[indexset_1.name, indexset_2.name],
            column_names=["Column 1", "Column 2"],
        )
        with pytest.raises(
            OptimizationDataValidationError, match="Data is missing for some Columns!"
        ):
            platform.backend.optimization.tables.add_data(
                table_id=table_3.id, data={"Column 1": ["bar"]}
            )

        test_data_3 = {"Column 1": ["bar"], "Column 2": [2]}
        platform.backend.optimization.tables.add_data(
            table_id=table_3.id, data=test_data_3
        )
        table_3 = platform.backend.optimization.tables.get(
            run_id=run.id, name="Table 3"
        )
        assert table_3.data == test_data_3

        # Test data is expanded when Column.name is already present
        platform.backend.optimization.tables.add_data(
            table_id=table_3.id,
            data=pd.DataFrame({"Column 1": ["foo"], "Column 2": [3]}),
        )
        table_3 = platform.backend.optimization.tables.get(
            run_id=run.id, name="Table 3"
        )
        assert table_3.data == {"Column 1": ["bar", "foo"], "Column 2": [2, 3]}

        # Test raising on non-existing Column.name
        with pytest.raises(
            OptimizationDataValidationError,
            match="Trying to add data to unknown Columns!",
        ):
            platform.backend.optimization.tables.add_data(
                table_id=table_3.id,
                data={"Column 1": ["foo"], "Column 2": [1], "Column 3": [1]},
            )

        # Test that order is not important...
        table_4 = platform.backend.optimization.tables.create(
            run_id=run.id,
            name="Table 4",
            constrained_to_indexsets=[indexset_1.name, indexset_2.name],
        )
        test_data_4 = {indexset_2.name: [2], indexset_1.name: ["bar"]}
        platform.backend.optimization.tables.add_data(
            table_id=table_4.id, data=test_data_4
        )
        table_4 = platform.backend.optimization.tables.get(
            run_id=run.id, name="Table 4"
        )
        assert table_4.data == test_data_4

        # ...even for expanding
        platform.backend.optimization.tables.add_data(
            table_id=table_4.id, data={indexset_1.name: ["foo"], indexset_2.name: [1]}
        )
        table_4 = platform.backend.optimization.tables.get(
            run_id=run.id, name="Table 4"
        )
        assert table_4.data == {
            indexset_2.name: [2, 1],
            indexset_1.name: ["bar", "foo"],
        }

        # This doesn't seem to test a distinct case compared to the above
        with pytest.raises(
            OptimizationDataValidationError,
            match="Trying to add data to unknown Columns!",
        ):
            platform.backend.optimization.tables.add_data(
                table_id=table_4.id,
                data={
                    indexset_1.name: ["bar"],
                    indexset_2.name: [3],
                    "Indexset": ["foo"],
                },
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
            indexset_id=indexset_3.id, data=[1.0, 2.2, 3.14]
        )
        table_5 = platform.backend.optimization.tables.create(
            run_id=run.id,
            name="Table 5",
            constrained_to_indexsets=[indexset_1.name, indexset_3.name],
        )
        platform.backend.optimization.tables.add_data(
            table_id=table_5.id, data=test_data_5
        )
        table_5 = platform.backend.optimization.tables.get(
            run_id=run.id, name="Table 5"
        )
        assert table_5.data == test_data_5

        # This raises since only the new data are validated
        with pytest.raises(
            OptimizationDataValidationError, match="Data is missing for some Columns!"
        ):
            platform.backend.optimization.tables.add_data(table_id=table_5.id, data={})

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
            indexset_id=indexset_1.id, data=["foo", "bar"]
        )
        platform.backend.optimization.indexsets.add_data(
            indexset_id=indexset_2.id, data=[1, 2, 3]
        )
        test_data_1 = {indexset_1.name: ["foo"], indexset_2.name: [1]}
        platform.backend.optimization.tables.add_data(
            table_id=table.id, data=test_data_1
        )
        table = platform.backend.optimization.tables.get(run_id=run.id, name="Table")

        test_data_2 = {indexset_2.name: [2, 3], indexset_1.name: ["foo", "bar"]}
        platform.backend.optimization.tables.add_data(
            table_id=table_2.id, data=test_data_2
        )
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
