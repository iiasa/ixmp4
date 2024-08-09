import pandas as pd
import pytest

from ixmp4.core import IndexSet, Platform, Table

from ..utils import all_platforms, create_indexsets_for_run


def df_from_list(tables: list[Table]):
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


@all_platforms
class TestCoreTable:
    def test_create_table(self, test_mp, request):
        test_mp: Platform = request.getfixturevalue(test_mp)  # type: ignore
        run = test_mp.runs.create("Model", "Scenario")

        # Test normal creation
        indexset, indexset_2 = tuple(
            IndexSet(_backend=test_mp.backend, _model=model)
            for model in create_indexsets_for_run(platform=test_mp, run_id=run.id)
        )
        table = run.optimization.tables.create(
            "Table 1",
            constrained_to_indexsets=[indexset.name],
        )
        assert table.run_id == run.id
        assert table.id == 1
        assert table.name == "Table 1"
        assert table.data == {}
        assert table.columns[0].name == indexset.name
        assert table.constrained_to_indexsets == [indexset.name]

        # Test duplicate name raises
        with pytest.raises(Table.NotUnique):
            _ = run.optimization.tables.create(
                "Table 1", constrained_to_indexsets=[indexset.name]
            )

        # Test mismatch in constrained_to_indexsets and column_names raises
        with pytest.raises(ValueError, match="not equal in length"):
            _ = run.optimization.tables.create(
                name="Table 2",
                constrained_to_indexsets=[indexset.name],
                column_names=["Dimension 1", "Dimension 2"],
            )

        # Test columns_names are used for names if given
        table_2 = run.optimization.tables.create(
            name="Table 2",
            constrained_to_indexsets=[indexset.name],
            column_names=["Column 1"],
        )
        assert table_2.columns[0].name == "Column 1"

        # Test duplicate column_names raise
        with pytest.raises(ValueError, match="`column_names` are not unique"):
            _ = run.optimization.tables.create(
                name="Table 3",
                constrained_to_indexsets=[indexset.name, indexset.name],
                column_names=["Column 1", "Column 1"],
            )

        # Test column.dtype is registered correctly
        indexset_2.add(elements=2024)
        table_3 = run.optimization.tables.create(
            "Table 5",
            constrained_to_indexsets=[indexset.name, indexset_2.name],
        )
        # If indexset doesn't have elements, a generic dtype is registered
        assert table_3.columns[0].dtype == "object"
        assert table_3.columns[1].dtype == "int64"

    def test_get_table(self, test_mp, request):
        test_mp: Platform = request.getfixturevalue(test_mp)  # type: ignore
        run = test_mp.runs.create("Model", "Scenario")
        (indexset,) = create_indexsets_for_run(
            platform=test_mp, run_id=run.id, amount=1
        )
        _ = run.optimization.tables.create(
            name="Table", constrained_to_indexsets=[indexset.name]
        )
        table = run.optimization.tables.get("Table")
        assert table.run_id == run.id
        assert table.id == 1
        assert table.name == "Table"
        assert table.data == {}
        assert table.columns[0].name == indexset.name
        assert table.constrained_to_indexsets == [indexset.name]

        with pytest.raises(Table.NotFound):
            _ = run.optimization.tables.get(name="Table 2")

    def test_table_add_data(self, test_mp, request):
        test_mp: Platform = request.getfixturevalue(test_mp)  # type: ignore
        run = test_mp.runs.create("Model", "Scenario")
        indexset, indexset_2 = tuple(
            IndexSet(_backend=test_mp.backend, _model=model)
            for model in create_indexsets_for_run(platform=test_mp, run_id=run.id)
        )
        indexset.add(elements=["foo", "bar", ""])
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

        table_2 = run.optimization.tables.create(
            name="Table 2",
            constrained_to_indexsets=[indexset.name, indexset_2.name],
        )

        with pytest.raises(ValueError, match="missing values"):
            table_2.add(
                pd.DataFrame({indexset.name: [None], indexset_2.name: [2]}),
                # empty string is allowed for now, but None or NaN raise
            )

        with pytest.raises(ValueError, match="contains duplicate rows"):
            table_2.add(
                data={indexset.name: ["foo", "foo"], indexset_2.name: [2, 2]},
            )

        # Test raising on unrecognised data.values()
        with pytest.raises(ValueError, match="contains values that are not allowed"):
            table_2.add(
                data={indexset.name: ["foo"], indexset_2.name: [0]},
            )

        test_data_2 = {indexset.name: [""], indexset_2.name: [3]}
        table_2.add(data=test_data_2)
        assert table_2.data == test_data_2

        table_3 = run.optimization.tables.create(
            name="Table 3",
            constrained_to_indexsets=[indexset.name, indexset_2.name],
            column_names=["Column 1", "Column 2"],
        )
        with pytest.raises(ValueError, match="Data is missing for some Columns!"):
            table_3.add(data={"Column 1": ["bar"]})

        test_data_3 = {"Column 1": ["bar"], "Column 2": [2]}
        table_3.add(data=test_data_3)
        assert table_3.data == test_data_3

        # Test data is expanded when Column.name is already present
        table_3.add(
            data=pd.DataFrame({"Column 1": ["foo"], "Column 2": [3]}),
        )
        assert table_3.data == {"Column 1": ["bar", "foo"], "Column 2": [2, 3]}

        # Test raising on non-existing Column.name
        with pytest.raises(ValueError, match="Trying to add data to unknown Columns!"):
            table_3.add({"Column 3": [1]})

        # Test that order is not important...
        table_4 = run.optimization.tables.create(
            name="Table 4",
            constrained_to_indexsets=[indexset.name, indexset_2.name],
            column_names=["Column 1", "Column 2"],
        )
        test_data_4 = {"Column 2": [2], "Column 1": ["bar"]}
        table_4.add(data=test_data_4)
        assert table_4.data == test_data_4

        # ...even for expanding
        table_4.add(data={"Column 1": ["foo"], "Column 2": [1]})
        assert table_4.data == {"Column 2": [2, 1], "Column 1": ["bar", "foo"]}

        # This doesn't seem to test a distinct case compared to the above
        with pytest.raises(ValueError, match="Trying to add data to unknown Columns!"):
            table_4.add(
                data={"Column 1": ["bar"], "Column 2": [3], indexset.name: ["foo"]},
            )

        # Test various data types
        indexset_3 = run.optimization.indexsets.create(name="Indexset 3")
        test_data_5 = {
            indexset.name: ["foo", "foo", "bar"],
            indexset_3.name: [1, "2", 3.14],
        }
        indexset_3.add(elements=[1, "2", 3.14])
        table_5 = run.optimization.tables.create(
            name="Table 5",
            constrained_to_indexsets=[indexset.name, indexset_3.name],
        )
        table_5.add(test_data_5)
        assert table_5.data == test_data_5

        # This doesn't raise since the union of existing and new data is validated
        table_5.add(data={})
        assert table_5.data == test_data_5

    def test_list_tables(self, test_mp, request):
        test_mp: Platform = request.getfixturevalue(test_mp)  # type: ignore
        run = test_mp.runs.create("Model", "Scenario")
        create_indexsets_for_run(platform=test_mp, run_id=run.id)
        table = run.optimization.tables.create(
            "Table", constrained_to_indexsets=["Indexset 1"]
        )
        table_2 = run.optimization.tables.create(
            "Table 2", constrained_to_indexsets=["Indexset 2"]
        )
        # Create table in another run to test listing tables for specific run
        run_2 = test_mp.runs.create("Model", "Scenario")
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

    def test_tabulate_table(self, test_mp, request):
        test_mp: Platform = request.getfixturevalue(test_mp)  # type: ignore
        run = test_mp.runs.create("Model", "Scenario")
        indexset, indexset_2 = tuple(
            IndexSet(_backend=test_mp.backend, _model=model)
            for model in create_indexsets_for_run(platform=test_mp, run_id=run.id)
        )
        table = run.optimization.tables.create(
            name="Table",
            constrained_to_indexsets=[indexset.name, indexset_2.name],
        )
        table_2 = run.optimization.tables.create(
            name="Table 2",
            constrained_to_indexsets=[indexset.name, indexset_2.name],
        )
        # Create table in another run to test listing tables for specific run
        run_2 = test_mp.runs.create("Model", "Scenario")
        indexset_3 = run_2.optimization.indexsets.create("Indexset 3")
        run_2.optimization.tables.create(
            "Table 1", constrained_to_indexsets=[indexset_3.name]
        )

        pd.testing.assert_frame_equal(
            df_from_list([table_2]),
            run.optimization.tables.tabulate(name="Table 2"),
        )

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

    def test_table_docs(self, test_mp, request):
        test_mp: Platform = request.getfixturevalue(test_mp)  # type: ignore
        run = test_mp.runs.create("Model", "Scenario")
        (indexset,) = create_indexsets_for_run(
            platform=test_mp, run_id=run.id, amount=1
        )
        table_1 = run.optimization.tables.create(
            "Table 1", constrained_to_indexsets=[indexset.name]
        )
        docs = "Documentation of Table 1"
        table_1.docs = docs
        assert table_1.docs == docs

        table_1.docs = None
        assert table_1.docs is None
