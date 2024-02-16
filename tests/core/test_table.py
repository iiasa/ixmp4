import pandas as pd
import pytest

from ixmp4 import Platform, Table

from ..utils import all_platforms


def df_from_list(tables: list[Table]):
    return pd.DataFrame(
        # Order is important here to avoid utils.assert_unordered_equality,
        # which doesn't like lists
        [
            [
                table.name,
                table.data,
                table.run_id,
                table.created_at,
                table.created_by,
                table.id,
            ]
            for table in tables
        ],
        columns=[
            "name",
            "data",
            "run__id",
            "created_at",
            "created_by",
            "id",
        ],
    )


@all_platforms
class TestCoreTable:
    def test_create_table(self, test_mp, request):
        test_mp: Platform = request.getfixturevalue(test_mp)  # type: ignore
        run = test_mp.runs.create("Model", "Scenario")
        indexset = run.optimization.indexsets.create("Indexset")
        table = run.optimization.tables.create(
            "Table 1",
            constrained_to_indexsets=[indexset.name],
        )
        assert table.run_id == run.id
        assert table.id == 1
        assert table.name == "Table 1"
        assert table.data == {}
        assert table.columns[0].name == "Indexset"
        assert table.constrained_to_indexsets == [indexset.id]

        with pytest.raises(Table.NotUnique):
            _ = run.optimization.tables.create(
                "Table 1", constrained_to_indexsets=["Indexset"]
            )

        with pytest.raises(ValueError, match="not equal in length"):
            _ = run.optimization.tables.create(
                name="Table 2",
                constrained_to_indexsets=["Indexset"],
                column_names=["Dimension 1", "Dimension 2"],
            )

        # TODO: do we want this to raise an error?
        # with pytest.raises(ValueError):
        #     _ = run.optimization.tables.create(
        #         name="Table 2",
        #         constrained_to_indexsets=[indexset_1.name, indexset_1.name],
        #         column_names=["Column 1", "Column 2"]
        #     )

        table_2 = run.optimization.tables.create(
            name="Table 2",
            constrained_to_indexsets=[indexset.name],
            column_names=["Column 1"],
        )
        assert table_2.columns[0].name == "Column 1"

        with pytest.raises(ValueError, match="`column_names` are not unique"):
            _ = run.optimization.tables.create(
                name="Table 3",
                constrained_to_indexsets=[indexset.name, indexset.name],
                column_names=["Column 1", "Column 1"],
            )

    def test_get_table(self, test_mp, request):
        test_mp: Platform = request.getfixturevalue(test_mp)  # type: ignore
        run = test_mp.runs.create("Model", "Scenario")
        indexset = run.optimization.indexsets.create(name="Indexset")
        _ = run.optimization.tables.create(
            name="Table", constrained_to_indexsets=["Indexset"]
        )
        table = run.optimization.tables.get("Table")
        assert table.run_id == run.id
        assert table.id == 1
        assert table.name == "Table"
        assert table.data == {}
        assert table.columns[0].name == indexset.name
        assert table.constrained_to_indexsets == [indexset.id]

        with pytest.raises(Table.NotFound):
            _ = run.optimization.tables.get(name="Table 2")

    def test_table_add_data(self, test_mp, request):
        test_mp: Platform = request.getfixturevalue(test_mp)  # type: ignore
        run = test_mp.runs.create("Model", "Scenario")
        indexset = run.optimization.indexsets.create("Indexset")
        indexset.add(elements=["foo", "bar"])
        indexset_2 = run.optimization.indexsets.create("Indexset 2")
        indexset_2.add([1, 2, 3])
        # pandas can only convert dicts to dataframes if the values are lists
        # or if index is given. But maybe using read_json instead of from_dict
        # can remedy this. Or maybe we want to catch the resulting
        # "ValueError: If using all scalar values, you must pass an index" and
        # reraise a custom informative error?
        test_data_1 = {"Indexset": ["foo"], "Indexset 2": [1]}
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
            _ = table_2.add(
                pd.DataFrame({"Indexset": [None], "Indexset 2": [2]}),
                # empty string is allowed for now, but None or NaN raise
            )

        with pytest.raises(ValueError, match="contains duplicate rows"):
            _ = table_2.add(
                data={"Indexset": ["foo", "foo"], "Indexset 2": [2, 2]},
            )

        with pytest.raises(
            ValueError, match="contains keys and/or values that are not allowed"
        ):
            _ = table_2.add(
                data={"Indexset": ["foo"], "Indexset 2": [0]},
            )

        table_3 = run.optimization.tables.create(
            name="Table 3",
            constrained_to_indexsets=[indexset.name, indexset_2.name],
            column_names=["Column 1", "Column 2"],
        )
        table_3.add(data={"Column 1": ["bar"]})
        assert table_3.data == {"Column 1": ["bar"]}

        # TODO we should add a way of seeing which columns already have data if we want
        # to keep this kind of one-by-one filling; otherwise add check that all data
        # must be present at once
        table_3.add(data={"Column 2": [2]})
        assert table_3.data == {"Column 1": ["bar"], "Column 2": [2]}

        table_3.add(
            data=pd.DataFrame({"Column 1": ["foo"], "Column 2": [3]}),
        )
        assert table_3.data == {"Column 1": ["foo"], "Column 2": [3]}

        with pytest.raises(
            ValueError, match="contains keys and/or values that are not allowed"
        ):
            table_3.add({"Column 3": [1]})

        table_4 = run.optimization.tables.create(
            name="Table 4",
            constrained_to_indexsets=[indexset.name, indexset_2.name],
            column_names=["Column 1", "Column 2"],
        )
        table_4.add(data={"Column 2": [2], "Column 1": ["bar"]})
        assert table_4.data == {"Column 2": [2], "Column 1": ["bar"]}

        table_4.add(data={"Column 1": ["foo"], "Column 2": [1]})
        assert table_4.data == {"Column 2": [1], "Column 1": ["foo"]}

        with pytest.raises(
            ValueError, match="contains keys and/or values that are not allowed"
        ):
            table_4.add(
                data={"Column 1": ["bar"], "Column 2": [3], "Indexset": ["foo"]},
            )

        test_data_2 = {"Indexset": ["foo", "foo", "bar"], "Indexset 3": [1, "2", 3.14]}
        indexset_3 = run.optimization.indexsets.create(name="Indexset 3")
        indexset_3.add(elements=[1, "2", 3.14])
        table_5 = run.optimization.tables.create(
            name="Table 5",
            constrained_to_indexsets=[indexset.name, indexset_3.name],
        )
        table_5.add(test_data_2)
        assert table_5.data == test_data_2

        table_5.add(data={})
        assert table_5.data == test_data_2

    def test_list_tables(self, test_mp, request):
        test_mp: Platform = request.getfixturevalue(test_mp)  # type: ignore
        run = test_mp.runs.create("Model", "Scenario")
        # Per default, list() lists scalars for `default` version runs:
        run.set_as_default()
        _ = run.optimization.indexsets.create("Indexset")
        _ = run.optimization.indexsets.create("Indexset 2")
        table = run.optimization.tables.create(
            "Table", constrained_to_indexsets=["Indexset"]
        )
        table_2 = run.optimization.tables.create(
            "Table 2", constrained_to_indexsets=["Indexset 2"]
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
        # Per default, tabulate() lists scalars for `default` version runs:
        run.set_as_default()
        indexset = run.optimization.indexsets.create("Indexset")
        indexset_2 = run.optimization.indexsets.create("Indexset 2")
        table = run.optimization.tables.create(
            name="Table",
            constrained_to_indexsets=["Indexset", "Indexset 2"],
        )
        table_2 = run.optimization.tables.create(
            name="Table 2",
            constrained_to_indexsets=["Indexset", "Indexset 2"],
        )
        pd.testing.assert_frame_equal(
            df_from_list([table_2]),
            run.optimization.tables.tabulate(name="Table 2"),
        )

        indexset.add(["foo", "bar"])
        indexset_2.add([1, 2, 3])
        test_data_1 = {"Indexset": ["foo"], "Indexset 2": [1]}
        table.add(test_data_1)
        test_data_2 = {"Indexset 2": [2, 3], "Indexset": ["foo", "bar"]}
        table_2.add(test_data_2)
        pd.testing.assert_frame_equal(
            df_from_list([table, table_2]),
            run.optimization.tables.tabulate(),
        )

    def test_table_docs(self, test_mp, request):
        test_mp: Platform = request.getfixturevalue(test_mp)  # type: ignore
        run = test_mp.runs.create("Model", "Scenario")
        indexset = run.optimization.indexsets.create("Indexset")
        table_1 = run.optimization.tables.create(
            "Table 1", constrained_to_indexsets=[indexset.name]
        )
        docs = "Documentation of Table 1"
        table_1.docs = docs
        assert table_1.docs == docs

        table_1.docs = None
        assert table_1.docs is None
