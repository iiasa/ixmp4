import pandas as pd
import pytest

from ixmp4 import Table

from ..utils import database_platforms


@database_platforms
class TestDataOptimizationTable:
    def test_create_table(self, test_mp):
        run = test_mp.backend.runs.create("Model", "Scenario")
        indexset_1 = test_mp.backend.optimization.indexsets.create(
            run_id=run.id, name="Indexset"
        )
        table = test_mp.backend.optimization.tables.create(
            run_id=run.id, name="Table", constrained_to_indexsets=["Indexset"]
        )

        # check why column.dtype is 'object'

        assert table.run__id == run.id
        assert table.name == "Table"
        assert table.data == {}  # JsonDict type currently requires a dict, not None

        assert table.columns[0].name == "Indexset"
        # TODO: confirm that we are okay with having colum.constrained_to_indexset, but
        # tables.create(constrained_to_indexsets=...) (extra s)
        assert table.columns[0].constrained_to_indexset == indexset_1.id

        with pytest.raises(Table.NotUnique):
            _ = test_mp.backend.optimization.tables.create(
                run_id=run.id, name="Table", constrained_to_indexsets=["Indexset"]
            )

        with pytest.raises(ValueError, match="not equal in length"):
            _ = test_mp.backend.optimization.tables.create(
                run_id=run.id,
                name="Table 2",
                constrained_to_indexsets=["Indexset"],
                column_names=["Dimension 1", "Dimension 2"],
            )

        # TODO: do we want this to raise an error?
        # with pytest.raises(ValueError):
        #     _ = test_mp.backend.optimization.tables.create(
        #         run_id=run.id,
        #         name="Table 2",
        #         constrained_to_indexsets=[indexset_1.name, indexset_1.name],
        #         column_names=["Column 1", "Column 2"]
        #     )

        table_2 = test_mp.backend.optimization.tables.create(
            run_id=run.id,
            name="Table 2",
            constrained_to_indexsets=[indexset_1.name],
            column_names=["Column 1"],
        )
        assert table_2.columns[0].name == "Column 1"

        with pytest.raises(ValueError, match="`column_names` are not unique"):
            _ = test_mp.backend.optimization.tables.create(
                run_id=run.id,
                name="Table 3",
                constrained_to_indexsets=[indexset_1.name, indexset_1.name],
                column_names=["Column 1", "Column 1"],
            )

    def test_table_add_data(self, test_mp):
        run = test_mp.backend.runs.create("Model", "Scenario")
        indexset_1 = test_mp.backend.optimization.indexsets.create(
            run_id=run.id, name="Indexset"
        )
        test_mp.backend.optimization.indexsets.add_elements(
            indexset_id=indexset_1.id, elements=["foo", "bar"]
        )
        indexset_2 = test_mp.backend.optimization.indexsets.create(
            run_id=run.id, name="Indexset 2"
        )
        test_mp.backend.optimization.indexsets.add_elements(
            indexset_id=indexset_2.id, elements=[1, 2, 3]
        )
        # pandas can only convert dicts to dataframes if the values are lists
        # or if index is given. But maybe using read_json instead of from_dict
        # can remedy this. Or maybe we want to catch the resulting
        # "ValueError: If using all scalar values, you must pass an index" and
        # reraise a custom informative error?
        test_data_1 = {"Indexset": ["foo"], "Indexset 2": [1]}
        table = test_mp.backend.optimization.tables.create(
            run_id=run.id,
            name="Table",
            constrained_to_indexsets=[indexset_1.name, indexset_2.name],
        )
        test_mp.backend.optimization.tables.add_data(
            table_id=table.id, data=test_data_1
        )
        assert table.data == test_data_1

        table_2 = test_mp.backend.optimization.tables.create(
            run_id=run.id,
            name="Table 2",
            constrained_to_indexsets=[indexset_1.name, indexset_2.name],
        )

        with pytest.raises(ValueError, match="missing values"):
            _ = test_mp.backend.optimization.tables.add_data(
                table_id=table_2.id,
                data=pd.DataFrame({"Indexset": [None], "Indexset 2": [2]}),
                # empty string is allowed for now, but None or NaN raise
            )

        with pytest.raises(ValueError, match="contains duplicate rows"):
            _ = test_mp.backend.optimization.tables.add_data(
                table_id=table_2.id,
                data={"Indexset": ["foo", "foo"], "Indexset 2": [2, 2]},
            )

        with pytest.raises(
            ValueError, match="contains keys and/or values that are not allowed"
        ):
            _ = test_mp.backend.optimization.tables.add_data(
                table_id=table_2.id,
                data={"Indexset": ["foo"], "Indexset 2": [0]},
            )

        table_3 = test_mp.backend.optimization.tables.create(
            run_id=run.id,
            name="Table 3",
            constrained_to_indexsets=[indexset_1.name, indexset_2.name],
            column_names=["Column 1", "Column 2"],
        )
        test_mp.backend.optimization.tables.add_data(
            table_id=table_3.id, data={"Column 1": ["bar"]}
        )
        assert table_3.data == {"Column 1": ["bar"]}

        test_mp.backend.optimization.tables.add_data(
            table_id=table_3.id,
            data=pd.DataFrame({"Column 1": ["foo"], "Column 2": [3]}),
        )
        assert table_3.data == {"Column 1": ["foo"], "Column 2": [3]}

        with pytest.raises(
            ValueError, match="contains keys and/or values that are not allowed"
        ):
            test_mp.backend.optimization.tables.add_data(
                table_id=table_3.id, data={"Column 3": [1]}
            )

        table_4 = test_mp.backend.optimization.tables.create(
            run_id=run.id,
            name="Table 4",
            constrained_to_indexsets=[indexset_1.name, indexset_2.name],
            column_names=["Column 1", "Column 2"],
        )
        test_mp.backend.optimization.tables.add_data(
            table_id=table_4.id, data={"Column 2": [2], "Column 1": ["bar"]}
        )
        assert table_4.data == {"Column 2": [2], "Column 1": ["bar"]}

        test_mp.backend.optimization.tables.add_data(
            table_id=table_4.id, data={"Column 1": ["foo"], "Column 2": [1]}
        )
        assert table_4.data == {"Column 2": [1], "Column 1": ["foo"]}

        with pytest.raises(
            ValueError, match="contains keys and/or values that are not allowed"
        ):
            test_mp.backend.optimization.tables.add_data(
                table_id=table_4.id,
                data={"Column 1": ["bar"], "Column 2": [3], "Indexset": ["foo"]},
            )

        test_data_2 = {"Indexset": ["foo", "foo", "bar"], "Indexset 3": [1, "2", 3.14]}
        indexset_3 = test_mp.backend.optimization.indexsets.create(
            run_id=run.id, name="Indexset 3"
        )
        test_mp.backend.optimization.indexsets.add_elements(
            indexset_id=indexset_3.id, elements=[1, "2", 3.14]
        )
        table_5 = test_mp.backend.optimization.tables.create(
            run_id=run.id,
            name="Table 5",
            constrained_to_indexsets=[indexset_1.name, indexset_3.name],
        )
        test_mp.backend.optimization.tables.add_data(
            table_id=table_5.id, data=test_data_2
        )
        assert table_5.data == test_data_2

        test_mp.backend.optimization.tables.add_data(table_id=table_5.id, data={})
        assert table_5.data == test_data_2

    # def test_get_scalar(self, test_mp):
    #     run = test_mp.backend.runs.create("Model", "Scenario")
    #     unit = test_mp.backend.units.create("Unit")
    #     scalar = test_mp.backend.optimization.scalars.create(
    #         run_id=run.id, name="Scalar", value=1, unit_name=unit.name
    #     )

    #     assert scalar == test_mp.backend.optimization.scalars.get(
    #         run_id=run.id, name="Scalar"
    #     )

    # def test_update_scalar(self, test_mp):
    #     run = test_mp.backend.runs.create("Model", "Scenario")
    #     unit = test_mp.backend.units.create("Unit")
    #     unit2 = test_mp.backend.units.create("Unit 2")
    #     scalar = test_mp.backend.optimization.scalars.create(
    #         run_id=run.id, name="Scalar", value=1, unit_name=unit.name
    #     )
    #     assert scalar.id == 1
    #     assert scalar.unit__id == unit.id

    #     scalar = test_mp.backend.optimization.scalars.update(
    #         "Scalar", value=20, unit_name="Unit 2", run_id=run.id
    #     )
    #     assert scalar.value == 20
    #     assert scalar.unit__id == unit2.id

    # def test_list_scalars(self, test_mp):
    #     run = test_mp.backend.runs.create("Model", "Scenario")
    #     # Per default, list() lists scalars for `default` version runs:
    #     test_mp.backend.runs.set_as_default_version(run.id)
    #     unit = test_mp.backend.units.create("Unit")
    #     unit2 = test_mp.backend.units.create("Unit 2")
    #     scalar_1 = test_mp.backend.optimization.scalars.create(
    #         run_id=run.id, name="Scalar", value=1, unit_name=unit.name
    #     )
    #     scalar_2 = test_mp.backend.optimization.scalars.create(
    #         run_id=run.id, name="Scalar 2", value=2, unit_name=unit2.name
    #     )
    #     assert [scalar_1, scalar_2] == test_mp.backend.optimization.scalars.list()
