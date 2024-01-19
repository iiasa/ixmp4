import pytest

# import pandas as pd
from ixmp4 import Table

from ..utils import database_platforms


@database_platforms
class TestDataOptimizationTable:
    def test_create_table(self, test_mp):
        run = test_mp.backend.runs.create("Model", "Scenario")
        table = test_mp.backend.optimization.tables.create(run_id=run.id, name="Table")

        assert table.run__id == run.id
        assert table.name == "Table"
        assert table.data == {}  # JsonDict type currently requires a dict, not None
        assert table.constrained_to_indexsets is None

        with pytest.raises(Table.NotUnique):
            _ = test_mp.backend.optimization.scalars.create(
                run_id=run.id,
                name="Table",
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
        test_data_1 = {"Indexset": "foo", "Indexset 2": 1}
        test_data_2 = {"First Indexset": ["foo", "bar"], "Second Indexset": [1, 3]}
        # IDEA: if constrained_to_indexsets is given, use that to constrain data.
        # Otherwise, use data.keys().
        table = test_mp.backend.optimization.tables.create(run_id=run.id, name="Table")
        test_mp.backend.optimization.tables.add_data(
            table_id=table.id, data=test_data_1
        )
        assert table.data == test_data_1
        assert table.constrained_to_indexsets is None

        table_2 = test_mp.backend.optimization.tables.create(
            run_id=run.id, name="Table 2"
        )
        test_mp.backend.optimization.tables.add_data(
            table_id=table_2.id,
            data=test_data_2,
            constrained_to_indexsets=["Indexset", "Indexset 2"],
        )
        assert table_2.data == test_data_2
        assert table_2.constrained_to_indexsets == ["Indexset", "Indexset 2"]
        # Should this be part of create() (to allow specifying constraining indexsets
        # already)?

        # TODO: catch errors:
        # when neither data.keys() nor constrained_to_indexsets are valid indexsets
        # when data is added that's not valid given the indexset constraints
        # when there are not exactly the same number of data.keys() and constraints
        # when the same indexset is used twice?
        # when the same column name is used twice?
        # assert that column data can consist of different types

    #     # Really, though?
    #     with pytest.raises(Table.NotUnique):
    #         _ = test_mp.backend.optimization.scalars.create(
    #             run_id=run.id, name="Indexset", data=test_data_2
    #         )

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
