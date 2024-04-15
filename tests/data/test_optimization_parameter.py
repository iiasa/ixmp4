import pandas as pd
import pytest

from ixmp4 import Parameter, Platform

from ..utils import database_platforms


def df_from_list(parameters: list):
    return pd.DataFrame(
        [
            [
                parameter.name,
                parameter.data,
                parameter.run__id,
                parameter.created_at,
                parameter.created_by,
                parameter.id,
            ]
            for parameter in parameters
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


@database_platforms
class TestDataOptimizationParameter:
    def test_create_parameter(self, test_mp, request):
        test_mp: Platform = request.getfixturevalue(test_mp)  # type: ignore
        run = test_mp.backend.runs.create("Model", "Scenario")

        # Test normal creation
        indexset_1 = test_mp.backend.optimization.indexsets.create(
            run_id=run.id, name="Indexset"
        )
        parameter = test_mp.backend.optimization.parameters.create(
            run_id=run.id,
            name="Parameter",
            constrained_to_indexsets=["Indexset"],
        )

        assert parameter.run__id == run.id
        assert parameter.name == "Parameter"
        assert parameter.data == {}  # JsonDict type currently requires a dict, not None
        assert parameter.values == []
        assert parameter.units == []
        assert parameter.columns[0].name == "Indexset"
        assert parameter.columns[0].constrained_to_indexset == indexset_1.id

        # Test duplicate name raises
        with pytest.raises(Parameter.NotUnique):
            _ = test_mp.backend.optimization.parameters.create(
                run_id=run.id, name="Parameter", constrained_to_indexsets=["Indexset"]
            )

        # Test mismatch in constrained_to_indexsets and column_names raises
        with pytest.raises(ValueError, match="not equal in length"):
            _ = test_mp.backend.optimization.parameters.create(
                run_id=run.id,
                name="Parameter 2",
                constrained_to_indexsets=["Indexset"],
                column_names=["Dimension 1", "Dimension 2"],
            )

        # Test columns_names are used for names if given
        parameter_2 = test_mp.backend.optimization.parameters.create(
            run_id=run.id,
            name="Parameter 2",
            constrained_to_indexsets=[indexset_1.name],
            column_names=["Column 1"],
        )
        assert parameter_2.columns[0].name == "Column 1"

        # Test duplicate column_names raise
        with pytest.raises(ValueError, match="`column_names` are not unique"):
            _ = test_mp.backend.optimization.parameters.create(
                run_id=run.id,
                name="Parameter 3",
                constrained_to_indexsets=[indexset_1.name, indexset_1.name],
                column_names=["Column 1", "Column 1"],
            )

        # Test column.dtype is registered correctly
        indexset_2 = test_mp.backend.optimization.indexsets.create(
            run_id=run.id, name="Indexset 2"
        )
        test_mp.backend.optimization.indexsets.add_elements(
            indexset_2.id, elements=2024
        )
        indexset_2 = test_mp.backend.optimization.indexsets.get(run.id, indexset_2.name)
        parameter_3 = test_mp.backend.optimization.parameters.create(
            run_id=run.id,
            name="Parameter 5",
            constrained_to_indexsets=["Indexset", indexset_2.name],
        )
        # If indexset doesn't have elements, a generic dtype is registered
        assert parameter_3.columns[0].dtype == "object"
        assert parameter_3.columns[1].dtype == "int64"

    def test_get_parameter(self, test_mp, request):
        test_mp: Platform = request.getfixturevalue(test_mp)  # type: ignore
        run = test_mp.backend.runs.create("Model", "Scenario")
        _ = test_mp.backend.optimization.indexsets.create(
            run_id=run.id, name="Indexset"
        )
        parameter = test_mp.backend.optimization.parameters.create(
            run_id=run.id, name="Parameter", constrained_to_indexsets=["Indexset"]
        )
        assert parameter == test_mp.backend.optimization.parameters.get(
            run_id=run.id, name="Parameter"
        )

        with pytest.raises(Parameter.NotFound):
            _ = test_mp.backend.optimization.parameters.get(
                run_id=run.id, name="Parameter 2"
            )

    # def test_parameter_add_data(self, test_mp, request):
    #     test_mp: Platform = request.getfixturevalue(test_mp)  # type: ignore
    #     run = test_mp.backend.runs.create("Model", "Scenario")
    #     unit = test_mp.backend.units.create("Unit")
    #     indexset_1 = test_mp.backend.optimization.indexsets.create(
    #         run_id=run.id, name="Indexset"
    #     )
    #     test_mp.backend.optimization.indexsets.add_elements(
    #         indexset_id=indexset_1.id, elements=["foo", "bar", ""]
    #     )
    #     indexset_2 = test_mp.backend.optimization.indexsets.create(
    #         run_id=run.id, name="Indexset 2"
    #     )
    #     test_mp.backend.optimization.indexsets.add_elements(
    #         indexset_id=indexset_2.id, elements=[1, 2, 3]
    #     )
    #     # pandas can only convert dicts to dataframes if the values are lists
    #     # or if index is given. But maybe using read_json instead of from_dict
    #     # can remedy this. Or maybe we want to catch the resulting
    #     # "ValueError: If using all scalar values, you must pass an index" and
    #     # reraise a custom informative error?
    #     test_data_1 = {"Indexset": ["foo"], "Indexset 2": [1]}
    #     parameter = test_mp.backend.optimization.parameters.create(
    #         run_id=run.id,
    #         name="Parameter",
    #         constrained_to_indexsets=[indexset_1.name, indexset_2.name],
    #     )
    #     test_mp.backend.optimization.parameters.add_data(
    #         parameter_id=parameter.id, data=test_data_1
    #     )

    #     parameter = test_mp.backend.optimization.parameters.get(
    #         run_id=run.id, name="Parameter"
    #     )
    #     assert parameter.data == test_data_1

    #     parameter_2 = test_mp.backend.optimization.parameters.create(
    #         run_id=run.id,
    #         name="Parameter 2",
    #         constrained_to_indexsets=[indexset_1.name, indexset_2.name],
    #     )

    #     with pytest.raises(ValueError, match="missing values"):
    #         test_mp.backend.optimization.parameters.add_data(
    #             parameter_id=parameter_2.id,
    #             data=pd.DataFrame({"Indexset": [None], "Indexset 2": [2]}),
    #             # empty string is allowed for now (see below), but None or NaN raise
    #         )

    #     with pytest.raises(ValueError, match="contains duplicate rows"):
    #         test_mp.backend.optimization.parameters.add_data(
    #             parameter_id=parameter_2.id,
    #             data={"Indexset": ["foo", "foo"], "Indexset 2": [2, 2]},
    #         )

    #     # Test raising on unrecognised data.values()
    #     with pytest.raises(ValueError, match="contains values that are not allowed"):
    #         test_mp.backend.optimization.parameters.add_data(
    #             parameter_id=parameter_2.id,
    #             data={"Indexset": ["foo"], "Indexset 2": [0]},
    #         )

    #     test_data_2 = {"Indexset": [""], "Indexset 2": [3]}
    #     test_mp.backend.optimization.parameters.add_data(
    #         parameter_id=parameter_2.id, data=test_data_2
    #     )
    #     parameter_2 = test_mp.backend.optimization.parameters.get(
    #         run_id=run.id, name="Parameter 2"
    #     )
    #     assert parameter_2.data == test_data_2

    #     parameter_3 = test_mp.backend.optimization.parameters.create(
    #         run_id=run.id,
    #         name="Parameter 3",
    #         constrained_to_indexsets=[indexset_1.name, indexset_2.name],
    #         column_names=["Column 1", "Column 2"],
    #     )
    #     with pytest.raises(ValueError, match="Data is missing for some Columns!"):
    #         test_mp.backend.optimization.parameters.add_data(
    #             parameter_id=parameter_3.id, data={"Column 1": ["bar"]}
    #         )

    #     test_data_3 = {"Column 1": ["bar"], "Column 2": [2]}
    #     test_mp.backend.optimization.parameters.add_data(
    #         parameter_id=parameter_3.id, data=test_data_3
    #     )
    #     parameter_3 = test_mp.backend.optimization.parameters.get(
    #         run_id=run.id, name="Parameter 3"
    #     )
    #     assert parameter_3.data == test_data_3

    #     # Test data is expanded when Column.name is already present
    #     test_mp.backend.optimization.parameters.add_data(
    #         parameter_id=parameter_3.id,
    #         data=pd.DataFrame({"Column 1": ["foo"], "Column 2": [3]}),
    #     )
    #     parameter_3 = test_mp.backend.optimization.parameters.get(
    #         run_id=run.id, name="Parameter 3"
    #     )
    #     assert parameter_3.data == {"Column 1": ["bar", "foo"], "Column 2": [2, 3]}

    #     # Test raising on non-existing Column.name
    #     with pytest.raises(ValueError, match="Trying to add data to unknown Columns!"):
    #         test_mp.backend.optimization.parameters.add_data(
    #             parameter_id=parameter_3.id, data={"Column 3": [1]}
    #         )

    #     # Test that order is not important...
    #     parameter_4 = test_mp.backend.optimization.parameters.create(
    #         run_id=run.id,
    #         name="Parameter 4",
    #         constrained_to_indexsets=[indexset_1.name, indexset_2.name],
    #         column_names=["Column 1", "Column 2"],
    #     )
    #     test_data_4 = {"Column 2": [2], "Column 1": ["bar"]}
    #     test_mp.backend.optimization.parameters.add_data(
    #         parameter_id=parameter_4.id, data=test_data_4
    #     )
    #     parameter_4 = test_mp.backend.optimization.parameters.get(
    #         run_id=run.id, name="Parameter 4"
    #     )
    #     assert parameter_4.data == test_data_4

    #     # ...even for expanding
    #     test_mp.backend.optimization.parameters.add_data(
    #         parameter_id=parameter_4.id, data={"Column 1": ["foo"], "Column 2": [1]}
    #     )
    #     parameter_4 = test_mp.backend.optimization.parameters.get(
    #         run_id=run.id, name="Parameter 4"
    #     )
    #     assert parameter_4.data == {"Column 2": [2, 1], "Column 1": ["bar", "foo"]}

    #     # This doesn't seem to test a distinct case compared to the above
    #     with pytest.raises(ValueError, match="Trying to add data to unknown Columns!"):
    #         test_mp.backend.optimization.parameters.add_data(
    #             parameter_id=parameter_4.id,
    #             data={"Column 1": ["bar"], "Column 2": [3], "Indexset": ["foo"]},
    #         )

    #     # Test various data types
    #     test_data_5 = {"Indexset": ["foo", "foo", "bar"], "Indexset 3": [1, "2", 3.14]}
    #     indexset_3 = test_mp.backend.optimization.indexsets.create(
    #         run_id=run.id, name="Indexset 3"
    #     )
    #     test_mp.backend.optimization.indexsets.add_elements(
    #         indexset_id=indexset_3.id, elements=[1, "2", 3.14]
    #     )
    #     parameter_5 = test_mp.backend.optimization.parameters.create(
    #         run_id=run.id,
    #         name="Parameter 5",
    #         constrained_to_indexsets=[indexset_1.name, indexset_3.name],
    #     )
    #     test_mp.backend.optimization.parameters.add_data(
    #         parameter_id=parameter_5.id, data=test_data_5
    #     )
    #     parameter_5 = test_mp.backend.optimization.parameters.get(
    #         run_id=run.id, name="Parameter 5"
    #     )
    #     assert parameter_5.data == test_data_5

    #     # This doesn't raise since the union of existing and new data is validated
    #     test_mp.backend.optimization.parameters.add_data(
    #         parameter_id=parameter_5.id, data={}
    #     )
    #     parameter_5 = test_mp.backend.optimization.parameters.get(
    #         run_id=run.id, name="Parameter 5"
    #     )
    #     assert parameter_5.data == test_data_5

    # def test_list_parameter(self, test_mp, request):
    #     test_mp = request.getfixturevalue(test_mp)
    #     run = test_mp.backend.runs.create("Model", "Scenario")
    #     # Per default, list() lists scalars for `default` version runs:
    #     test_mp.backend.runs.set_as_default_version(run.id)
    #     _ = test_mp.backend.optimization.indexsets.create(
    #         run_id=run.id, name="Indexset"
    #     )
    #     _ = test_mp.backend.optimization.indexsets.create(
    #         run_id=run.id, name="Indexset 2"
    #     )
    #     parameter = test_mp.backend.optimization.parameters.create(
    #         run_id=run.id, name="Parameter", constrained_to_indexsets=["Indexset"]
    #     )
    #     parameter_2 = test_mp.backend.optimization.parameters.create(
    #         run_id=run.id, name="Parameter 2", constrained_to_indexsets=["Indexset 2"]
    #     )
    #     assert [
    #         parameter,
    #         parameter_2,
    #     ] == test_mp.backend.optimization.parameters.list()

    #     assert [parameter] == test_mp.backend.optimization.parameters.list(
    #         name="Parameter"
    #     )

    # def test_tabulate_parameter(self, test_mp, request):
    #     test_mp: Platform = request.getfixturevalue(test_mp)  # type: ignore
    #     run = test_mp.backend.runs.create("Model", "Scenario")
    #     # Per default, tabulate() lists scalars for `default` version runs:
    #     test_mp.backend.runs.set_as_default_version(run.id)
    #     indexset = test_mp.backend.optimization.indexsets.create(
    #         run_id=run.id, name="Indexset"
    #     )
    #     indexset_2 = test_mp.backend.optimization.indexsets.create(
    #         run_id=run.id, name="Indexset 2"
    #     )
    #     parameter = test_mp.backend.optimization.parameters.create(
    #         run_id=run.id,
    #         name="Parameter",
    #         constrained_to_indexsets=["Indexset", "Indexset 2"],
    #     )
    #     parameter_2 = test_mp.backend.optimization.parameters.create(
    #         run_id=run.id,
    #         name="Parameter 2",
    #         constrained_to_indexsets=["Indexset", "Indexset 2"],
    #     )
    #     pd.testing.assert_frame_equal(
    #         df_from_list([parameter_2]),
    #         test_mp.backend.optimization.parameters.tabulate(name="Parameter 2"),
    #     )

    #     test_mp.backend.optimization.indexsets.add_elements(
    #         indexset_id=indexset.id, elements=["foo", "bar"]
    #     )
    #     test_mp.backend.optimization.indexsets.add_elements(
    #         indexset_id=indexset_2.id, elements=[1, 2, 3]
    #     )
    #     test_data_1 = {"Indexset": ["foo"], "Indexset 2": [1]}
    #     test_mp.backend.optimization.parameters.add_data(
    #         parameter_id=parameter.id, data=test_data_1
    #     )
    #     parameter = test_mp.backend.optimization.parameters.get(
    #         run_id=run.id, name="Parameter"
    #     )

    #     test_data_2 = {"Indexset 2": [2, 3], "Indexset": ["foo", "bar"]}
    #     test_mp.backend.optimization.parameters.add_data(
    #         parameter_id=parameter_2.id, data=test_data_2
    #     )
    #     parameter_2 = test_mp.backend.optimization.parameters.get(
    #         run_id=run.id, name="Parameter 2"
    #     )
    #     pd.testing.assert_frame_equal(
    #         df_from_list([parameter, parameter_2]),
    #         test_mp.backend.optimization.parameters.tabulate(),
    #     )
