import pandas as pd
import pytest

from ixmp4 import Parameter, Platform

from ..utils import all_platforms


def df_from_list(parameters: list):
    return pd.DataFrame(
        [
            [
                parameter.id,
                parameter.data,
                parameter.name,
                parameter.created_at,
                parameter.created_by,
                parameter.run__id,
            ]
            for parameter in parameters
        ],
        columns=[
            "id",
            "data",
            "name",
            "created_at",
            "created_by",
            "run__id",
        ],
    )


@all_platforms
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

    def test_parameter_add_data(self, test_mp, request):
        test_mp: Platform = request.getfixturevalue(test_mp)  # type: ignore
        run = test_mp.backend.runs.create("Model", "Scenario")
        unit = test_mp.backend.units.create("Unit")
        indexset_1 = test_mp.backend.optimization.indexsets.create(
            run_id=run.id, name="Indexset"
        )
        test_mp.backend.optimization.indexsets.add_elements(
            indexset_id=indexset_1.id, elements=["foo", "bar", ""]
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
        test_data_1 = {
            "Indexset": ["foo"],
            "Indexset 2": [1],
            "values": [3.14],
            "units": [unit.name],
        }
        parameter = test_mp.backend.optimization.parameters.create(
            run_id=run.id,
            name="Parameter",
            constrained_to_indexsets=[indexset_1.name, indexset_2.name],
        )
        test_mp.backend.optimization.parameters.add_data(
            parameter_id=parameter.id, data=test_data_1
        )

        parameter = test_mp.backend.optimization.parameters.get(
            run_id=run.id, name="Parameter"
        )
        assert parameter.data == test_data_1

        parameter_2 = test_mp.backend.optimization.parameters.create(
            run_id=run.id,
            name="Parameter 2",
            constrained_to_indexsets=[indexset_1.name, indexset_2.name],
        )

        with pytest.raises(
            AssertionError, match=r"must include the column\(s\): values!"
        ):
            test_mp.backend.optimization.parameters.add_data(
                parameter_id=parameter_2.id,
                data=pd.DataFrame(
                    {
                        "Indexset": [None],
                        "Indexset 2": [2],
                        "units": [unit.name],
                    }
                ),
            )

        with pytest.raises(
            AssertionError, match=r"must include the column\(s\): units!"
        ):
            test_mp.backend.optimization.parameters.add_data(
                parameter_id=parameter_2.id,
                data=pd.DataFrame(
                    {
                        "Indexset": [None],
                        "Indexset 2": [2],
                        "values": [""],
                    }
                ),
            )

        # By converting data to pd.DataFrame, we automatically enforce equal length
        # of new columns, raises All arrays must be of the same length otherwise:
        with pytest.raises(ValueError, match="All arrays must be of the same length"):
            test_mp.backend.optimization.parameters.add_data(
                parameter_id=parameter_2.id,
                data={
                    "Indexset": ["foo", "foo"],
                    "Indexset 2": [2, 2],
                    "values": [1, 2],
                    "units": [unit.name],
                },
            )

        with pytest.raises(ValueError, match="contains duplicate rows"):
            test_mp.backend.optimization.parameters.add_data(
                parameter_id=parameter_2.id,
                data={
                    "Indexset": ["foo", "foo"],
                    "Indexset 2": [2, 2],
                    "values": [1, 2],
                    "units": [unit.name, unit.name],
                },
            )

        # Test that order is conserved
        test_data_2 = {
            "Indexset": ["", "", "foo", "foo", "bar", "bar"],
            "Indexset 2": [3, 1, 2, 1, 2, 3],
            "values": [6, 5, 4, 3, 2, 1],
            "units": [unit.name] * 6,
        }
        test_mp.backend.optimization.parameters.add_data(
            parameter_id=parameter_2.id, data=test_data_2
        )
        parameter_2 = test_mp.backend.optimization.parameters.get(
            run_id=run.id, name="Parameter 2"
        )
        assert parameter_2.data == test_data_2

        # Test order is conserved with varying types and upon later addition of data
        parameter_3 = test_mp.backend.optimization.parameters.create(
            run_id=run.id,
            name="Parameter 3",
            constrained_to_indexsets=[indexset_1.name, indexset_2.name],
            column_names=["Column 1", "Column 2"],
        )
        unit_2 = test_mp.backend.units.create("Unit 2")
        unit_3 = test_mp.backend.units.create("Unit 3")

        test_data_3 = {
            "Column 1": ["bar", "foo", ""],
            "Column 2": [2, 3, 1],
            "values": ["3", 2.0, 1],
            "units": [unit_3.name, unit_2.name, unit.name],
        }
        test_mp.backend.optimization.parameters.add_data(
            parameter_id=parameter_3.id, data=test_data_3
        )
        parameter_3 = test_mp.backend.optimization.parameters.get(
            run_id=run.id, name="Parameter 3"
        )
        assert parameter_3.data == test_data_3

        test_data_4 = {
            "Column 1": ["foo", "", "bar"],
            "Column 2": [2, 3, 1],
            "values": [3.14, 2, "1"],
            "units": [unit_2.name, unit.name, unit_3.name],
        }
        test_mp.backend.optimization.parameters.add_data(
            parameter_id=parameter_3.id, data=test_data_4
        )
        parameter_3 = test_mp.backend.optimization.parameters.get(
            run_id=run.id, name="Parameter 3"
        )
        test_data_5 = test_data_3.copy()
        for key, value in test_data_4.items():
            test_data_5[key].extend(value)
        assert parameter_3.data == test_data_5

    def test_list_parameter(self, test_mp, request):
        test_mp: Platform = request.getfixturevalue(test_mp)  # type: ignore
        run = test_mp.backend.runs.create("Model", "Scenario")
        # Per default, list() lists scalars for `default` version runs:
        test_mp.backend.runs.set_as_default_version(run.id)
        _ = test_mp.backend.optimization.indexsets.create(
            run_id=run.id, name="Indexset"
        )
        _ = test_mp.backend.optimization.indexsets.create(
            run_id=run.id, name="Indexset 2"
        )
        parameter = test_mp.backend.optimization.parameters.create(
            run_id=run.id, name="Parameter", constrained_to_indexsets=["Indexset"]
        )
        parameter_2 = test_mp.backend.optimization.parameters.create(
            run_id=run.id, name="Parameter 2", constrained_to_indexsets=["Indexset 2"]
        )
        assert [
            parameter,
            parameter_2,
        ] == test_mp.backend.optimization.parameters.list()

        assert [parameter] == test_mp.backend.optimization.parameters.list(
            name="Parameter"
        )

    def test_tabulate_parameter(self, test_mp, request):
        test_mp: Platform = request.getfixturevalue(test_mp)  # type: ignore
        run = test_mp.backend.runs.create("Model", "Scenario")
        # Per default, tabulate() lists scalars for `default` version runs:
        test_mp.backend.runs.set_as_default_version(run.id)
        indexset = test_mp.backend.optimization.indexsets.create(
            run_id=run.id, name="Indexset"
        )
        indexset_2 = test_mp.backend.optimization.indexsets.create(
            run_id=run.id, name="Indexset 2"
        )
        parameter = test_mp.backend.optimization.parameters.create(
            run_id=run.id,
            name="Parameter",
            constrained_to_indexsets=["Indexset", "Indexset 2"],
        )
        parameter_2 = test_mp.backend.optimization.parameters.create(
            run_id=run.id,
            name="Parameter 2",
            constrained_to_indexsets=["Indexset", "Indexset 2"],
        )
        pd.testing.assert_frame_equal(
            df_from_list([parameter_2]),
            test_mp.backend.optimization.parameters.tabulate(name="Parameter 2"),
        )

        unit = test_mp.backend.units.create("Unit")
        unit_2 = test_mp.backend.units.create("Unit 2")
        test_mp.backend.optimization.indexsets.add_elements(
            indexset_id=indexset.id, elements=["foo", "bar"]
        )
        test_mp.backend.optimization.indexsets.add_elements(
            indexset_id=indexset_2.id, elements=[1, 2, 3]
        )
        test_data_1 = {
            "Indexset": ["foo"],
            "Indexset 2": [1],
            "values": ["value"],
            "units": [unit.name],
        }
        test_mp.backend.optimization.parameters.add_data(
            parameter_id=parameter.id, data=test_data_1
        )
        parameter = test_mp.backend.optimization.parameters.get(
            run_id=run.id, name="Parameter"
        )

        test_data_2 = {
            "Indexset 2": [2, 3],
            "Indexset": ["foo", "bar"],
            "values": [1, "value"],
            "units": [unit.name, unit_2.name],
        }
        test_mp.backend.optimization.parameters.add_data(
            parameter_id=parameter_2.id, data=test_data_2
        )
        parameter_2 = test_mp.backend.optimization.parameters.get(
            run_id=run.id, name="Parameter 2"
        )
        pd.testing.assert_frame_equal(
            df_from_list([parameter, parameter_2]),
            test_mp.backend.optimization.parameters.tabulate(),
        )