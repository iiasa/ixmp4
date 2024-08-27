import pandas as pd
import pytest

from ixmp4 import Parameter, Platform

from ..utils import all_platforms, assert_unordered_equality


def df_from_list(parameters: list):
    return pd.DataFrame(
        [
            [
                parameter.run_id,
                parameter.data,
                parameter.name,
                parameter.id,
                parameter.created_at,
                parameter.created_by,
            ]
            for parameter in parameters
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
class TestCoreParameter:
    def test_create_parameter(self, test_mp, request):
        test_mp: Platform = request.getfixturevalue(test_mp)  # type: ignore
        run = test_mp.runs.create("Model", "Scenario")

        # Test normal creation
        indexset_1 = run.optimization.indexsets.create("Indexset")
        parameter = run.optimization.parameters.create(
            name="Parameter",
            constrained_to_indexsets=["Indexset"],
        )

        assert parameter.run_id == run.id
        assert parameter.name == "Parameter"
        assert parameter.data == {}  # JsonDict type currently requires a dict, not None
        assert parameter.columns[0].name == "Indexset"
        assert parameter.constrained_to_indexsets == [indexset_1.name]
        assert parameter.values == []
        assert parameter.units == []

        # Test duplicate name raises
        with pytest.raises(Parameter.NotUnique):
            _ = run.optimization.parameters.create(
                "Parameter", constrained_to_indexsets=["Indexset"]
            )

        # Test mismatch in constrained_to_indexsets and column_names raises
        with pytest.raises(ValueError, match="not equal in length"):
            _ = run.optimization.parameters.create(
                "Parameter 2",
                constrained_to_indexsets=["Indexset"],
                column_names=["Dimension 1", "Dimension 2"],
            )

        # Test columns_names are used for names if given
        parameter_2 = run.optimization.parameters.create(
            "Parameter 2",
            constrained_to_indexsets=[indexset_1.name],
            column_names=["Column 1"],
        )
        assert parameter_2.columns[0].name == "Column 1"

        # Test duplicate column_names raise
        with pytest.raises(ValueError, match="`column_names` are not unique"):
            _ = run.optimization.parameters.create(
                name="Parameter 3",
                constrained_to_indexsets=[indexset_1.name, indexset_1.name],
                column_names=["Column 1", "Column 1"],
            )

        # Test column.dtype is registered correctly
        indexset_2 = run.optimization.indexsets.create("Indexset 2")
        indexset_2.add(elements=2024)
        parameter_3 = run.optimization.parameters.create(
            "Parameter 5",
            constrained_to_indexsets=["Indexset", indexset_2.name],
        )
        # If indexset doesn't have elements, a generic dtype is registered
        assert parameter_3.columns[0].dtype == "object"
        assert parameter_3.columns[1].dtype == "int64"

    def test_get_parameter(self, test_mp, request):
        test_mp: Platform = request.getfixturevalue(test_mp)  # type: ignore
        run = test_mp.runs.create("Model", "Scenario")
        indexset = run.optimization.indexsets.create("Indexset")
        _ = run.optimization.parameters.create(
            name="Parameter", constrained_to_indexsets=["Indexset"]
        )
        parameter = run.optimization.parameters.get(name="Parameter")
        assert parameter.run_id == run.id
        assert parameter.id == 1
        assert parameter.name == "Parameter"
        assert parameter.data == {}
        assert parameter.values == []
        assert parameter.units == []
        assert parameter.columns[0].name == indexset.name
        assert parameter.constrained_to_indexsets == [indexset.name]

        with pytest.raises(Parameter.NotFound):
            _ = run.optimization.parameters.get("Parameter 2")

    def test_parameter_add_data(self, test_mp, request):
        test_mp: Platform = request.getfixturevalue(test_mp)  # type: ignore
        run = test_mp.runs.create("Model", "Scenario")
        unit = test_mp.units.create("Unit")
        indexset_1 = run.optimization.indexsets.create("Indexset")
        indexset_1.add(elements=["foo", "bar", ""])
        indexset_2 = run.optimization.indexsets.create("Indexset 2")
        indexset_2.add(elements=[1, 2, 3])
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
        parameter = run.optimization.parameters.create(
            "Parameter",
            constrained_to_indexsets=[indexset_1.name, indexset_2.name],
        )
        parameter.add(data=test_data_1)
        assert parameter.data == test_data_1
        assert parameter.values == test_data_1["values"]
        assert parameter.units == test_data_1["units"]

        parameter_2 = run.optimization.parameters.create(
            name="Parameter 2",
            constrained_to_indexsets=[indexset_1.name, indexset_2.name],
        )

        with pytest.raises(
            AssertionError, match=r"must include the column\(s\): values!"
        ):
            parameter_2.add(
                pd.DataFrame(
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
            parameter_2.add(
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
            parameter_2.add(
                data={
                    "Indexset": ["foo", "foo"],
                    "Indexset 2": [2, 2],
                    "values": [1, 2],
                    "units": [unit.name],
                },
            )

        with pytest.raises(ValueError, match="contains duplicate rows"):
            parameter_2.add(
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
        parameter_2.add(test_data_2)
        assert parameter_2.data == test_data_2
        assert parameter_2.values == test_data_2["values"]
        assert parameter_2.units == test_data_2["units"]

        unit_2 = test_mp.backend.units.create("Unit 2")

        # Test updating of existing keys
        parameter_4 = test_mp.backend.optimization.parameters.create(
            run_id=run.id,
            name="Parameter 4",
            constrained_to_indexsets=[indexset_1.name, indexset_2.name],
        )
        test_data_6 = {
            indexset_1.name: ["foo", "foo", "bar", "bar"],
            indexset_2.name: [1, 3, 1, 2],
            "values": [1, "2", 2.3, "4"],
            "units": [unit.name] * 4,
        }
        test_mp.backend.optimization.parameters.add_data(
            parameter_id=parameter_4.id, data=test_data_6
        )
        test_data_7 = {
            indexset_1.name: ["foo", "foo", "bar", "bar", "bar"],
            indexset_2.name: [1, 2, 3, 2, 1],
            "values": [1, 2.3, 3, 4, "5"],
            "units": [unit.name] * 2 + [unit_2.name] * 3,
        }
        test_mp.backend.optimization.parameters.add_data(
            parameter_id=parameter_4.id, data=test_data_7
        )
        parameter_4 = test_mp.backend.optimization.parameters.get(
            run_id=run.id, name="Parameter 4"
        )
        expected = (
            pd.DataFrame(test_data_7)
            .set_index([indexset_1.name, indexset_2.name])
            .combine_first(
                pd.DataFrame(test_data_6).set_index([indexset_1.name, indexset_2.name])
            )
            .reset_index()
        )
        assert_unordered_equality(expected, pd.DataFrame(parameter_4.data))

    def test_list_parameter(self, test_mp, request):
        test_mp: Platform = request.getfixturevalue(test_mp)  # type: ignore
        run = test_mp.runs.create("Model", "Scenario")
        # Per default, list() lists scalars for `default` version runs:
        run.set_as_default()
        _ = run.optimization.indexsets.create("Indexset")
        _ = run.optimization.indexsets.create("Indexset 2")
        parameter = run.optimization.parameters.create(
            "Parameter", constrained_to_indexsets=["Indexset"]
        )
        parameter_2 = run.optimization.parameters.create(
            "Parameter 2", constrained_to_indexsets=["Indexset 2"]
        )
        expected_ids = [parameter.id, parameter_2.id]
        list_ids = [parameter.id for parameter in run.optimization.parameters.list()]
        assert not (set(expected_ids) ^ set(list_ids))

        # Test retrieving just one result by providing a name
        expected_id = [parameter.id]
        list_id = [
            parameter.id
            for parameter in run.optimization.parameters.list(name="Parameter")
        ]
        assert not (set(expected_id) ^ set(list_id))

    def test_tabulate_parameter(self, test_mp, request):
        test_mp: Platform = request.getfixturevalue(test_mp)  # type: ignore
        run = test_mp.runs.create("Model", "Scenario")
        # Per default, tabulate() lists scalars for `default` version runs:
        run.set_as_default()
        indexset = run.optimization.indexsets.create("Indexset")
        indexset_2 = run.optimization.indexsets.create("Indexset 2")
        parameter = run.optimization.parameters.create(
            name="Parameter",
            constrained_to_indexsets=["Indexset", "Indexset 2"],
        )
        parameter_2 = run.optimization.parameters.create(
            name="Parameter 2",
            constrained_to_indexsets=["Indexset", "Indexset 2"],
        )
        pd.testing.assert_frame_equal(
            df_from_list([parameter_2]),
            run.optimization.parameters.tabulate(name="Parameter 2"),
        )

        unit = test_mp.units.create("Unit")
        unit_2 = test_mp.units.create("Unit 2")
        indexset.add(elements=["foo", "bar"])
        indexset_2.add(elements=[1, 2, 3])
        test_data_1 = {
            "Indexset": ["foo"],
            "Indexset 2": [1],
            "values": ["value"],
            "units": [unit.name],
        }
        parameter.add(data=test_data_1)

        test_data_2 = {
            "Indexset 2": [2, 3],
            "Indexset": ["foo", "bar"],
            "values": [1, "value"],
            "units": [unit.name, unit_2.name],
        }
        parameter_2.add(data=test_data_2)
        pd.testing.assert_frame_equal(
            df_from_list([parameter, parameter_2]),
            run.optimization.parameters.tabulate(),
        )

    def test_parameter_docs(self, test_mp, request):
        test_mp: Platform = request.getfixturevalue(test_mp)  # type: ignore
        run = test_mp.runs.create("Model", "Scenario")
        indexset = run.optimization.indexsets.create("Indexset")
        parameter_1 = run.optimization.parameters.create(
            "Parameter 1", constrained_to_indexsets=[indexset.name]
        )
        docs = "Documentation of Parameter 1"
        parameter_1.docs = docs
        assert parameter_1.docs == docs

        parameter_1.docs = None
        assert parameter_1.docs is None
