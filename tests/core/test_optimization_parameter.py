import pandas as pd
import pytest

import ixmp4
from ixmp4.core import IndexSet, Parameter

from ..utils import assert_unordered_equality, create_indexsets_for_run


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


class TestCoreParameter:
    def test_create_parameter(self, platform: ixmp4.Platform):
        run = platform.runs.create("Model", "Scenario")

        # Test normal creation
        indexset, indexset_2 = tuple(
            IndexSet(_backend=platform.backend, _model=model)
            for model in create_indexsets_for_run(platform=platform, run_id=run.id)
        )
        parameter = run.optimization.parameters.create(
            name="Parameter",
            constrained_to_indexsets=[indexset.name],
        )

        assert parameter.run_id == run.id
        assert parameter.name == "Parameter"
        assert parameter.data == {}  # JsonDict type currently requires a dict, not None
        assert parameter.columns[0].name == indexset.name
        assert parameter.constrained_to_indexsets == [indexset.name]
        assert parameter.values == []
        assert parameter.units == []

        # Test duplicate name raises
        with pytest.raises(Parameter.NotUnique):
            _ = run.optimization.parameters.create(
                "Parameter", constrained_to_indexsets=[indexset.name]
            )

        # Test mismatch in constrained_to_indexsets and column_names raises
        with pytest.raises(ValueError, match="not equal in length"):
            _ = run.optimization.parameters.create(
                "Parameter 2",
                constrained_to_indexsets=[indexset.name],
                column_names=["Dimension 1", "Dimension 2"],
            )

        # Test columns_names are used for names if given
        parameter_2 = run.optimization.parameters.create(
            "Parameter 2",
            constrained_to_indexsets=[indexset.name],
            column_names=["Column 1"],
        )
        assert parameter_2.columns[0].name == "Column 1"

        # Test duplicate column_names raise
        with pytest.raises(ValueError, match="`column_names` are not unique"):
            _ = run.optimization.parameters.create(
                name="Parameter 3",
                constrained_to_indexsets=[indexset.name, indexset.name],
                column_names=["Column 1", "Column 1"],
            )

        # Test column.dtype is registered correctly
        indexset_2.add(elements=2024)
        parameter_3 = run.optimization.parameters.create(
            "Parameter 5",
            constrained_to_indexsets=[indexset.name, indexset_2.name],
        )
        # If indexset doesn't have elements, a generic dtype is registered
        assert parameter_3.columns[0].dtype == "object"
        assert parameter_3.columns[1].dtype == "int64"

    def test_get_parameter(self, platform: ixmp4.Platform):
        run = platform.runs.create("Model", "Scenario")
        (indexset,) = create_indexsets_for_run(
            platform=platform, run_id=run.id, amount=1
        )
        _ = run.optimization.parameters.create(
            name="Parameter", constrained_to_indexsets=[indexset.name]
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

    def test_parameter_add_data(self, platform: ixmp4.Platform):
        run = platform.runs.create("Model", "Scenario")
        unit = platform.units.create("Unit")
        indexset, indexset_2 = tuple(
            IndexSet(_backend=platform.backend, _model=model)
            for model in create_indexsets_for_run(platform=platform, run_id=run.id)
        )
        indexset.add(elements=["foo", "bar", ""])
        indexset_2.add(elements=[1, 2, 3])
        # pandas can only convert dicts to dataframes if the values are lists
        # or if index is given. But maybe using read_json instead of from_dict
        # can remedy this. Or maybe we want to catch the resulting
        # "ValueError: If using all scalar values, you must pass an index" and
        # reraise a custom informative error?
        test_data_1 = {
            indexset.name: ["foo"],
            indexset_2.name: [1],
            "values": [3.14],
            "units": [unit.name],
        }
        parameter = run.optimization.parameters.create(
            "Parameter",
            constrained_to_indexsets=[indexset.name, indexset_2.name],
        )
        parameter.add(data=test_data_1)
        assert parameter.data == test_data_1
        assert parameter.values == test_data_1["values"]
        assert parameter.units == test_data_1["units"]

        parameter_2 = run.optimization.parameters.create(
            name="Parameter 2",
            constrained_to_indexsets=[indexset.name, indexset_2.name],
        )

        with pytest.raises(
            AssertionError, match=r"must include the column\(s\): values!"
        ):
            parameter_2.add(
                pd.DataFrame(
                    {
                        indexset.name: [None],
                        indexset_2.name: [2],
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
                        indexset.name: [None],
                        indexset_2.name: [2],
                        "values": [""],
                    }
                ),
            )

        # By converting data to pd.DataFrame, we automatically enforce equal length
        # of new columns, raises All arrays must be of the same length otherwise:
        with pytest.raises(ValueError, match="All arrays must be of the same length"):
            parameter_2.add(
                data={
                    indexset.name: ["foo", "foo"],
                    indexset_2.name: [2, 2],
                    "values": [1, 2],
                    "units": [unit.name],
                },
            )

        with pytest.raises(ValueError, match="contains duplicate rows"):
            parameter_2.add(
                data={
                    indexset.name: ["foo", "foo"],
                    indexset_2.name: [2, 2],
                    "values": [1, 2],
                    "units": [unit.name, unit.name],
                },
            )

        # Test that order is conserved
        test_data_2 = {
            indexset.name: ["", "", "foo", "foo", "bar", "bar"],
            indexset_2.name: [3, 1, 2, 1, 2, 3],
            "values": [6, 5, 4, 3, 2, 1],
            "units": [unit.name] * 6,
        }
        parameter_2.add(test_data_2)
        assert parameter_2.data == test_data_2
        assert parameter_2.values == test_data_2["values"]
        assert parameter_2.units == test_data_2["units"]

        unit_2 = platform.units.create("Unit 2")

        # Test updating of existing keys
        parameter_4 = run.optimization.parameters.create(
            name="Parameter 4",
            constrained_to_indexsets=[indexset.name, indexset_2.name],
        )
        test_data_6 = {
            indexset.name: ["foo", "foo", "bar", "bar"],
            indexset_2.name: [1, 3, 1, 2],
            "values": [1, "2", 2.3, "4"],
            "units": [unit.name] * 4,
        }
        parameter_4.add(data=test_data_6)
        test_data_7 = {
            indexset.name: ["foo", "foo", "bar", "bar", "bar"],
            indexset_2.name: [1, 2, 3, 2, 1],
            "values": [1, 2.3, 3, 4, "5"],
            "units": [unit.name] * 2 + [unit_2.name] * 3,
        }
        parameter_4.add(data=test_data_7)
        expected = (
            pd.DataFrame(test_data_7)
            .set_index([indexset.name, indexset_2.name])
            .combine_first(
                pd.DataFrame(test_data_6).set_index([indexset.name, indexset_2.name])
            )
            .reset_index()
        )
        assert_unordered_equality(expected, pd.DataFrame(parameter_4.data))

    def test_list_parameter(self, platform: ixmp4.Platform):
        run = platform.runs.create("Model", "Scenario")
        create_indexsets_for_run(platform=platform, run_id=run.id)
        parameter = run.optimization.parameters.create(
            "Parameter", constrained_to_indexsets=["Indexset 1"]
        )
        parameter_2 = run.optimization.parameters.create(
            "Parameter 2", constrained_to_indexsets=["Indexset 2"]
        )
        # Create new run to test listing parameters for specific run
        run_2 = platform.runs.create("Model", "Scenario")
        (indexset,) = create_indexsets_for_run(
            platform=platform, run_id=run_2.id, amount=1
        )
        run_2.optimization.parameters.create(
            "Parameter", constrained_to_indexsets=[indexset.name]
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

    def test_tabulate_parameter(self, platform: ixmp4.Platform):
        run = platform.runs.create("Model", "Scenario")
        indexset, indexset_2 = tuple(
            IndexSet(_backend=platform.backend, _model=model)
            for model in create_indexsets_for_run(platform=platform, run_id=run.id)
        )
        parameter = run.optimization.parameters.create(
            name="Parameter",
            constrained_to_indexsets=[indexset.name, indexset_2.name],
        )
        parameter_2 = run.optimization.parameters.create(
            name="Parameter 2",
            constrained_to_indexsets=[indexset.name, indexset_2.name],
        )
        # Create new run to test listing parameters for specific run
        run_2 = platform.runs.create("Model", "Scenario")
        (indexset_3,) = create_indexsets_for_run(
            platform=platform, run_id=run_2.id, amount=1
        )
        run_2.optimization.parameters.create(
            "Parameter", constrained_to_indexsets=[indexset_3.name]
        )
        pd.testing.assert_frame_equal(
            df_from_list([parameter_2]),
            run.optimization.parameters.tabulate(name="Parameter 2"),
        )

        unit = platform.units.create("Unit")
        unit_2 = platform.units.create("Unit 2")
        indexset.add(elements=["foo", "bar"])
        indexset_2.add(elements=[1, 2, 3])
        test_data_1 = {
            indexset.name: ["foo"],
            indexset_2.name: [1],
            "values": ["value"],
            "units": [unit.name],
        }
        parameter.add(data=test_data_1)

        test_data_2 = {
            indexset_2.name: [2, 3],
            indexset.name: ["foo", "bar"],
            "values": [1, "value"],
            "units": [unit.name, unit_2.name],
        }
        parameter_2.add(data=test_data_2)
        pd.testing.assert_frame_equal(
            df_from_list([parameter, parameter_2]),
            run.optimization.parameters.tabulate(),
        )

    def test_parameter_docs(self, platform: ixmp4.Platform):
        run = platform.runs.create("Model", "Scenario")
        (indexset,) = create_indexsets_for_run(
            platform=platform, run_id=run.id, amount=1
        )
        parameter_1 = run.optimization.parameters.create(
            "Parameter 1", constrained_to_indexsets=[indexset.name]
        )
        docs = "Documentation of Parameter 1"
        parameter_1.docs = docs
        assert parameter_1.docs == docs

        parameter_1.docs = None
        assert parameter_1.docs is None
