import pandas as pd
import pytest

import ixmp4
from ixmp4.data.abstract import Parameter

from ..utils import create_indexsets_for_run


def df_from_list(parameters: list):
    return pd.DataFrame(
        [
            [
                parameter.run__id,
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


class TestDataOptimizationParameter:
    def test_create_parameter(self, platform: ixmp4.Platform):
        run = platform.backend.runs.create("Model", "Scenario")

        # Test normal creation
        indexset, indexset_2 = create_indexsets_for_run(
            platform=platform, run_id=run.id
        )
        parameter = platform.backend.optimization.parameters.create(
            run_id=run.id,
            name="Parameter",
            constrained_to_indexsets=[indexset.name],
        )

        assert parameter.run__id == run.id
        assert parameter.name == "Parameter"
        assert parameter.data == {}  # JsonDict type currently requires a dict, not None
        assert parameter.columns[0].name == indexset.name
        assert parameter.columns[0].constrained_to_indexset == indexset.id

        # Test duplicate name raises
        with pytest.raises(Parameter.NotUnique):
            _ = platform.backend.optimization.parameters.create(
                run_id=run.id,
                name="Parameter",
                constrained_to_indexsets=[indexset.name],
            )

        # Test mismatch in constrained_to_indexsets and column_names raises
        with pytest.raises(ValueError, match="not equal in length"):
            _ = platform.backend.optimization.parameters.create(
                run_id=run.id,
                name="Parameter 2",
                constrained_to_indexsets=[indexset.name],
                column_names=["Dimension 1", "Dimension 2"],
            )

        # Test columns_names are used for names if given
        parameter_2 = platform.backend.optimization.parameters.create(
            run_id=run.id,
            name="Parameter 2",
            constrained_to_indexsets=[indexset.name],
            column_names=["Column 1"],
        )
        assert parameter_2.columns[0].name == "Column 1"

        # Test duplicate column_names raise
        with pytest.raises(ValueError, match="`column_names` are not unique"):
            _ = platform.backend.optimization.parameters.create(
                run_id=run.id,
                name="Parameter 3",
                constrained_to_indexsets=[indexset.name, indexset.name],
                column_names=["Column 1", "Column 1"],
            )

        # Test column.dtype is registered correctly
        platform.backend.optimization.indexsets.add_elements(
            indexset_2.id, elements=2024
        )
        indexset_2 = platform.backend.optimization.indexsets.get(
            run.id, indexset_2.name
        )
        parameter_3 = platform.backend.optimization.parameters.create(
            run_id=run.id,
            name="Parameter 5",
            constrained_to_indexsets=[indexset.name, indexset_2.name],
        )
        # If indexset doesn't have elements, a generic dtype is registered
        assert parameter_3.columns[0].dtype == "object"
        assert parameter_3.columns[1].dtype == "int64"

    def test_get_parameter(self, platform: ixmp4.Platform):
        run = platform.backend.runs.create("Model", "Scenario")
        create_indexsets_for_run(platform=platform, run_id=run.id, amount=1)
        parameter = platform.backend.optimization.parameters.create(
            run_id=run.id, name="Parameter", constrained_to_indexsets=["Indexset 1"]
        )
        assert parameter == platform.backend.optimization.parameters.get(
            run_id=run.id, name="Parameter"
        )

        with pytest.raises(Parameter.NotFound):
            _ = platform.backend.optimization.parameters.get(
                run_id=run.id, name="Parameter 2"
            )

    def test_parameter_add_data(self, platform: ixmp4.Platform):
        run = platform.backend.runs.create("Model", "Scenario")
        unit = platform.backend.units.create("Unit")
        indexset, indexset_2 = create_indexsets_for_run(
            platform=platform, run_id=run.id
        )
        platform.backend.optimization.indexsets.add_elements(
            indexset_id=indexset.id, elements=["foo", "bar", ""]
        )
        platform.backend.optimization.indexsets.add_elements(
            indexset_id=indexset_2.id, elements=[1, 2, 3]
        )
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
        parameter = platform.backend.optimization.parameters.create(
            run_id=run.id,
            name="Parameter",
            constrained_to_indexsets=[indexset.name, indexset_2.name],
        )
        platform.backend.optimization.parameters.add_data(
            parameter_id=parameter.id, data=test_data_1
        )

        parameter = platform.backend.optimization.parameters.get(
            run_id=run.id, name="Parameter"
        )
        assert parameter.data == test_data_1

        parameter_2 = platform.backend.optimization.parameters.create(
            run_id=run.id,
            name="Parameter 2",
            constrained_to_indexsets=[indexset.name, indexset_2.name],
        )

        with pytest.raises(
            AssertionError, match=r"must include the column\(s\): values!"
        ):
            platform.backend.optimization.parameters.add_data(
                parameter_id=parameter_2.id,
                data=pd.DataFrame(
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
            platform.backend.optimization.parameters.add_data(
                parameter_id=parameter_2.id,
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
            platform.backend.optimization.parameters.add_data(
                parameter_id=parameter_2.id,
                data={
                    indexset.name: ["foo", "foo"],
                    indexset_2.name: [2, 2],
                    "values": [1, 2],
                    "units": [unit.name],
                },
            )

        with pytest.raises(ValueError, match="contains duplicate rows"):
            platform.backend.optimization.parameters.add_data(
                parameter_id=parameter_2.id,
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
        platform.backend.optimization.parameters.add_data(
            parameter_id=parameter_2.id, data=test_data_2
        )
        parameter_2 = platform.backend.optimization.parameters.get(
            run_id=run.id, name="Parameter 2"
        )
        assert parameter_2.data == test_data_2

        # Test order is conserved with varying types and upon later addition of data
        parameter_3 = platform.backend.optimization.parameters.create(
            run_id=run.id,
            name="Parameter 3",
            constrained_to_indexsets=[indexset.name, indexset_2.name],
            column_names=["Column 1", "Column 2"],
        )
        unit_2 = platform.backend.units.create("Unit 2")
        unit_3 = platform.backend.units.create("Unit 3")

        test_data_3 = {
            "Column 1": ["bar", "foo", ""],
            "Column 2": [2, 3, 1],
            "values": ["3", 2.0, 1],
            "units": [unit_3.name, unit_2.name, unit.name],
        }
        platform.backend.optimization.parameters.add_data(
            parameter_id=parameter_3.id, data=test_data_3
        )
        parameter_3 = platform.backend.optimization.parameters.get(
            run_id=run.id, name="Parameter 3"
        )
        assert parameter_3.data == test_data_3

        test_data_4 = {
            "Column 1": ["foo", "", "bar"],
            "Column 2": [2, 3, 1],
            "values": [3.14, 2, "1"],
            "units": [unit_2.name, unit.name, unit_3.name],
        }
        platform.backend.optimization.parameters.add_data(
            parameter_id=parameter_3.id, data=test_data_4
        )
        parameter_3 = platform.backend.optimization.parameters.get(
            run_id=run.id, name="Parameter 3"
        )
        test_data_5 = test_data_3.copy()
        for key, value in test_data_4.items():
            test_data_5[key].extend(value)  # type: ignore
        assert parameter_3.data == test_data_5

    def test_list_parameter(self, platform: ixmp4.Platform):
        run = platform.backend.runs.create("Model", "Scenario")
        indexset, indexset_2 = create_indexsets_for_run(
            platform=platform, run_id=run.id
        )
        parameter = platform.backend.optimization.parameters.create(
            run_id=run.id, name="Parameter", constrained_to_indexsets=[indexset.name]
        )
        parameter_2 = platform.backend.optimization.parameters.create(
            run_id=run.id,
            name="Parameter 2",
            constrained_to_indexsets=[indexset_2.name],
        )
        assert [
            parameter,
            parameter_2,
        ] == platform.backend.optimization.parameters.list()

        assert [parameter] == platform.backend.optimization.parameters.list(
            name="Parameter"
        )

        # Test listing of Parameters belonging to specific Run
        run_2 = platform.backend.runs.create("Model", "Scenario")
        (indexset,) = create_indexsets_for_run(
            platform=platform, run_id=run_2.id, amount=1
        )

        parameter_3 = platform.backend.optimization.parameters.create(
            run_id=run_2.id, name="Parameter", constrained_to_indexsets=[indexset.name]
        )
        parameter_4 = platform.backend.optimization.parameters.create(
            run_id=run_2.id,
            name="Parameter 2",
            constrained_to_indexsets=[indexset.name],
        )
        assert [
            parameter_3,
            parameter_4,
        ] == platform.backend.optimization.parameters.list(run_id=run_2.id)

    def test_tabulate_parameter(self, platform: ixmp4.Platform):
        run = platform.backend.runs.create("Model", "Scenario")
        indexset, indexset_2 = create_indexsets_for_run(
            platform=platform, run_id=run.id
        )
        parameter = platform.backend.optimization.parameters.create(
            run_id=run.id,
            name="Parameter",
            constrained_to_indexsets=[indexset.name, indexset_2.name],
        )
        parameter_2 = platform.backend.optimization.parameters.create(
            run_id=run.id,
            name="Parameter 2",
            constrained_to_indexsets=[indexset.name, indexset_2.name],
        )
        pd.testing.assert_frame_equal(
            df_from_list([parameter_2]),
            platform.backend.optimization.parameters.tabulate(name="Parameter 2"),
        )

        unit = platform.backend.units.create("Unit")
        unit_2 = platform.backend.units.create("Unit 2")
        platform.backend.optimization.indexsets.add_elements(
            indexset_id=indexset.id, elements=["foo", "bar"]
        )
        platform.backend.optimization.indexsets.add_elements(
            indexset_id=indexset_2.id, elements=[1, 2, 3]
        )
        test_data_1 = {
            indexset.name: ["foo"],
            indexset_2.name: [1],
            "values": ["value"],
            "units": [unit.name],
        }
        platform.backend.optimization.parameters.add_data(
            parameter_id=parameter.id, data=test_data_1
        )
        parameter = platform.backend.optimization.parameters.get(
            run_id=run.id, name="Parameter"
        )

        test_data_2 = {
            indexset_2.name: [2, 3],
            indexset.name: ["foo", "bar"],
            "values": [1, "value"],
            "units": [unit.name, unit_2.name],
        }
        platform.backend.optimization.parameters.add_data(
            parameter_id=parameter_2.id, data=test_data_2
        )
        parameter_2 = platform.backend.optimization.parameters.get(
            run_id=run.id, name="Parameter 2"
        )
        pd.testing.assert_frame_equal(
            df_from_list([parameter, parameter_2]),
            platform.backend.optimization.parameters.tabulate(),
        )

        # Test tabulation of Parameters belonging to specific Run
        run_2 = platform.backend.runs.create("Model", "Scenario")
        (indexset,) = create_indexsets_for_run(
            platform=platform, run_id=run_2.id, amount=1
        )
        parameter_3 = platform.backend.optimization.parameters.create(
            run_id=run_2.id, name="Parameter", constrained_to_indexsets=[indexset.name]
        )
        parameter_4 = platform.backend.optimization.parameters.create(
            run_id=run_2.id,
            name="Parameter 2",
            constrained_to_indexsets=[indexset.name],
        )
        pd.testing.assert_frame_equal(
            df_from_list([parameter_3, parameter_4]),
            platform.backend.optimization.parameters.tabulate(run_id=run_2.id),
        )
