import pandas as pd
import pytest

import ixmp4
from ixmp4.core.exceptions import (
    OptimizationDataValidationError,
    OptimizationItemUsageError,
)
from ixmp4.data.abstract import Parameter

from ..utils import assert_unordered_equality, create_indexsets_for_run


def df_from_list(parameters: list[Parameter]) -> pd.DataFrame:
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
    def test_create_parameter(self, platform: ixmp4.Platform) -> None:
        run = platform.backend.runs.create("Model", "Scenario")

        # Test normal creation
        indexset_1, indexset_2 = create_indexsets_for_run(
            platform=platform, run_id=run.id
        )
        parameter = platform.backend.optimization.parameters.create(
            run_id=run.id,
            name="Parameter",
            constrained_to_indexsets=[indexset_1.name],
        )

        assert parameter.run__id == run.id
        assert parameter.name == "Parameter"
        assert parameter.data == {}  # JsonDict type currently requires a dict, not None
        assert parameter.column_names is None
        assert parameter.indexset_names == [indexset_1.name]

        # Test duplicate name raises
        with pytest.raises(Parameter.NotUnique):
            _ = platform.backend.optimization.parameters.create(
                run_id=run.id,
                name="Parameter",
                constrained_to_indexsets=[indexset_1.name],
            )

        # Test mismatch in constrained_to_indexsets and column_names raises
        with pytest.raises(OptimizationItemUsageError, match="not equal in length"):
            _ = platform.backend.optimization.parameters.create(
                run_id=run.id,
                name="Parameter 2",
                constrained_to_indexsets=[indexset_1.name],
                column_names=["Dimension 1", "Dimension 2"],
            )

        # Test columns_names are used for names if given
        parameter_2 = platform.backend.optimization.parameters.create(
            run_id=run.id,
            name="Parameter 2",
            constrained_to_indexsets=[indexset_1.name],
            column_names=["Column 1"],
        )
        assert parameter_2.column_names == ["Column 1"]

        # Test duplicate column_names raise
        with pytest.raises(
            OptimizationItemUsageError, match="`column_names` are not unique"
        ):
            _ = platform.backend.optimization.parameters.create(
                run_id=run.id,
                name="Parameter 3",
                constrained_to_indexsets=[indexset_1.name, indexset_1.name],
                column_names=["Column 1", "Column 1"],
            )

        # Test using different column names for same indexset
        parameter_3 = platform.backend.optimization.parameters.create(
            run_id=run.id,
            name="Parameter 3",
            constrained_to_indexsets=[indexset_1.name, indexset_1.name],
            column_names=["Column 1", "Column 2"],
        )

        assert parameter_3.column_names == ["Column 1", "Column 2"]
        assert parameter_3.indexset_names == [indexset_1.name, indexset_1.name]

    def test_delete_parameter(self, platform: ixmp4.Platform) -> None:
        run = platform.backend.runs.create("Model", "Scenario")
        indexset_1, _ = create_indexsets_for_run(platform=platform, run_id=run.id)
        parameter = platform.backend.optimization.parameters.create(
            run_id=run.id, name="Parameter", constrained_to_indexsets=[indexset_1.name]
        )

        # TODO How to check that DeletionPrevented is raised? No other object uses
        # Parameter.id, so nothing could prevent the deletion.

        # Test unknown id raises
        with pytest.raises(Parameter.NotFound):
            platform.backend.optimization.parameters.delete(id=(parameter.id + 1))

        # Test normal deletion
        platform.backend.optimization.parameters.delete(id=parameter.id)

        assert platform.backend.optimization.parameters.tabulate().empty

        # Confirm that IndexSet has not been deleted
        assert not platform.backend.optimization.indexsets.tabulate().empty

        # Test that association table rows are deleted
        # If they haven't, this would raise DeletionPrevented
        platform.backend.optimization.indexsets.delete(id=indexset_1.id)

    def test_get_parameter(self, platform: ixmp4.Platform) -> None:
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

    def test_parameter_add_data(self, platform: ixmp4.Platform) -> None:
        run = platform.backend.runs.create("Model", "Scenario")
        unit = platform.backend.units.create("Unit")
        indexset, indexset_2 = create_indexsets_for_run(
            platform=platform, run_id=run.id
        )
        platform.backend.optimization.indexsets.add_data(
            id=indexset.id, data=["foo", "bar", ""]
        )
        platform.backend.optimization.indexsets.add_data(
            id=indexset_2.id, data=[1, 2, 3]
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
            id=parameter.id, data=test_data_1
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
            OptimizationItemUsageError, match=r"must include the column\(s\): values!"
        ):
            platform.backend.optimization.parameters.add_data(
                id=parameter_2.id,
                data=pd.DataFrame(
                    {
                        indexset.name: [None],
                        indexset_2.name: [2],
                        "units": [unit.name],
                    }
                ),
            )

        with pytest.raises(
            OptimizationItemUsageError, match=r"must include the column\(s\): units!"
        ):
            platform.backend.optimization.parameters.add_data(
                id=parameter_2.id,
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
        with pytest.raises(
            OptimizationDataValidationError,
            match="All arrays must be of the same length",
        ):
            platform.backend.optimization.parameters.add_data(
                id=parameter_2.id,
                data={
                    indexset.name: ["foo", "foo"],
                    indexset_2.name: [2, 2],
                    "values": [1, 2],
                    "units": [unit.name],
                },
            )

        with pytest.raises(
            OptimizationDataValidationError, match="contains duplicate rows"
        ):
            platform.backend.optimization.parameters.add_data(
                id=parameter_2.id,
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
            id=parameter_2.id, data=test_data_2
        )
        parameter_2 = platform.backend.optimization.parameters.get(
            run_id=run.id, name="Parameter 2"
        )
        assert parameter_2.data == test_data_2

        # TODO With the current update method (using pandas), order is not conserved.
        # Is that a bad thing, though? Because order is based on the indexsets, which
        # shouldn't be too bad.
        # It seems a little inconsistent though, at the moment: when there's no data
        # before, add_data will combine_first() with empty df as other, which doesn't
        # change anything, so reset_index() restores order. But if other is not empty,
        # order is not restored after combination. And how would it be? All new in place
        #  or appended?
        unit_2 = platform.backend.units.create("Unit 2")

        # Test updating of existing keys
        parameter_4 = platform.backend.optimization.parameters.create(
            run_id=run.id,
            name="Parameter 4",
            constrained_to_indexsets=[indexset.name, indexset_2.name],
        )
        test_data_6 = {
            indexset.name: ["foo", "foo", "bar", "bar"],
            indexset_2.name: [1, 3, 1, 2],
            "values": [1, "2", 2.3, "4"],
            "units": [unit.name] * 4,
        }
        platform.backend.optimization.parameters.add_data(
            id=parameter_4.id, data=test_data_6
        )
        test_data_7 = {
            indexset.name: ["foo", "foo", "bar", "bar", "bar"],
            indexset_2.name: [1, 2, 3, 2, 1],
            "values": [1, 2.3, 3, 4, "5"],
            "units": [unit.name] * 2 + [unit_2.name] * 3,
        }
        platform.backend.optimization.parameters.add_data(
            id=parameter_4.id, data=test_data_7
        )
        parameter_4 = platform.backend.optimization.parameters.get(
            run_id=run.id, name="Parameter 4"
        )
        expected = (
            pd.DataFrame(test_data_7)
            .set_index([indexset.name, indexset_2.name])
            .combine_first(
                pd.DataFrame(test_data_6).set_index([indexset.name, indexset_2.name])
            )
            .reset_index()
        )
        assert_unordered_equality(expected, pd.DataFrame(parameter_4.data))

        # Test adding with column_names
        parameter_3 = platform.backend.optimization.parameters.create(
            run_id=run.id,
            name="Parameter 3",
            constrained_to_indexsets=[indexset.name, indexset_2.name],
            column_names=["Column 1", "Column 2"],
        )
        test_data_8 = {
            "Column 1": ["", "", "foo", "foo", "bar", "bar"],
            "Column 2": [3, 1, 2, 1, 2, 3],
            "values": [6, 5, 4, 3, 2, 1],
            "units": [unit.name] * 6,
        }
        platform.backend.optimization.parameters.add_data(
            id=parameter_3.id, data=test_data_8
        )
        parameter_3 = platform.backend.optimization.parameters.get(
            run_id=run.id, name="Parameter 3"
        )

        assert parameter_3.data == test_data_8

        # Test adding empty data works
        platform.backend.optimization.parameters.add_data(id=parameter_3.id, data={})
        parameter_3 = platform.backend.optimization.parameters.get(
            run_id=run.id, name="Parameter 3"
        )
        assert parameter_3.data == test_data_8

    def test_parameter_remove_data(self, platform: ixmp4.Platform) -> None:
        run = platform.backend.runs.create("Model", "Scenario")

        # Prepare a Parameter containing some test data
        unit = platform.backend.units.create("Unit")
        indexset_1, indexset_2 = create_indexsets_for_run(
            platform=platform, run_id=run.id
        )
        platform.backend.optimization.indexsets.add_data(
            id=indexset_1.id, data=["foo", "bar", ""]
        )
        platform.backend.optimization.indexsets.add_data(
            id=indexset_2.id, data=[1, 2, 3]
        )
        initial_data: dict[str, list[int] | list[str]] = {
            indexset_1.name: ["foo", "foo", "foo", "bar", "bar", "bar"],
            indexset_2.name: [1, 2, 3, 1, 2, 3],
            "values": [1, 2, 3, 4, 5, 6],
            "units": [unit.name] * 6,
        }
        parameter = platform.backend.optimization.parameters.create(
            run_id=run.id,
            name="Parameter",
            constrained_to_indexsets=[indexset_1.name, indexset_2.name],
        )
        platform.backend.optimization.parameters.add_data(
            id=parameter.id, data=initial_data
        )

        # Test removing empty data removes nothing
        platform.backend.optimization.parameters.remove_data(id=parameter.id, data={})
        parameter = platform.backend.optimization.parameters.get(
            run_id=run.id, name="Parameter"
        )

        assert parameter.data == initial_data

        # Test incomplete index raises
        with pytest.raises(
            OptimizationItemUsageError, match="data to be removed must specify"
        ):
            platform.backend.optimization.parameters.remove_data(
                id=parameter.id, data={indexset_1.name: ["foo"]}
            )

        # Test unknown keys without indexed columns raises...
        with pytest.raises(
            OptimizationItemUsageError, match="data to be removed must specify"
        ):
            platform.backend.optimization.parameters.remove_data(
                id=parameter.id, data={"foo": ["bar"]}
            )

        # ...even when removing a column that's known in principle
        with pytest.raises(
            OptimizationItemUsageError, match="data to be removed must specify"
        ):
            platform.backend.optimization.parameters.remove_data(
                id=parameter.id, data={"units": [unit.name]}
            )

        # Test removing one row
        remove_data_1: dict[str, list[int] | list[str]] = {
            indexset_1.name: ["foo"],
            indexset_2.name: [1],
        }
        platform.backend.optimization.parameters.remove_data(
            id=parameter.id, data=remove_data_1
        )
        parameter = platform.backend.optimization.parameters.get(
            run_id=run.id, name="Parameter"
        )

        # Prepare the expectation from the original test data
        # You can confirm manually that only the correct types are removed
        for key in remove_data_1.keys():
            initial_data[key].remove(remove_data_1[key][0])  # type: ignore[arg-type]
        initial_data["values"].remove(1)  # type: ignore[arg-type]
        initial_data["units"].remove(unit.name)  # type: ignore[arg-type]

        assert parameter.data == initial_data

        # Test removing non-existing (but correctly formatted) data works, even with
        # additional/unused columns
        remove_data_1["values"] = [1]
        platform.backend.optimization.parameters.remove_data(
            id=parameter.id, data=remove_data_1
        )
        parameter = platform.backend.optimization.parameters.get(
            run_id=run.id, name="Parameter"
        )

        assert parameter.data == initial_data

        # Test removing multiple rows
        remove_data_2 = pd.DataFrame(
            {indexset_1.name: ["foo", "bar", "bar"], indexset_2.name: [3, 1, 3]}
        )
        platform.backend.optimization.parameters.remove_data(
            id=parameter.id, data=remove_data_2
        )
        parameter = platform.backend.optimization.parameters.get(
            run_id=run.id, name="Parameter"
        )

        # Prepare the expectation
        expected = {
            indexset_1.name: ["foo", "bar"],
            indexset_2.name: [2, 2],
            "values": [2, 5],
            "units": [unit.name] * 2,
        }

        assert parameter.data == expected

        # Test removing all remaining data
        remove_data_3 = {indexset_1.name: ["foo", "bar"], indexset_2.name: [2, 2]}
        platform.backend.optimization.parameters.remove_data(
            id=parameter.id, data=remove_data_3
        )
        parameter = platform.backend.optimization.parameters.get(
            run_id=run.id, name="Parameter"
        )

        assert parameter.data == {}

    def test_list_parameter(self, platform: ixmp4.Platform) -> None:
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

    def test_tabulate_parameter(self, platform: ixmp4.Platform) -> None:
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
        platform.backend.optimization.indexsets.add_data(
            id=indexset.id, data=["foo", "bar"]
        )
        platform.backend.optimization.indexsets.add_data(
            id=indexset_2.id, data=[1, 2, 3]
        )
        test_data_1 = {
            indexset.name: ["foo"],
            indexset_2.name: [1],
            "values": ["value"],
            "units": [unit.name],
        }
        platform.backend.optimization.parameters.add_data(
            id=parameter.id, data=test_data_1
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
            id=parameter_2.id, data=test_data_2
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
