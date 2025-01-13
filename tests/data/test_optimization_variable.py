import pandas as pd
import pytest

import ixmp4
from ixmp4.core.exceptions import (
    OptimizationDataValidationError,
    OptimizationItemUsageError,
)
from ixmp4.data.abstract import OptimizationVariable

from ..utils import assert_unordered_equality, create_indexsets_for_run


def df_from_list(variables: list[OptimizationVariable]) -> pd.DataFrame:
    return pd.DataFrame(
        [
            [
                variable.run__id,
                variable.data,
                variable.name,
                variable.id,
                variable.created_at,
                variable.created_by,
            ]
            for variable in variables
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


class TestDataOptimizationVariable:
    def test_create_variable(self, platform: ixmp4.Platform) -> None:
        run = platform.backend.runs.create("Model", "Scenario")

        # Test creation without indexset
        variable = platform.backend.optimization.variables.create(
            run_id=run.id, name="Variable"
        )
        assert variable.run__id == run.id
        assert variable.name == "Variable"
        assert variable.data == {}
        assert variable.columns == []

        # Test creation with indexset
        indexset, indexset_2 = create_indexsets_for_run(
            platform=platform, run_id=run.id
        )
        variable_2 = platform.backend.optimization.variables.create(
            run_id=run.id,
            name="Variable 2",
            constrained_to_indexsets=[indexset.name],
        )

        assert variable_2.run__id == run.id
        assert variable_2.name == "Variable 2"
        assert variable_2.data == {}  # JsonDict type currently requires dict, not None
        assert variable_2.columns is not None
        assert variable_2.columns[0].name == indexset.name
        assert variable_2.columns[0].constrained_to_indexset == indexset.id

        # Test duplicate name raises
        with pytest.raises(OptimizationVariable.NotUnique):
            _ = platform.backend.optimization.variables.create(
                run_id=run.id, name="Variable", constrained_to_indexsets=[indexset.name]
            )

        # Test that giving column_names, but not constrained_to_indexsets raises
        with pytest.raises(
            OptimizationItemUsageError,
            match="Received `column_names` to name columns, but no "
            "`constrained_to_indexsets`",
        ):
            _ = platform.backend.optimization.variables.create(
                run_id=run.id,
                name="Variable 0",
                column_names=["Dimension 1"],
            )

        # Test mismatch in constrained_to_indexsets and column_names raises
        with pytest.raises(OptimizationItemUsageError, match="not equal in length"):
            _ = platform.backend.optimization.variables.create(
                run_id=run.id,
                name="Variable 0",
                constrained_to_indexsets=[indexset.name],
                column_names=["Dimension 1", "Dimension 2"],
            )

        # Test columns_names are used for names if given
        variable_3 = platform.backend.optimization.variables.create(
            run_id=run.id,
            name="Variable 3",
            constrained_to_indexsets=[indexset.name],
            column_names=["Column 1"],
        )
        assert variable_3.columns is not None
        assert variable_3.columns[0].name == "Column 1"

        # Test duplicate column_names raise
        with pytest.raises(
            OptimizationItemUsageError, match="`column_names` are not unique"
        ):
            _ = platform.backend.optimization.variables.create(
                run_id=run.id,
                name="Variable 0",
                constrained_to_indexsets=[indexset.name, indexset.name],
                column_names=["Column 1", "Column 1"],
            )

        # Test column.dtype is registered correctly
        platform.backend.optimization.indexsets.add_data(indexset_2.id, data=2024)
        indexset_2 = platform.backend.optimization.indexsets.get(
            run.id, indexset_2.name
        )
        variable_4 = platform.backend.optimization.variables.create(
            run_id=run.id,
            name="Variable 4",
            constrained_to_indexsets=[indexset.name, indexset_2.name],
        )
        # If indexset doesn't have data, a generic dtype is registered
        assert variable_4.columns is not None
        assert variable_4.columns[0].dtype == "object"
        assert variable_4.columns[1].dtype == "int64"

    def test_get_variable(self, platform: ixmp4.Platform) -> None:
        run = platform.backend.runs.create("Model", "Scenario")
        (indexset,) = create_indexsets_for_run(
            platform=platform, run_id=run.id, amount=1
        )
        variable = platform.backend.optimization.variables.create(
            run_id=run.id, name="Variable", constrained_to_indexsets=[indexset.name]
        )
        assert variable == platform.backend.optimization.variables.get(
            run_id=run.id, name="Variable"
        )

        with pytest.raises(OptimizationVariable.NotFound):
            _ = platform.backend.optimization.variables.get(
                run_id=run.id, name="Variable 2"
            )

    def test_variable_add_data(self, platform: ixmp4.Platform) -> None:
        run = platform.backend.runs.create("Model", "Scenario")
        indexset, indexset_2 = create_indexsets_for_run(
            platform=platform, run_id=run.id
        )
        platform.backend.optimization.indexsets.add_data(
            indexset_id=indexset.id, data=["foo", "bar", ""]
        )
        platform.backend.optimization.indexsets.add_data(
            indexset_id=indexset_2.id, data=[1, 2, 3]
        )
        # pandas can only convert dicts to dataframes if the values are lists
        # or if index is given. But maybe using read_json instead of from_dict
        # can remedy this. Or maybe we want to catch the resulting
        # "ValueError: If using all scalar values, you must pass an index" and
        # reraise a custom informative error?
        test_data_1 = {
            indexset.name: ["foo"],
            indexset_2.name: [1],
            "levels": [3.14],
            "marginals": [-3.14],
        }
        variable = platform.backend.optimization.variables.create(
            run_id=run.id,
            name="Variable",
            constrained_to_indexsets=[indexset.name, indexset_2.name],
        )
        platform.backend.optimization.variables.add_data(
            variable_id=variable.id, data=test_data_1
        )

        variable = platform.backend.optimization.variables.get(
            run_id=run.id, name="Variable"
        )
        assert variable.data == test_data_1

        variable_2 = platform.backend.optimization.variables.create(
            run_id=run.id,
            name="Variable 2",
            constrained_to_indexsets=[indexset.name, indexset_2.name],
        )

        with pytest.raises(
            OptimizationItemUsageError, match=r"must include the column\(s\): levels!"
        ):
            platform.backend.optimization.variables.add_data(
                variable_id=variable_2.id,
                data=pd.DataFrame(
                    {
                        indexset.name: [None],
                        indexset_2.name: [2],
                        "marginals": [1],
                    }
                ),
            )

        with pytest.raises(
            OptimizationItemUsageError,
            match=r"must include the column\(s\): marginals!",
        ):
            platform.backend.optimization.variables.add_data(
                variable_id=variable_2.id,
                data=pd.DataFrame(
                    {
                        indexset.name: [None],
                        indexset_2.name: [2],
                        "levels": [1],
                    }
                ),
            )

        # By converting data to pd.DataFrame, we automatically enforce equal length
        # of new columns, raises All arrays must be of the same length otherwise:
        with pytest.raises(
            OptimizationDataValidationError,
            match="All arrays must be of the same length",
        ):
            platform.backend.optimization.variables.add_data(
                variable_id=variable_2.id,
                data={
                    indexset.name: ["foo", "foo"],
                    indexset_2.name: [2, 2],
                    "levels": [1, 2],
                    "marginals": [1],
                },
            )

        with pytest.raises(
            OptimizationDataValidationError, match="contains duplicate rows"
        ):
            platform.backend.optimization.variables.add_data(
                variable_id=variable_2.id,
                data={
                    indexset.name: ["foo", "foo"],
                    indexset_2.name: [2, 2],
                    "levels": [1, 2],
                    "marginals": [-1, -2],
                },
            )

        # Test that order is conserved
        test_data_2 = {
            indexset.name: ["", "", "foo", "foo", "bar", "bar"],
            indexset_2.name: [3, 1, 2, 1, 2, 3],
            "levels": [6, 5, 4, 3, 2, 1],
            "marginals": [1, 3, 5, 6, 4, 2],
        }
        platform.backend.optimization.variables.add_data(
            variable_id=variable_2.id, data=test_data_2
        )
        variable_2 = platform.backend.optimization.variables.get(
            run_id=run.id, name="Variable 2"
        )
        assert variable_2.data == test_data_2

        # Test updating of existing keys
        variable_4 = platform.backend.optimization.variables.create(
            run_id=run.id,
            name="Variable 4",
            constrained_to_indexsets=[indexset.name, indexset_2.name],
        )
        test_data_6 = {
            indexset.name: ["foo", "foo", "bar", "bar"],
            indexset_2.name: [1, 3, 1, 2],
            "levels": [0.00001, "2", 2.3, 400000],
            "marginals": [6, 7.8, 9, 0],
        }
        platform.backend.optimization.variables.add_data(
            variable_id=variable_4.id, data=test_data_6
        )
        test_data_7 = {
            indexset.name: ["foo", "foo", "bar", "bar", "bar"],
            indexset_2.name: [1, 2, 3, 2, 1],
            "levels": [0.00001, 2.3, 3, "400000", "5"],
            "marginals": [6, 7.8, 9, "0", 3],
        }
        platform.backend.optimization.variables.add_data(
            variable_id=variable_4.id, data=test_data_7
        )
        variable_4 = platform.backend.optimization.variables.get(
            run_id=run.id, name="Variable 4"
        )
        expected = (
            pd.DataFrame(test_data_7)
            .set_index([indexset.name, indexset_2.name])
            .combine_first(
                pd.DataFrame(test_data_6).set_index([indexset.name, indexset_2.name])
            )
            .reset_index()
        )
        assert_unordered_equality(expected, pd.DataFrame(variable_4.data))

    def test_variable_remove_data(self, platform: ixmp4.Platform) -> None:
        run = platform.backend.runs.create("Model", "Scenario")
        indexset = platform.backend.optimization.indexsets.create(run.id, "Indexset")
        platform.backend.optimization.indexsets.add_data(
            indexset_id=indexset.id, data=["foo", "bar"]
        )
        test_data = {
            "Indexset": ["bar", "foo"],
            "levels": [2.0, 1],
            "marginals": [0, "test"],
        }
        variable = platform.backend.optimization.variables.create(
            run_id=run.id,
            name="Variable",
            constrained_to_indexsets=[indexset.name],
        )
        platform.backend.optimization.variables.add_data(variable.id, test_data)
        variable = platform.backend.optimization.variables.get(
            run_id=run.id, name="Variable"
        )
        assert variable.data == test_data

        platform.backend.optimization.variables.remove_data(variable_id=variable.id)
        variable = platform.backend.optimization.variables.get(
            run_id=run.id, name="Variable"
        )
        assert variable.data == {}

    def test_list_variable(self, platform: ixmp4.Platform) -> None:
        run = platform.backend.runs.create("Model", "Scenario")
        indexset, indexset_2 = create_indexsets_for_run(
            platform=platform, run_id=run.id
        )
        variable = platform.backend.optimization.variables.create(
            run_id=run.id, name="Variable", constrained_to_indexsets=[indexset.name]
        )
        variable_2 = platform.backend.optimization.variables.create(
            run_id=run.id, name="Variable 2", constrained_to_indexsets=[indexset_2.name]
        )
        assert [
            variable,
            variable_2,
        ] == platform.backend.optimization.variables.list()

        assert [variable] == platform.backend.optimization.variables.list(
            name="Variable"
        )

        # Test listing Variables for specific Run
        run_2 = platform.backend.runs.create("Model", "Scenario")
        (indexset,) = create_indexsets_for_run(
            platform=platform, run_id=run_2.id, amount=1
        )
        variable_3 = platform.backend.optimization.variables.create(
            run_id=run_2.id, name="Variable", constrained_to_indexsets=[indexset.name]
        )
        variable_4 = platform.backend.optimization.variables.create(
            run_id=run_2.id, name="Variable 2", constrained_to_indexsets=[indexset.name]
        )
        assert [
            variable_3,
            variable_4,
        ] == platform.backend.optimization.variables.list(run_id=run_2.id)

    def test_tabulate_variable(self, platform: ixmp4.Platform) -> None:
        run = platform.backend.runs.create("Model", "Scenario")
        indexset, indexset_2 = create_indexsets_for_run(
            platform=platform, run_id=run.id
        )
        variable = platform.backend.optimization.variables.create(
            run_id=run.id,
            name="Variable",
            constrained_to_indexsets=[indexset.name, indexset_2.name],
        )
        variable_2 = platform.backend.optimization.variables.create(
            run_id=run.id,
            name="Variable 2",
            constrained_to_indexsets=[indexset.name, indexset_2.name],
        )
        pd.testing.assert_frame_equal(
            df_from_list([variable_2]),
            platform.backend.optimization.variables.tabulate(name="Variable 2"),
        )

        platform.backend.optimization.indexsets.add_data(
            indexset_id=indexset.id, data=["foo", "bar"]
        )
        platform.backend.optimization.indexsets.add_data(
            indexset_id=indexset_2.id, data=[1, 2, 3]
        )
        test_data_1 = {
            indexset.name: ["foo"],
            indexset_2.name: [1],
            "levels": [32],
            "marginals": [-0],
        }
        platform.backend.optimization.variables.add_data(
            variable_id=variable.id, data=test_data_1
        )
        variable = platform.backend.optimization.variables.get(
            run_id=run.id, name="Variable"
        )

        test_data_2 = {
            indexset_2.name: [2, 3],
            indexset.name: ["foo", "bar"],
            "levels": [1, -3.1],
            "marginals": [2.0, -4],
        }
        platform.backend.optimization.variables.add_data(
            variable_id=variable_2.id, data=test_data_2
        )
        variable_2 = platform.backend.optimization.variables.get(
            run_id=run.id, name="Variable 2"
        )
        pd.testing.assert_frame_equal(
            df_from_list([variable, variable_2]),
            platform.backend.optimization.variables.tabulate(),
        )

        # Test tabulation of Variables for specific Run
        run_2 = platform.backend.runs.create("Model", "Scenario")
        (indexset,) = create_indexsets_for_run(
            platform=platform, run_id=run_2.id, amount=1
        )
        variable_3 = platform.backend.optimization.variables.create(
            run_id=run_2.id, name="Variable", constrained_to_indexsets=[indexset.name]
        )
        variable_4 = platform.backend.optimization.variables.create(
            run_id=run_2.id, name="Variable 2", constrained_to_indexsets=[indexset.name]
        )
        pd.testing.assert_frame_equal(
            df_from_list([variable_3, variable_4]),
            platform.backend.optimization.variables.tabulate(run_id=run_2.id),
        )
