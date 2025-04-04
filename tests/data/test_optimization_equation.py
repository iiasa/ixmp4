import pandas as pd
import pytest

import ixmp4
from ixmp4.core.exceptions import (
    OptimizationDataValidationError,
    OptimizationItemUsageError,
)
from ixmp4.data.abstract import Equation
from ixmp4.data.backend.api import RestBackend

from ..utils import assert_unordered_equality, create_indexsets_for_run


def df_from_list(equations: list[Equation]) -> pd.DataFrame:
    return pd.DataFrame(
        [
            [
                equation.run__id,
                equation.data,
                equation.name,
                equation.id,
                equation.created_at,
                equation.created_by,
            ]
            for equation in equations
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


class TestDataOptimizationEquation:
    def test_create_equation(self, platform: ixmp4.Platform) -> None:
        run = platform.backend.runs.create("Model", "Scenario")

        # Test creation without indexset
        equation_1 = platform.backend.optimization.equations.create(
            run_id=run.id, name="Equation 1"
        )
        assert equation_1.run__id == run.id
        assert equation_1.name == "Equation 1"
        assert equation_1.data == {}
        assert equation_1.indexset_names is None
        assert equation_1.column_names is None

        # Test creation with indexset
        indexset_1, _ = create_indexsets_for_run(platform=platform, run_id=run.id)
        equation_2 = platform.backend.optimization.equations.create(
            run_id=run.id,
            name="Equation 2",
            constrained_to_indexsets=[indexset_1.name],
        )

        assert equation_2.run__id == run.id
        assert equation_2.name == "Equation 2"
        assert equation_2.data == {}  # JsonDict type requires a dict, not None
        assert equation_2.column_names is None
        assert equation_2.indexset_names == [indexset_1.name]

        # Test duplicate name raises
        with pytest.raises(Equation.NotUnique):
            _ = platform.backend.optimization.equations.create(
                run_id=run.id,
                name="Equation 1",
                constrained_to_indexsets=[indexset_1.name],
            )
        with pytest.raises(Equation.NotUnique):
            _ = platform.backend.optimization.equations.create(
                run_id=run.id,
                name="Equation 1",
                constrained_to_indexsets=[indexset_1.name],
                column_names=["Column 1"],
            )

        # Test that giving column_names, but not constrained_to_indexsets raises
        with pytest.raises(
            OptimizationItemUsageError,
            match="Received `column_names` to name columns, but no "
            "`constrained_to_indexsets`",
        ):
            _ = platform.backend.optimization.equations.create(
                run_id=run.id,
                name="Equation 0",
                column_names=["Dimension 1"],
            )

        # Test mismatch in constrained_to_indexsets and column_names raises
        with pytest.raises(OptimizationItemUsageError, match="not equal in length"):
            _ = platform.backend.optimization.equations.create(
                run_id=run.id,
                name="Equation 2",
                constrained_to_indexsets=[indexset_1.name],
                column_names=["Dimension 1", "Dimension 2"],
            )

        # Test columns_names are used for names if given
        equation_3 = platform.backend.optimization.equations.create(
            run_id=run.id,
            name="Equation 3",
            constrained_to_indexsets=[indexset_1.name],
            column_names=["Column 1"],
        )
        assert equation_3.column_names == ["Column 1"]

        # Test duplicate column_names raise
        with pytest.raises(
            OptimizationItemUsageError, match="`column_names` are not unique"
        ):
            _ = platform.backend.optimization.equations.create(
                run_id=run.id,
                name="Equation 4",
                constrained_to_indexsets=[indexset_1.name, indexset_1.name],
                column_names=["Column 1", "Column 1"],
            )

        # Test using different column names for same indexset
        equation_4 = platform.backend.optimization.equations.create(
            run_id=run.id,
            name="Equation 4",
            constrained_to_indexsets=[indexset_1.name, indexset_1.name],
            column_names=["Column 1", "Column 2"],
        )

        assert equation_4.column_names == ["Column 1", "Column 2"]
        assert equation_4.indexset_names == [indexset_1.name, indexset_1.name]

    def test_delete_equation(self, platform: ixmp4.Platform) -> None:
        run = platform.backend.runs.create("Model", "Scenario")

        equation_1 = platform.backend.optimization.equations.create(
            run_id=run.id, name="Equation 1"
        )

        # Test deletion without linked IndexSets
        platform.backend.optimization.equations.delete(id=equation_1.id)

        assert platform.backend.optimization.equations.tabulate().empty

        indexset_1, _ = create_indexsets_for_run(platform=platform, run_id=run.id)
        equation_2 = platform.backend.optimization.equations.create(
            run_id=run.id, name="Equation 2", constrained_to_indexsets=[indexset_1.name]
        )

        # TODO How to check that DeletionPrevented is raised? No other object uses
        # Equation.id, so nothing could prevent the deletion.

        # Test unknown id raises
        with pytest.raises(Equation.NotFound):
            platform.backend.optimization.equations.delete(id=(equation_2.id + 1))

        # Test normal deletion
        platform.backend.optimization.equations.delete(id=equation_2.id)

        assert platform.backend.optimization.equations.tabulate().empty

        # Confirm that IndexSet has not been deleted
        assert not platform.backend.optimization.indexsets.tabulate().empty

        # Test that association table rows are deleted
        # If they haven't, this would raise DeletionPrevented
        platform.backend.optimization.indexsets.delete(id=indexset_1.id)

    def test_get_equation(self, platform: ixmp4.Platform) -> None:
        run = platform.backend.runs.create("Model", "Scenario")
        (indexset,) = create_indexsets_for_run(
            platform=platform, run_id=run.id, amount=1
        )
        equation = platform.backend.optimization.equations.create(
            run_id=run.id, name="Equation", constrained_to_indexsets=[indexset.name]
        )
        assert equation == platform.backend.optimization.equations.get(
            run_id=run.id, name="Equation"
        )

        with pytest.raises(Equation.NotFound):
            _ = platform.backend.optimization.equations.get(
                run_id=run.id, name="Equation 2"
            )

    def test_equation_add_data(self, platform: ixmp4.Platform) -> None:
        run = platform.backend.runs.create("Model", "Scenario")
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
            "levels": [3.14],
            "marginals": [-3.14],
        }
        equation = platform.backend.optimization.equations.create(
            run_id=run.id,
            name="Equation",
            constrained_to_indexsets=[indexset.name, indexset_2.name],
        )
        platform.backend.optimization.equations.add_data(
            id=equation.id, data=test_data_1
        )

        equation = platform.backend.optimization.equations.get(
            run_id=run.id, name="Equation"
        )
        assert equation.data == test_data_1

        equation_2 = platform.backend.optimization.equations.create(
            run_id=run.id,
            name="Equation 2",
            constrained_to_indexsets=[indexset.name, indexset_2.name],
        )

        with pytest.raises(
            OptimizationItemUsageError, match=r"must include the column\(s\): levels!"
        ):
            platform.backend.optimization.equations.add_data(
                id=equation_2.id,
                data=pd.DataFrame(
                    {
                        indexset.name: ["foo"],
                        indexset_2.name: [2],
                        "marginals": [1],
                    }
                ),
            )

        with pytest.raises(
            OptimizationItemUsageError,
            match=r"must include the column\(s\): marginals!",
        ):
            platform.backend.optimization.equations.add_data(
                id=equation_2.id,
                data=pd.DataFrame(
                    {
                        indexset.name: ["foo"],
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
            platform.backend.optimization.equations.add_data(
                id=equation_2.id,
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
            platform.backend.optimization.equations.add_data(
                id=equation_2.id,
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
        platform.backend.optimization.equations.add_data(
            id=equation_2.id, data=test_data_2
        )
        equation_2 = platform.backend.optimization.equations.get(
            run_id=run.id, name="Equation 2"
        )
        assert equation_2.data == test_data_2

        # Test updating of existing keys
        equation_4 = platform.backend.optimization.equations.create(
            run_id=run.id,
            name="Equation 4",
            constrained_to_indexsets=[indexset.name, indexset_2.name],
        )
        # NOTE entries for levels and marginals must be convertible to one of
        # (float, int, str)
        test_data_6 = {
            indexset.name: ["foo", "foo", "bar", "bar"],
            indexset_2.name: [1, 3, 1, 2],
            "levels": [0.00001, "2", 2.3, 400000],
            "marginals": [6, 7.8, 9, 0],
        }
        platform.backend.optimization.equations.add_data(
            id=equation_4.id, data=test_data_6
        )
        test_data_7 = {
            indexset.name: ["foo", "foo", "bar", "bar", "bar"],
            indexset_2.name: [1, 2, 3, 2, 1],
            "levels": [0.00001, 2.3, 3, "400000", "5"],
            "marginals": [6, 7.8, 9, "0", 3],
        }
        platform.backend.optimization.equations.add_data(
            id=equation_4.id, data=test_data_7
        )
        equation_4 = platform.backend.optimization.equations.get(
            run_id=run.id, name="Equation 4"
        )
        expected = (
            pd.DataFrame(test_data_7)
            .set_index([indexset.name, indexset_2.name])
            .combine_first(
                pd.DataFrame(test_data_6).set_index([indexset.name, indexset_2.name])
            )
            .reset_index()
        )
        # NOTE Something along the API route converts all levels and marginals to float,
        # while the direct pandas call respects the different dtypes. However, anyone
        # accessing .levels and .marginals will always be served float, so that's fine.
        if isinstance(platform.backend, RestBackend):
            expected = expected.astype({"levels": float, "marginals": float})
        assert_unordered_equality(
            expected, pd.DataFrame(equation_4.data), check_dtype=False
        )

        # Test adding to scalar equation raises
        with pytest.raises(
            OptimizationDataValidationError,
            match="Trying to add data to unknown columns!",
        ):
            equation_5 = platform.backend.optimization.equations.create(
                run_id=run.id, name="Equation 5"
            )
            platform.backend.optimization.equations.add_data(
                id=equation_5.id, data={"foo": ["bar"], "levels": [1], "marginals": [0]}
            )

        # Test adding with column_names
        equation_6 = platform.backend.optimization.equations.create(
            run_id=run.id,
            name="Equation 6",
            constrained_to_indexsets=[indexset.name, indexset_2.name],
            column_names=["Column 1", "Column 2"],
        )
        test_data_8 = {
            "Column 1": ["", "", "foo", "foo", "bar", "bar"],
            "Column 2": [3, 1, 2, 1, 2, 3],
            "levels": [6, 5, 4, 3, 2, 1],
            "marginals": [0.5] * 6,
        }
        platform.backend.optimization.equations.add_data(
            id=equation_6.id, data=test_data_8
        )
        equation_6 = platform.backend.optimization.equations.get(
            run_id=run.id, name="Equation 6"
        )

        assert equation_6.data == test_data_8

        # Test adding empty data works
        platform.backend.optimization.equations.add_data(id=equation_6.id, data={})
        equation_6 = platform.backend.optimization.equations.get(
            run_id=run.id, name="Equation 6"
        )

        assert equation_6.data == test_data_8

    def test_equation_remove_data(self, platform: ixmp4.Platform) -> None:
        run = platform.backend.runs.create("Model", "Scenario")
        (indexset,) = create_indexsets_for_run(
            platform=platform, run_id=run.id, amount=1
        )
        platform.backend.optimization.indexsets.add_data(
            id=indexset.id, data=["foo", "bar"]
        )
        test_data: dict[str, list[float | int | str]] = {
            indexset.name: ["bar", "foo"],
            "levels": [2.3, 1],
            "marginals": [0, 4.2],
        }
        equation = platform.backend.optimization.equations.create(
            run_id=run.id,
            name="Equation",
            constrained_to_indexsets=[indexset.name],
        )
        platform.backend.optimization.equations.add_data(equation.id, test_data)
        equation = platform.backend.optimization.equations.get(
            run_id=run.id, name="Equation"
        )
        assert equation.data == test_data

        # Test removing empty data removes nothing
        platform.backend.optimization.equations.remove_data(
            id=equation.id, data=pd.DataFrame()
        )
        equation = platform.backend.optimization.equations.get(
            run_id=run.id, name="Equation"
        )

        assert equation.data == test_data

        # Test incomplete index raises...
        with pytest.raises(
            OptimizationItemUsageError, match="data to be removed must specify"
        ):
            platform.backend.optimization.equations.remove_data(
                id=equation.id, data={"foo": ["bar"]}
            )

        # ...even when removing a column that's known in principle
        with pytest.raises(
            OptimizationItemUsageError, match="data to be removed must specify"
        ):
            platform.backend.optimization.equations.remove_data(
                id=equation.id, data={"levels": [2.3]}
            )

        # Test removing one row
        remove_data = {indexset.name: [test_data[indexset.name][0]]}
        test_data_2 = {k: [v[1]] for k, v in test_data.items()}
        platform.backend.optimization.equations.remove_data(
            id=equation.id, data=remove_data
        )
        equation = platform.backend.optimization.equations.get(
            run_id=run.id, name="Equation"
        )
        assert equation.data == test_data_2

        # Test removing non-existing (but correctly formatted) data works, even with
        # additional/unused columns
        remove_data["levels"] = [1]
        platform.backend.optimization.equations.remove_data(
            id=equation.id, data=remove_data
        )

        assert equation.data == test_data_2

        # Test removing all rows
        platform.backend.optimization.equations.remove_data(id=equation.id)
        equation = platform.backend.optimization.equations.get(
            run_id=run.id, name="Equation"
        )
        assert equation.data == {}

    def test_list_equation(self, platform: ixmp4.Platform) -> None:
        run = platform.backend.runs.create("Model", "Scenario")
        # Per default, list() lists scalars for `default` version runs:
        platform.backend.runs.set_as_default_version(run.id)
        indexset, indexset_2 = create_indexsets_for_run(
            platform=platform, run_id=run.id
        )
        equation = platform.backend.optimization.equations.create(
            run_id=run.id, name="Equation", constrained_to_indexsets=[indexset.name]
        )
        equation_2 = platform.backend.optimization.equations.create(
            run_id=run.id, name="Equation 2", constrained_to_indexsets=[indexset_2.name]
        )
        assert [
            equation,
            equation_2,
        ] == platform.backend.optimization.equations.list()

        assert [equation] == platform.backend.optimization.equations.list(
            name="Equation"
        )

        # Test listing Equations for specific Run
        run_2 = platform.backend.runs.create("Model", "Scenario")
        (indexset,) = create_indexsets_for_run(
            platform=platform, run_id=run_2.id, amount=1
        )
        equation_3 = platform.backend.optimization.equations.create(
            run_id=run_2.id, name="Equation", constrained_to_indexsets=[indexset.name]
        )
        equation_4 = platform.backend.optimization.equations.create(
            run_id=run_2.id, name="Equation 2", constrained_to_indexsets=[indexset.name]
        )
        assert [
            equation_3,
            equation_4,
        ] == platform.backend.optimization.equations.list(run_id=run_2.id)

    def test_tabulate_equation(self, platform: ixmp4.Platform) -> None:
        run = platform.backend.runs.create("Model", "Scenario")
        indexset, indexset_2 = create_indexsets_for_run(
            platform=platform, run_id=run.id
        )
        equation = platform.backend.optimization.equations.create(
            run_id=run.id,
            name="Equation",
            constrained_to_indexsets=[indexset.name, indexset_2.name],
        )
        equation_2 = platform.backend.optimization.equations.create(
            run_id=run.id,
            name="Equation 2",
            constrained_to_indexsets=[indexset.name, indexset_2.name],
        )
        pd.testing.assert_frame_equal(
            df_from_list([equation_2]),
            platform.backend.optimization.equations.tabulate(name="Equation 2"),
        )

        platform.backend.optimization.indexsets.add_data(
            id=indexset.id, data=["foo", "bar"]
        )
        platform.backend.optimization.indexsets.add_data(
            id=indexset_2.id, data=[1, 2, 3]
        )
        test_data_1 = {
            indexset.name: ["foo"],
            indexset_2.name: [1],
            "levels": [32],
            "marginals": [-0],
        }
        platform.backend.optimization.equations.add_data(
            id=equation.id, data=test_data_1
        )
        equation = platform.backend.optimization.equations.get(
            run_id=run.id, name="Equation"
        )

        test_data_2 = {
            indexset_2.name: [2, 3],
            indexset.name: ["foo", "bar"],
            "levels": [1, -3.1],
            "marginals": [2.0, -4],
        }
        platform.backend.optimization.equations.add_data(
            id=equation_2.id, data=test_data_2
        )
        equation_2 = platform.backend.optimization.equations.get(
            run_id=run.id, name="Equation 2"
        )
        pd.testing.assert_frame_equal(
            df_from_list([equation, equation_2]),
            platform.backend.optimization.equations.tabulate(),
        )

        # Test tabulating Equations for specific Run
        run_2 = platform.backend.runs.create("Model", "Scenario")
        (indexset,) = create_indexsets_for_run(
            platform=platform, run_id=run_2.id, amount=1
        )
        equation_3 = platform.backend.optimization.equations.create(
            run_id=run_2.id, name="Equation", constrained_to_indexsets=[indexset.name]
        )
        equation_4 = platform.backend.optimization.equations.create(
            run_id=run_2.id, name="Equation 2", constrained_to_indexsets=[indexset.name]
        )
        pd.testing.assert_frame_equal(
            df_from_list([equation_3, equation_4]),
            platform.backend.optimization.equations.tabulate(run_id=run_2.id),
        )
