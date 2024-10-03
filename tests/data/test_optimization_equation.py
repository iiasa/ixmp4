import pandas as pd
import pytest

import ixmp4
from ixmp4.core.exceptions import (
    OptimizationDataValidationError,
    OptimizationItemUsageError,
)
from ixmp4.data.abstract import Equation

from ..utils import assert_unordered_equality, create_indexsets_for_run


def df_from_list(equations: list):
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
    def test_create_equation(self, platform: ixmp4.Platform):
        run = platform.backend.runs.create("Model", "Scenario")

        # Test normal creation
        indexset, indexset_2 = create_indexsets_for_run(
            platform=platform, run_id=run.id
        )
        equation = platform.backend.optimization.equations.create(
            run_id=run.id,
            name="Equation",
            constrained_to_indexsets=[indexset.name],
        )

        assert equation.run__id == run.id
        assert equation.name == "Equation"
        assert equation.data == {}  # JsonDict type currently requires a dict, not None
        assert equation.columns[0].name == indexset.name
        assert equation.columns[0].constrained_to_indexset == indexset.id

        # Test duplicate name raises
        with pytest.raises(Equation.NotUnique):
            _ = platform.backend.optimization.equations.create(
                run_id=run.id, name="Equation", constrained_to_indexsets=[indexset.name]
            )

        # Test mismatch in constrained_to_indexsets and column_names raises
        with pytest.raises(OptimizationItemUsageError, match="not equal in length"):
            _ = platform.backend.optimization.equations.create(
                run_id=run.id,
                name="Equation 2",
                constrained_to_indexsets=[indexset.name],
                column_names=["Dimension 1", "Dimension 2"],
            )

        # Test columns_names are used for names if given
        equation_2 = platform.backend.optimization.equations.create(
            run_id=run.id,
            name="Equation 2",
            constrained_to_indexsets=[indexset.name],
            column_names=["Column 1"],
        )
        assert equation_2.columns[0].name == "Column 1"

        # Test duplicate column_names raise
        with pytest.raises(
            OptimizationItemUsageError, match="`column_names` are not unique"
        ):
            _ = platform.backend.optimization.equations.create(
                run_id=run.id,
                name="Equation 3",
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
        equation_3 = platform.backend.optimization.equations.create(
            run_id=run.id,
            name="Equation 5",
            constrained_to_indexsets=[indexset.name, indexset_2.name],
        )
        # If indexset doesn't have elements, a generic dtype is registered
        assert equation_3.columns[0].dtype == "object"
        assert equation_3.columns[1].dtype == "int64"

    def test_get_equation(self, platform: ixmp4.Platform):
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

    def test_equation_add_data(self, platform: ixmp4.Platform):
        run = platform.backend.runs.create("Model", "Scenario")
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
            "levels": [3.14],
            "marginals": [-3.14],
        }
        equation = platform.backend.optimization.equations.create(
            run_id=run.id,
            name="Equation",
            constrained_to_indexsets=[indexset.name, indexset_2.name],
        )
        platform.backend.optimization.equations.add_data(
            equation_id=equation.id, data=test_data_1
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
                equation_id=equation_2.id,
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
            platform.backend.optimization.equations.add_data(
                equation_id=equation_2.id,
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
            platform.backend.optimization.equations.add_data(
                equation_id=equation_2.id,
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
                equation_id=equation_2.id,
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
            equation_id=equation_2.id, data=test_data_2
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
        test_data_6 = {
            indexset.name: ["foo", "foo", "bar", "bar"],
            indexset_2.name: [1, 3, 1, 2],
            "levels": [0.00001, "2", 2.3, 400000],
            "marginals": [6, 7.8, 9, 0],
        }
        platform.backend.optimization.equations.add_data(
            equation_id=equation_4.id, data=test_data_6
        )
        test_data_7 = {
            indexset.name: ["foo", "foo", "bar", "bar", "bar"],
            indexset_2.name: [1, 2, 3, 2, 1],
            "levels": [0.00001, 2.3, 3, "400000", "5"],
            "marginals": [6, 7.8, 9, "0", 3],
        }
        platform.backend.optimization.equations.add_data(
            equation_id=equation_4.id, data=test_data_7
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
        assert_unordered_equality(expected, pd.DataFrame(equation_4.data))

    def test_equation_remove_data(self, platform: ixmp4.Platform):
        run = platform.backend.runs.create("Model", "Scenario")
        (indexset,) = create_indexsets_for_run(
            platform=platform, run_id=run.id, amount=1
        )
        platform.backend.optimization.indexsets.add_elements(
            indexset_id=indexset.id, elements=["foo", "bar"]
        )
        test_data = {
            indexset.name: ["bar", "foo"],
            "levels": [2.0, 1],
            "marginals": [0, "test"],
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

        platform.backend.optimization.equations.remove_data(equation_id=equation.id)
        equation = platform.backend.optimization.equations.get(
            run_id=run.id, name="Equation"
        )
        assert equation.data == {}

    def test_list_equation(self, platform: ixmp4.Platform):
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

    def test_tabulate_equation(self, platform: ixmp4.Platform):
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

        platform.backend.optimization.indexsets.add_elements(
            indexset_id=indexset.id, elements=["foo", "bar"]
        )
        platform.backend.optimization.indexsets.add_elements(
            indexset_id=indexset_2.id, elements=[1, 2, 3]
        )
        test_data_1 = {
            indexset.name: ["foo"],
            indexset_2.name: [1],
            "levels": [32],
            "marginals": [-0],
        }
        platform.backend.optimization.equations.add_data(
            equation_id=equation.id, data=test_data_1
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
            equation_id=equation_2.id, data=test_data_2
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
