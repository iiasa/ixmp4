import pandas as pd
import pytest

from ixmp4 import Platform
from ixmp4.core import Equation

from ..utils import all_platforms


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


@all_platforms
class TestDataOptimizationEquation:
    def test_create_equation(self, test_mp, request):
        test_mp: Platform = request.getfixturevalue(test_mp)  # type: ignore
        run = test_mp.backend.runs.create("Model", "Scenario")

        # Test normal creation
        indexset_1 = test_mp.backend.optimization.indexsets.create(
            run_id=run.id, name="Indexset"
        )
        equation = test_mp.backend.optimization.equations.create(
            run_id=run.id,
            name="Equation",
            constrained_to_indexsets=["Indexset"],
        )

        assert equation.run__id == run.id
        assert equation.name == "Equation"
        assert equation.data == {}  # JsonDict type currently requires a dict, not None
        assert equation.columns[0].name == "Indexset"
        assert equation.columns[0].constrained_to_indexset == indexset_1.id

        # Test duplicate name raises
        with pytest.raises(Equation.NotUnique):
            _ = test_mp.backend.optimization.equations.create(
                run_id=run.id, name="Equation", constrained_to_indexsets=["Indexset"]
            )

        # Test mismatch in constrained_to_indexsets and column_names raises
        with pytest.raises(ValueError, match="not equal in length"):
            _ = test_mp.backend.optimization.equations.create(
                run_id=run.id,
                name="Equation 2",
                constrained_to_indexsets=["Indexset"],
                column_names=["Dimension 1", "Dimension 2"],
            )

        # Test columns_names are used for names if given
        equation_2 = test_mp.backend.optimization.equations.create(
            run_id=run.id,
            name="Equation 2",
            constrained_to_indexsets=[indexset_1.name],
            column_names=["Column 1"],
        )
        assert equation_2.columns[0].name == "Column 1"

        # Test duplicate column_names raise
        with pytest.raises(ValueError, match="`column_names` are not unique"):
            _ = test_mp.backend.optimization.equations.create(
                run_id=run.id,
                name="Equation 3",
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
        equation_3 = test_mp.backend.optimization.equations.create(
            run_id=run.id,
            name="Equation 5",
            constrained_to_indexsets=["Indexset", indexset_2.name],
        )
        # If indexset doesn't have elements, a generic dtype is registered
        assert equation_3.columns[0].dtype == "object"
        assert equation_3.columns[1].dtype == "int64"

    def test_get_equation(self, test_mp, request):
        test_mp: Platform = request.getfixturevalue(test_mp)  # type: ignore
        run = test_mp.backend.runs.create("Model", "Scenario")
        _ = test_mp.backend.optimization.indexsets.create(
            run_id=run.id, name="Indexset"
        )
        equation = test_mp.backend.optimization.equations.create(
            run_id=run.id, name="Equation", constrained_to_indexsets=["Indexset"]
        )
        assert equation == test_mp.backend.optimization.equations.get(
            run_id=run.id, name="Equation"
        )

        with pytest.raises(Equation.NotFound):
            _ = test_mp.backend.optimization.equations.get(
                run_id=run.id, name="Equation 2"
            )

    def test_equation_add_data(self, test_mp, request):
        test_mp: Platform = request.getfixturevalue(test_mp)  # type: ignore
        run = test_mp.backend.runs.create("Model", "Scenario")
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
            "levels": [3.14],
            "marginals": [-3.14],
        }
        equation = test_mp.backend.optimization.equations.create(
            run_id=run.id,
            name="Equation",
            constrained_to_indexsets=[indexset_1.name, indexset_2.name],
        )
        test_mp.backend.optimization.equations.add_data(
            equation_id=equation.id, data=test_data_1
        )

        equation = test_mp.backend.optimization.equations.get(
            run_id=run.id, name="Equation"
        )
        assert equation.data == test_data_1

        equation_2 = test_mp.backend.optimization.equations.create(
            run_id=run.id,
            name="Equation 2",
            constrained_to_indexsets=[indexset_1.name, indexset_2.name],
        )

        with pytest.raises(
            AssertionError, match=r"must include the column\(s\): levels!"
        ):
            test_mp.backend.optimization.equations.add_data(
                equation_id=equation_2.id,
                data=pd.DataFrame(
                    {
                        "Indexset": [None],
                        "Indexset 2": [2],
                        "marginals": [1],
                    }
                ),
            )

        with pytest.raises(
            AssertionError, match=r"must include the column\(s\): marginals!"
        ):
            test_mp.backend.optimization.equations.add_data(
                equation_id=equation_2.id,
                data=pd.DataFrame(
                    {
                        "Indexset": [None],
                        "Indexset 2": [2],
                        "levels": [1],
                    }
                ),
            )

        # By converting data to pd.DataFrame, we automatically enforce equal length
        # of new columns, raises All arrays must be of the same length otherwise:
        with pytest.raises(ValueError, match="All arrays must be of the same length"):
            test_mp.backend.optimization.equations.add_data(
                equation_id=equation_2.id,
                data={
                    "Indexset": ["foo", "foo"],
                    "Indexset 2": [2, 2],
                    "levels": [1, 2],
                    "marginals": [1],
                },
            )

        with pytest.raises(ValueError, match="contains duplicate rows"):
            test_mp.backend.optimization.equations.add_data(
                equation_id=equation_2.id,
                data={
                    "Indexset": ["foo", "foo"],
                    "Indexset 2": [2, 2],
                    "levels": [1, 2],
                    "marginals": [-1, -2],
                },
            )

        # Test that order is conserved
        test_data_2 = {
            "Indexset": ["", "", "foo", "foo", "bar", "bar"],
            "Indexset 2": [3, 1, 2, 1, 2, 3],
            "levels": [6, 5, 4, 3, 2, 1],
            "marginals": [1, 3, 5, 6, 4, 2],
        }
        test_mp.backend.optimization.equations.add_data(
            equation_id=equation_2.id, data=test_data_2
        )
        equation_2 = test_mp.backend.optimization.equations.get(
            run_id=run.id, name="Equation 2"
        )
        assert equation_2.data == test_data_2

        # Test order is conserved with varying types and upon later addition of data
        equation_3 = test_mp.backend.optimization.equations.create(
            run_id=run.id,
            name="Equation 3",
            constrained_to_indexsets=[indexset_1.name, indexset_2.name],
            column_names=["Column 1", "Column 2"],
        )

        test_data_3 = {
            "Column 1": ["bar", "foo", ""],
            "Column 2": [2, 3, 1],
            "levels": [3, 2.0, -1],
            "marginals": [100000, 1, 0.00001],
        }
        test_mp.backend.optimization.equations.add_data(
            equation_id=equation_3.id, data=test_data_3
        )
        equation_3 = test_mp.backend.optimization.equations.get(
            run_id=run.id, name="Equation 3"
        )
        assert equation_3.data == test_data_3

        test_data_4 = {
            "Column 1": ["foo", "", "bar"],
            "Column 2": [2, 3, 1],
            "levels": [3.14, 2, -1],
            "marginals": [1, 0.00001, 100000],
        }
        test_mp.backend.optimization.equations.add_data(
            equation_id=equation_3.id, data=test_data_4
        )
        equation_3 = test_mp.backend.optimization.equations.get(
            run_id=run.id, name="Equation 3"
        )
        test_data_5 = test_data_3.copy()
        for key, value in test_data_4.items():
            test_data_5[key].extend(value)
        assert equation_3.data == test_data_5

    def test_list_equation(self, test_mp, request):
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
        equation = test_mp.backend.optimization.equations.create(
            run_id=run.id, name="Equation", constrained_to_indexsets=["Indexset"]
        )
        equation_2 = test_mp.backend.optimization.equations.create(
            run_id=run.id, name="Equation 2", constrained_to_indexsets=["Indexset 2"]
        )
        assert [
            equation,
            equation_2,
        ] == test_mp.backend.optimization.equations.list()

        assert [equation] == test_mp.backend.optimization.equations.list(
            name="Equation"
        )

    def test_tabulate_equation(self, test_mp, request):
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
        equation = test_mp.backend.optimization.equations.create(
            run_id=run.id,
            name="Equation",
            constrained_to_indexsets=["Indexset", "Indexset 2"],
        )
        equation_2 = test_mp.backend.optimization.equations.create(
            run_id=run.id,
            name="Equation 2",
            constrained_to_indexsets=["Indexset", "Indexset 2"],
        )
        pd.testing.assert_frame_equal(
            df_from_list([equation_2]),
            test_mp.backend.optimization.equations.tabulate(name="Equation 2"),
        )

        test_mp.backend.optimization.indexsets.add_elements(
            indexset_id=indexset.id, elements=["foo", "bar"]
        )
        test_mp.backend.optimization.indexsets.add_elements(
            indexset_id=indexset_2.id, elements=[1, 2, 3]
        )
        test_data_1 = {
            "Indexset": ["foo"],
            "Indexset 2": [1],
            "levels": [32],
            "marginals": [-0],
        }
        test_mp.backend.optimization.equations.add_data(
            equation_id=equation.id, data=test_data_1
        )
        equation = test_mp.backend.optimization.equations.get(
            run_id=run.id, name="Equation"
        )

        test_data_2 = {
            "Indexset 2": [2, 3],
            "Indexset": ["foo", "bar"],
            "levels": [1, -3.1],
            "marginals": [2.0, -4],
        }
        test_mp.backend.optimization.equations.add_data(
            equation_id=equation_2.id, data=test_data_2
        )
        equation_2 = test_mp.backend.optimization.equations.get(
            run_id=run.id, name="Equation 2"
        )
        pd.testing.assert_frame_equal(
            df_from_list([equation, equation_2]),
            test_mp.backend.optimization.equations.tabulate(),
        )