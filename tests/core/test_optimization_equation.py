import pandas as pd
import pytest

from ixmp4 import Platform
from ixmp4.core import Equation

from ..utils import all_platforms


def df_from_list(equations: list):
    return pd.DataFrame(
        [
            [
                equation.run_id,
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
class TestCoreEquation:
    def test_create_equation(self, test_mp, request):
        test_mp: Platform = request.getfixturevalue(test_mp)  # type: ignore
        run = test_mp.runs.create("Model", "Scenario")

        # Test normal creation
        indexset_1 = run.optimization.indexsets.create("Indexset")
        equation = run.optimization.equations.create(
            name="Equation",
            constrained_to_indexsets=["Indexset"],
        )

        assert equation.run_id == run.id
        assert equation.name == "Equation"
        assert equation.data == {}  # JsonDict type currently requires a dict, not None
        assert equation.columns[0].name == "Indexset"
        assert equation.constrained_to_indexsets == [indexset_1.name]
        assert equation.levels == []
        assert equation.marginals == []

        # Test duplicate name raises
        with pytest.raises(Equation.NotUnique):
            _ = run.optimization.equations.create(
                "Equation", constrained_to_indexsets=["Indexset"]
            )

        # Test mismatch in constrained_to_indexsets and column_names raises
        with pytest.raises(ValueError, match="not equal in length"):
            _ = run.optimization.equations.create(
                "Equation 2",
                constrained_to_indexsets=["Indexset"],
                column_names=["Dimension 1", "Dimension 2"],
            )

        # Test columns_names are used for names if given
        equation_2 = run.optimization.equations.create(
            "Equation 2",
            constrained_to_indexsets=[indexset_1.name],
            column_names=["Column 1"],
        )
        assert equation_2.columns[0].name == "Column 1"

        # Test duplicate column_names raise
        with pytest.raises(ValueError, match="`column_names` are not unique"):
            _ = run.optimization.equations.create(
                name="Equation 3",
                constrained_to_indexsets=[indexset_1.name, indexset_1.name],
                column_names=["Column 1", "Column 1"],
            )

        # Test column.dtype is registered correctly
        indexset_2 = run.optimization.indexsets.create("Indexset 2")
        indexset_2.add(elements=2024)
        equation_3 = run.optimization.equations.create(
            "Equation 5",
            constrained_to_indexsets=["Indexset", indexset_2.name],
        )
        # If indexset doesn't have elements, a generic dtype is registered
        assert equation_3.columns[0].dtype == "object"
        assert equation_3.columns[1].dtype == "int64"

    def test_get_equation(self, test_mp, request):
        test_mp: Platform = request.getfixturevalue(test_mp)  # type: ignore
        run = test_mp.runs.create("Model", "Scenario")
        indexset = run.optimization.indexsets.create("Indexset")
        _ = run.optimization.equations.create(
            name="Equation", constrained_to_indexsets=["Indexset"]
        )
        equation = run.optimization.equations.get(name="Equation")
        assert equation.run_id == run.id
        assert equation.id == 1
        assert equation.name == "Equation"
        assert equation.data == {}
        assert equation.levels == []
        assert equation.marginals == []
        assert equation.columns[0].name == indexset.name
        assert equation.constrained_to_indexsets == [indexset.name]

        with pytest.raises(Equation.NotFound):
            _ = run.optimization.equations.get("Equation 2")

    def test_equation_add_data(self, test_mp, request):
        test_mp: Platform = request.getfixturevalue(test_mp)  # type: ignore
        run = test_mp.runs.create("Model", "Scenario")
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
            "levels": [3.14],
            "marginals": [0.000314],
        }
        equation = run.optimization.equations.create(
            "Equation",
            constrained_to_indexsets=[indexset_1.name, indexset_2.name],
        )
        equation.add(data=test_data_1)
        assert equation.data == test_data_1
        assert equation.levels == test_data_1["levels"]
        assert equation.marginals == test_data_1["marginals"]

        equation_2 = run.optimization.equations.create(
            name="Equation 2",
            constrained_to_indexsets=[indexset_1.name, indexset_2.name],
        )

        with pytest.raises(
            AssertionError, match=r"must include the column\(s\): marginals!"
        ):
            equation_2.add(
                pd.DataFrame(
                    {
                        "Indexset": [None],
                        "Indexset 2": [2],
                        "levels": [1],
                    }
                ),
            )

        with pytest.raises(
            AssertionError, match=r"must include the column\(s\): levels!"
        ):
            equation_2.add(
                data=pd.DataFrame(
                    {
                        "Indexset": [None],
                        "Indexset 2": [2],
                        "marginals": [0],
                    }
                ),
            )

        # By converting data to pd.DataFrame, we automatically enforce equal length
        # of new columns, raises All arrays must be of the same length otherwise:
        with pytest.raises(ValueError, match="All arrays must be of the same length"):
            equation_2.add(
                data={
                    "Indexset": ["foo", "foo"],
                    "Indexset 2": [2, 2],
                    "levels": [1, 2],
                    "marginals": [3],
                },
            )

        with pytest.raises(ValueError, match="contains duplicate rows"):
            equation_2.add(
                data={
                    "Indexset": ["foo", "foo"],
                    "Indexset 2": [2, 2],
                    "levels": [1, 2],
                    "marginals": [3.4, 5.6],
                },
            )

        # Test that order is conserved
        test_data_2 = {
            "Indexset": ["", "", "foo", "foo", "bar", "bar"],
            "Indexset 2": [3, 1, 2, 1, 2, 3],
            "levels": [6, 5, 4, 3, 2, 1],
            "marginals": [1, 3, 5, 6, 4, 2],
        }
        equation_2.add(test_data_2)
        assert equation_2.data == test_data_2
        assert equation_2.levels == test_data_2["levels"]
        assert equation_2.marginals == test_data_2["marginals"]

        # Test order is conserved with varying types and upon later addition of data
        equation_3 = run.optimization.equations.create(
            name="Equation 3",
            constrained_to_indexsets=[indexset_1.name, indexset_2.name],
            column_names=["Column 1", "Column 2"],
        )

        test_data_3 = {
            "Column 1": ["bar", "foo", ""],
            "Column 2": [2, 3, 1],
            "levels": [3, 2.0, 1],
            "marginals": [100000, 1, 0.00001],
        }
        equation_3.add(data=test_data_3)
        assert equation_3.data == test_data_3
        assert equation_3.levels == test_data_3["levels"]
        assert equation_3.marginals == test_data_3["marginals"]

        test_data_4 = {
            "Column 1": ["foo", "", "bar"],
            "Column 2": [2, 3, 1],
            "levels": [3.14, 2, 1.0],
            "marginals": [1, 0.00001, 100000],
        }
        equation_3.add(data=test_data_4)
        test_data_5 = test_data_3.copy()
        for key, value in test_data_4.items():
            test_data_5[key].extend(value)
        assert equation_3.data == test_data_5
        assert equation_3.levels == test_data_5["levels"]
        assert equation_3.marginals == test_data_5["marginals"]

    def test_list_equation(self, test_mp, request):
        test_mp: Platform = request.getfixturevalue(test_mp)  # type: ignore
        run = test_mp.runs.create("Model", "Scenario")
        # Per default, list() lists scalars for `default` version runs:
        run.set_as_default()
        _ = run.optimization.indexsets.create("Indexset")
        _ = run.optimization.indexsets.create("Indexset 2")
        equation = run.optimization.equations.create(
            "Equation", constrained_to_indexsets=["Indexset"]
        )
        equation_2 = run.optimization.equations.create(
            "Equation 2", constrained_to_indexsets=["Indexset 2"]
        )
        expected_ids = [equation.id, equation_2.id]
        list_ids = [equation.id for equation in run.optimization.equations.list()]
        assert not (set(expected_ids) ^ set(list_ids))

        # Test retrieving just one result by providing a name
        expected_id = [equation.id]
        list_id = [
            equation.id for equation in run.optimization.equations.list(name="Equation")
        ]
        assert not (set(expected_id) ^ set(list_id))

    def test_tabulate_equation(self, test_mp, request):
        test_mp: Platform = request.getfixturevalue(test_mp)  # type: ignore
        run = test_mp.runs.create("Model", "Scenario")
        # Per default, tabulate() lists scalars for `default` version runs:
        run.set_as_default()
        indexset = run.optimization.indexsets.create("Indexset")
        indexset_2 = run.optimization.indexsets.create("Indexset 2")
        equation = run.optimization.equations.create(
            name="Equation",
            constrained_to_indexsets=["Indexset", "Indexset 2"],
        )
        equation_2 = run.optimization.equations.create(
            name="Equation 2",
            constrained_to_indexsets=["Indexset", "Indexset 2"],
        )
        pd.testing.assert_frame_equal(
            df_from_list([equation_2]),
            run.optimization.equations.tabulate(name="Equation 2"),
        )

        indexset.add(elements=["foo", "bar"])
        indexset_2.add(elements=[1, 2, 3])
        test_data_1 = {
            "Indexset": ["foo"],
            "Indexset 2": [1],
            "levels": [314],
            "marginals": [2.0],
        }
        equation.add(data=test_data_1)

        test_data_2 = {
            "Indexset 2": [2, 3],
            "Indexset": ["foo", "bar"],
            "levels": [1, -2.0],
            "marginals": [0, 10],
        }
        equation_2.add(data=test_data_2)
        pd.testing.assert_frame_equal(
            df_from_list([equation, equation_2]),
            run.optimization.equations.tabulate(),
        )

    def test_equation_docs(self, test_mp, request):
        test_mp: Platform = request.getfixturevalue(test_mp)  # type: ignore
        run = test_mp.runs.create("Model", "Scenario")
        indexset = run.optimization.indexsets.create("Indexset")
        equation_1 = run.optimization.equations.create(
            "Equation 1", constrained_to_indexsets=[indexset.name]
        )
        docs = "Documentation of Equation 1"
        equation_1.docs = docs
        assert equation_1.docs == docs

        equation_1.docs = None
        assert equation_1.docs is None