import pandas as pd
import pytest

from ixmp4 import Platform
from ixmp4.core import OptimizationVariable

from ..utils import all_platforms


def df_from_list(variables: list):
    return pd.DataFrame(
        [
            [
                variable.run_id,
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


@all_platforms
class TestCoreVariable:
    def test_create_variable(self, test_mp, request):
        test_mp: Platform = request.getfixturevalue(test_mp)  # type: ignore
        run = test_mp.runs.create("Model", "Scenario")

        # Test creation without indexset
        variable = run.optimization.variables.create("Variable")
        assert variable.run_id == run.id
        assert variable.name == "Variable"
        assert variable.data == {}
        assert variable.columns == []
        assert variable.constrained_to_indexsets == []
        assert variable.levels == []
        assert variable.marginals == []

        # Test creation with indexset
        indexset_1 = run.optimization.indexsets.create("Indexset")
        variable_2 = run.optimization.variables.create(
            name="Variable 2",
            constrained_to_indexsets=["Indexset"],
        )

        assert variable_2.run_id == run.id
        assert variable_2.name == "Variable 2"
        assert variable_2.data == {}  # JsonDict type currently requires dict, not None
        assert variable_2.columns[0].name == "Indexset"
        assert variable_2.constrained_to_indexsets == [indexset_1.name]
        assert variable_2.levels == []
        assert variable_2.marginals == []

        # Test duplicate name raises
        with pytest.raises(OptimizationVariable.NotUnique):
            _ = run.optimization.variables.create(
                "Variable", constrained_to_indexsets=["Indexset"]
            )

        # Test mismatch in constrained_to_indexsets and column_names raises
        with pytest.raises(ValueError, match="not equal in length"):
            _ = run.optimization.variables.create(
                "Variable 0",
                constrained_to_indexsets=["Indexset"],
                column_names=["Dimension 1", "Dimension 2"],
            )

        # Test columns_names are used for names if given
        variable_3 = run.optimization.variables.create(
            "Variable 3",
            constrained_to_indexsets=[indexset_1.name],
            column_names=["Column 1"],
        )
        assert variable_3.columns[0].name == "Column 1"

        # Test duplicate column_names raise
        with pytest.raises(ValueError, match="`column_names` are not unique"):
            _ = run.optimization.variables.create(
                name="Variable 0",
                constrained_to_indexsets=[indexset_1.name, indexset_1.name],
                column_names=["Column 1", "Column 1"],
            )

        # Test column.dtype is registered correctly
        indexset_2 = run.optimization.indexsets.create("Indexset 2")
        indexset_2.add(elements=2024)
        variable_4 = run.optimization.variables.create(
            "Variable 4",
            constrained_to_indexsets=["Indexset", indexset_2.name],
        )
        # If indexset doesn't have elements, a generic dtype is registered
        assert variable_4.columns[0].dtype == "object"
        assert variable_4.columns[1].dtype == "int64"

    def test_get_variable(self, test_mp, request):
        test_mp: Platform = request.getfixturevalue(test_mp)  # type: ignore
        run = test_mp.runs.create("Model", "Scenario")
        indexset = run.optimization.indexsets.create("Indexset")
        _ = run.optimization.variables.create(
            name="Variable", constrained_to_indexsets=["Indexset"]
        )
        variable = run.optimization.variables.get(name="Variable")
        assert variable.run_id == run.id
        assert variable.id == 1
        assert variable.name == "Variable"
        assert variable.data == {}
        assert variable.levels == []
        assert variable.marginals == []
        assert variable.columns[0].name == indexset.name
        assert variable.constrained_to_indexsets == [indexset.name]

        with pytest.raises(OptimizationVariable.NotFound):
            _ = run.optimization.variables.get("Variable 2")

    def test_variable_add_data(self, test_mp, request):
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
        variable = run.optimization.variables.create(
            "Variable",
            constrained_to_indexsets=[indexset_1.name, indexset_2.name],
        )
        variable.add(data=test_data_1)
        assert variable.data == test_data_1
        assert variable.levels == test_data_1["levels"]
        assert variable.marginals == test_data_1["marginals"]

        variable_2 = run.optimization.variables.create(
            name="Variable 2",
            constrained_to_indexsets=[indexset_1.name, indexset_2.name],
        )

        with pytest.raises(
            AssertionError, match=r"must include the column\(s\): marginals!"
        ):
            variable_2.add(
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
            variable_2.add(
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
            variable_2.add(
                data={
                    "Indexset": ["foo", "foo"],
                    "Indexset 2": [2, 2],
                    "levels": [1, 2],
                    "marginals": [3],
                },
            )

        with pytest.raises(ValueError, match="contains duplicate rows"):
            variable_2.add(
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
        variable_2.add(test_data_2)
        assert variable_2.data == test_data_2
        assert variable_2.levels == test_data_2["levels"]
        assert variable_2.marginals == test_data_2["marginals"]

        # Test order is conserved with varying types and upon later addition of data
        variable_3 = run.optimization.variables.create(
            name="Variable 3",
            constrained_to_indexsets=[indexset_1.name, indexset_2.name],
            column_names=["Column 1", "Column 2"],
        )

        test_data_3 = {
            "Column 1": ["bar", "foo", ""],
            "Column 2": [2, 3, 1],
            "levels": [3, 2.0, 1],
            "marginals": [100000, 1, 0.00001],
        }
        variable_3.add(data=test_data_3)
        assert variable_3.data == test_data_3
        assert variable_3.levels == test_data_3["levels"]
        assert variable_3.marginals == test_data_3["marginals"]

        test_data_4 = {
            "Column 1": ["foo", "", "bar"],
            "Column 2": [2, 3, 1],
            "levels": [3.14, 2, 1.0],
            "marginals": [1, 0.00001, 100000],
        }
        variable_3.add(data=test_data_4)
        test_data_5 = test_data_3.copy()
        for key, value in test_data_4.items():
            test_data_5[key].extend(value)
        assert variable_3.data == test_data_5
        assert variable_3.levels == test_data_5["levels"]
        assert variable_3.marginals == test_data_5["marginals"]

    def test_list_variable(self, test_mp, request):
        test_mp: Platform = request.getfixturevalue(test_mp)  # type: ignore
        run = test_mp.runs.create("Model", "Scenario")
        # Per default, list() lists scalars for `default` version runs:
        run.set_as_default()
        _ = run.optimization.indexsets.create("Indexset")
        _ = run.optimization.indexsets.create("Indexset 2")
        variable = run.optimization.variables.create(
            "Variable", constrained_to_indexsets=["Indexset"]
        )
        variable_2 = run.optimization.variables.create(
            "Variable 2", constrained_to_indexsets=["Indexset 2"]
        )
        expected_ids = [variable.id, variable_2.id]
        list_ids = [variable.id for variable in run.optimization.variables.list()]
        assert not (set(expected_ids) ^ set(list_ids))

        # Test retrieving just one result by providing a name
        expected_id = [variable.id]
        list_id = [
            variable.id for variable in run.optimization.variables.list(name="Variable")
        ]
        assert not (set(expected_id) ^ set(list_id))

    def test_tabulate_variable(self, test_mp, request):
        test_mp: Platform = request.getfixturevalue(test_mp)  # type: ignore
        run = test_mp.runs.create("Model", "Scenario")
        # Per default, tabulate() lists scalars for `default` version runs:
        run.set_as_default()
        indexset = run.optimization.indexsets.create("Indexset")
        indexset_2 = run.optimization.indexsets.create("Indexset 2")
        variable = run.optimization.variables.create(
            name="Variable",
            constrained_to_indexsets=["Indexset", "Indexset 2"],
        )
        variable_2 = run.optimization.variables.create(
            name="Variable 2",
            constrained_to_indexsets=["Indexset", "Indexset 2"],
        )
        pd.testing.assert_frame_equal(
            df_from_list([variable_2]),
            run.optimization.variables.tabulate(name="Variable 2"),
        )

        indexset.add(elements=["foo", "bar"])
        indexset_2.add(elements=[1, 2, 3])
        test_data_1 = {
            "Indexset": ["foo"],
            "Indexset 2": [1],
            "levels": [314],
            "marginals": [2.0],
        }
        variable.add(data=test_data_1)

        test_data_2 = {
            "Indexset 2": [2, 3],
            "Indexset": ["foo", "bar"],
            "levels": [1, -2.0],
            "marginals": [0, 10],
        }
        variable_2.add(data=test_data_2)
        pd.testing.assert_frame_equal(
            df_from_list([variable, variable_2]),
            run.optimization.variables.tabulate(),
        )

    def test_variable_docs(self, test_mp, request):
        test_mp: Platform = request.getfixturevalue(test_mp)  # type: ignore
        run = test_mp.runs.create("Model", "Scenario")
        indexset = run.optimization.indexsets.create("Indexset")
        variable_1 = run.optimization.variables.create(
            "Variable 1", constrained_to_indexsets=[indexset.name]
        )
        docs = "Documentation of Variable 1"
        variable_1.docs = docs
        assert variable_1.docs == docs

        variable_1.docs = None
        assert variable_1.docs is None