import pandas as pd
import pytest

import ixmp4
from ixmp4.core import Equation, IndexSet
from ixmp4.core.exceptions import (
    OptimizationDataValidationError,
    OptimizationItemUsageError,
)
from ixmp4.data.backend.api import RestBackend

from ..utils import assert_unordered_equality, create_indexsets_for_run


def df_from_list(equations: list[Equation]) -> pd.DataFrame:
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


class TestCoreEquation:
    def test_create_equation(self, platform: ixmp4.Platform) -> None:
        run = platform.runs.create("Model", "Scenario")

        # Test normal creation
        indexset, indexset_2 = tuple(
            IndexSet(_backend=platform.backend, _model=model)
            for model in create_indexsets_for_run(platform=platform, run_id=run.id)
        )
        equation = run.optimization.equations.create(
            name="Equation",
            constrained_to_indexsets=[indexset.name],
        )

        assert equation.run_id == run.id
        assert equation.name == "Equation"
        assert equation.data == {}  # JsonDict type currently requires a dict, not None
        assert equation.column_names is None
        assert equation.indexset_names == [indexset.name]
        assert equation.levels == []
        assert equation.marginals == []

        # Test duplicate name raises
        with pytest.raises(Equation.NotUnique):
            _ = run.optimization.equations.create(
                "Equation", constrained_to_indexsets=[indexset.name]
            )
            _ = run.optimization.equations.create(
                "Equation",
                constrained_to_indexsets=[indexset_1.name],
                column_names=["Column 1"],
            )

        # Test mismatch in constrained_to_indexsets and column_names raises
        with pytest.raises(OptimizationItemUsageError, match="not equal in length"):
            _ = run.optimization.equations.create(
                "Equation 2",
                constrained_to_indexsets=[indexset.name],
                column_names=["Dimension 1", "Dimension 2"],
            )

        # Test columns_names are used for names if given
        equation_2 = run.optimization.equations.create(
            "Equation 2",
            constrained_to_indexsets=[indexset.name],
            column_names=["Column 1"],
        )
        assert equation_2.column_names == ["Column 1"]

        # Test duplicate column_names raise
        with pytest.raises(
            OptimizationItemUsageError, match="`column_names` are not unique"
        ):
            _ = run.optimization.equations.create(
                name="Equation 3",
                constrained_to_indexsets=[indexset.name, indexset.name],
                column_names=["Column 1", "Column 1"],
            )

    def test_get_equation(self, platform: ixmp4.Platform) -> None:
        run = platform.runs.create("Model", "Scenario")
        (indexset,) = create_indexsets_for_run(
            platform=platform, run_id=run.id, amount=1
        )
        _ = run.optimization.equations.create(
            name="Equation", constrained_to_indexsets=[indexset.name]
        )
        equation = run.optimization.equations.get(name="Equation")
        assert equation.run_id == run.id
        assert equation.id == 1
        assert equation.name == "Equation"
        assert equation.data == {}
        assert equation.levels == []
        assert equation.marginals == []
        assert equation.column_names is None
        assert equation.indexset_names == [indexset.name]

        with pytest.raises(Equation.NotFound):
            _ = run.optimization.equations.get("Equation 2")

    def test_equation_add_data(self, platform: ixmp4.Platform) -> None:
        run = platform.runs.create("Model", "Scenario")
        indexset, indexset_2 = tuple(
            IndexSet(_backend=platform.backend, _model=model)
            for model in create_indexsets_for_run(platform=platform, run_id=run.id)
        )
        indexset.add(data=["foo", "bar", ""])
        indexset_2.add(data=[1, 2, 3])
        # pandas can only convert dicts to dataframes if the values are lists
        # or if index is given. But maybe using read_json instead of from_dict
        # can remedy this. Or maybe we want to catch the resulting
        # "ValueError: If using all scalar values, you must pass an index" and
        # reraise a custom informative error?
        test_data_1 = {
            indexset.name: ["foo"],
            indexset_2.name: [1],
            "levels": [3.14],
            "marginals": [0.000314],
        }
        equation = run.optimization.equations.create(
            "Equation",
            constrained_to_indexsets=[indexset.name, indexset_2.name],
        )
        equation.add(data=test_data_1)
        assert equation.data == test_data_1
        assert equation.levels == test_data_1["levels"]
        assert equation.marginals == test_data_1["marginals"]

        equation_2 = run.optimization.equations.create(
            name="Equation 2",
            constrained_to_indexsets=[indexset.name, indexset_2.name],
        )

        with pytest.raises(
            OptimizationItemUsageError,
            match=r"must include the column\(s\): marginals!",
        ):
            equation_2.add(
                pd.DataFrame(
                    {
                        indexset.name: ["foo"],
                        indexset_2.name: [2],
                        "levels": [1],
                    }
                ),
            )

        with pytest.raises(
            OptimizationItemUsageError, match=r"must include the column\(s\): levels!"
        ):
            equation_2.add(
                data=pd.DataFrame(
                    {
                        indexset.name: ["foo"],
                        indexset_2.name: [2],
                        "marginals": [0],
                    }
                ),
            )

        # By converting data to pd.DataFrame, we automatically enforce equal length
        # of new columns, raises All arrays must be of the same length otherwise:
        with pytest.raises(
            OptimizationDataValidationError,
            match="All arrays must be of the same length",
        ):
            equation_2.add(
                data={
                    indexset.name: ["foo", "foo"],
                    indexset_2.name: [2, 2],
                    "levels": [1, 2],
                    "marginals": [3],
                },
            )

        with pytest.raises(
            OptimizationDataValidationError, match="contains duplicate rows"
        ):
            equation_2.add(
                data={
                    indexset.name: ["foo", "foo"],
                    indexset_2.name: [2, 2],
                    "levels": [1, 2],
                    "marginals": [3.4, 5.6],
                },
            )

        # Test that order is conserved
        test_data_2 = {
            indexset.name: ["", "", "foo", "foo", "bar", "bar"],
            indexset_2.name: [3, 1, 2, 1, 2, 3],
            "levels": [6, 5, 4, 3, 2, 1],
            "marginals": [1, 3, 5, 6, 4, 2],
        }
        equation_2.add(test_data_2)
        assert equation_2.data == test_data_2
        assert equation_2.levels == test_data_2["levels"]
        assert equation_2.marginals == test_data_2["marginals"]

        # Test updating of existing keys
        equation_4 = run.optimization.equations.create(
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
        equation_4.add(data=test_data_6)
        test_data_7 = {
            indexset.name: ["foo", "foo", "bar", "bar", "bar"],
            indexset_2.name: [1, 2, 3, 2, 1],
            "levels": [0.00001, 2.3, 3, "400000", "5"],
            "marginals": [6, 7.8, 9, "0", 3],
        }
        equation_4.add(data=test_data_7)
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

        # Test adding with column_names
        equation_5 = run.optimization.equations.create(
            name="Equation 5",
            constrained_to_indexsets=[indexset.name, indexset_2.name],
            column_names=["Column 1", "Column 2"],
        )
        test_data_8 = {
            "Column 1": ["", "", "foo", "foo", "bar", "bar"],
            "Column 2": [3, 1, 2, 1, 2, 3],
            "levels": [6, 5, 4, 3, 2, 1],
            "marginals": [0.5] * 6,
        }
        equation_5.add(data=test_data_8)

        assert equation_5.data == test_data_8

    def test_equation_remove_data(self, platform: ixmp4.Platform) -> None:
        run = platform.runs.create("Model", "Scenario")
        indexset = run.optimization.indexsets.create("Indexset")
        indexset.add(data=["foo", "bar"])
        test_data = {
            "Indexset": ["bar", "foo"],
            "levels": [2.3, 1],
            "marginals": [0, 4.2],
        }
        equation = run.optimization.equations.create(
            "Equation",
            constrained_to_indexsets=[indexset.name],
        )
        equation.add(test_data)
        assert equation.data == test_data

        equation.remove_data()
        assert equation.data == {}

    def test_list_equation(self, platform: ixmp4.Platform) -> None:
        run = platform.runs.create("Model", "Scenario")
        # Per default, list() lists scalars for `default` version runs:
        run.set_as_default()
        indexset, indexset_2 = create_indexsets_for_run(
            platform=platform, run_id=run.id
        )
        equation = run.optimization.equations.create(
            "Equation", constrained_to_indexsets=[indexset.name]
        )
        equation_2 = run.optimization.equations.create(
            "Equation 2", constrained_to_indexsets=[indexset_2.name]
        )
        # Create new run to test listing equations of specific run
        run_2 = platform.runs.create("Model", "Scenario")
        (indexset,) = create_indexsets_for_run(
            platform=platform, run_id=run_2.id, amount=1
        )
        run_2.optimization.equations.create(
            "Equation", constrained_to_indexsets=[indexset.name]
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

    def test_tabulate_equation(self, platform: ixmp4.Platform) -> None:
        run = platform.runs.create("Model", "Scenario")
        indexset, indexset_2 = tuple(
            IndexSet(_backend=platform.backend, _model=model)
            for model in create_indexsets_for_run(platform=platform, run_id=run.id)
        )
        equation = run.optimization.equations.create(
            name="Equation",
            constrained_to_indexsets=[indexset.name, indexset_2.name],
        )
        equation_2 = run.optimization.equations.create(
            name="Equation 2",
            constrained_to_indexsets=[indexset.name, indexset_2.name],
        )
        # Create new run to test tabulating equations of specific run
        run_2 = platform.runs.create("Model", "Scenario")
        (indexset_3,) = create_indexsets_for_run(
            platform=platform, run_id=run_2.id, amount=1
        )
        run_2.optimization.equations.create(
            "Equation", constrained_to_indexsets=[indexset_3.name]
        )
        pd.testing.assert_frame_equal(
            df_from_list([equation_2]),
            run.optimization.equations.tabulate(name="Equation 2"),
        )

        indexset.add(data=["foo", "bar"])
        indexset_2.add(data=[1, 2, 3])
        test_data_1 = {
            indexset.name: ["foo"],
            indexset_2.name: [1],
            "levels": [314],
            "marginals": [2.0],
        }
        equation.add(data=test_data_1)

        test_data_2 = {
            indexset_2.name: [2, 3],
            indexset.name: ["foo", "bar"],
            "levels": [1, -2.0],
            "marginals": [0, 10],
        }
        equation_2.add(data=test_data_2)
        pd.testing.assert_frame_equal(
            df_from_list([equation, equation_2]),
            run.optimization.equations.tabulate(),
        )

    def test_equation_docs(self, platform: ixmp4.Platform) -> None:
        run = platform.runs.create("Model", "Scenario")
        (indexset,) = tuple(
            IndexSet(_backend=platform.backend, _model=model)
            for model in create_indexsets_for_run(
                platform=platform, run_id=run.id, amount=1
            )
        )
        equation_1 = run.optimization.equations.create(
            "Equation 1", constrained_to_indexsets=[indexset.name]
        )
        docs = "Documentation of Equation 1"
        equation_1.docs = docs
        assert equation_1.docs == docs

        equation_1.docs = None
        assert equation_1.docs is None
