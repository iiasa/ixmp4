import datetime
from typing import Any

import pandas as pd
import pytest

import ixmp4
from ixmp4.core.exceptions import OptimizationItemUsageError
from tests import backends
from tests.custom_exception import CustomException

from .base import PlatformTest

platform = backends.get_platform_fixture(scope="class")


class OptimizationEquationTest(PlatformTest):
    @pytest.fixture(scope="class")
    def run(
        self,
        platform: ixmp4.Platform,
    ) -> ixmp4.Run:
        run = platform.runs.create("Model", "Scenario")
        assert run.id == 1
        return run

    @pytest.fixture(scope="class")
    def indexset(self, run: ixmp4.Run) -> ixmp4.optimization.IndexSet:
        with run.transact("Create indexset"):
            indexset = run.optimization.indexsets.create("IndexSet")
            indexset.add_data(["do", "re", "mi", "fa", "so", "la", "ti"])
            return indexset


class TestEquation(OptimizationEquationTest):
    def test_create_equation(
        self,
        run: ixmp4.Run,
        indexset: ixmp4.optimization.IndexSet,
        fake_time: datetime.datetime,
    ):
        with run.transact("Create equations"):
            equation1 = run.optimization.equations.create("Equation 1")

            equation2 = run.optimization.equations.create(
                "Equation 2", constrained_to_indexsets=["IndexSet"]
            )
            equation3 = run.optimization.equations.create(
                "Equation 3",
                constrained_to_indexsets=["IndexSet"],
                column_names=["Column"],
            )

            equation4 = run.optimization.equations.create(
                "Equation 4",
                constrained_to_indexsets=["IndexSet", "IndexSet"],
                column_names=["Column 1", "Column 2"],
            )

        assert equation1.id == 1
        assert equation1.run_id == run.id
        assert equation1.name == "Equation 1"
        assert equation1.data == {}
        assert equation1.indexset_names is None
        assert equation1.column_names is None
        assert equation1.levels == []
        assert equation1.marginals == []
        assert equation1.created_by == "@unknown"
        assert equation1.created_at == fake_time.replace(tzinfo=None)

        assert equation2.id == 2
        assert equation2.indexset_names == ["IndexSet"]
        assert equation2.column_names is None

        assert equation3.id == 3
        assert equation3.indexset_names == ["IndexSet"]
        assert equation3.column_names == ["Column"]

        assert equation4.id == 4
        assert equation4.indexset_names == ["IndexSet", "IndexSet"]
        assert equation4.column_names == ["Column 1", "Column 2"]

    def test_tabulate_equation(self, run: ixmp4.Run):
        ret_df = run.optimization.equations.tabulate()
        assert len(ret_df) == 4
        assert "id" in ret_df.columns
        assert "name" in ret_df.columns
        assert "data" in ret_df.columns
        assert "created_at" in ret_df.columns
        assert "created_by" in ret_df.columns

        assert "run__id" not in ret_df.columns

    def test_list_equation(self, run: ixmp4.Run):
        assert len(run.optimization.equations.list()) == 4

    def test_delete_equation_via_func_obj(self, run: ixmp4.Run):
        with run.transact("Delete equation 1"):
            equation1 = run.optimization.equations.get_by_name("Equation 1")
            run.optimization.equations.delete(equation1)

    def test_delete_equation_via_func_id(self, run: ixmp4.Run):
        with run.transact("Delete equation 2"):
            run.optimization.equations.delete(2)

    def test_delete_equation_via_func_name(self, run: ixmp4.Run):
        with run.transact("Delete equation 3"):
            run.optimization.equations.delete("Equation 3")

    def test_delete_equation_via_obj(self, run: ixmp4.Run):
        equation4 = run.optimization.equations.get_by_name("Equation 4")
        with run.transact("Delete equation 4"):
            equation4.delete()

    def test_equation_empty(self, run: ixmp4.Run):
        assert run.optimization.equations.tabulate().empty
        assert len(run.optimization.equations.list()) == 0


class TestEquationUnique(OptimizationEquationTest):
    def test_equation_unique(
        self,
        run: ixmp4.Run,
    ) -> None:
        with run.transact("Equation not unique"):
            run.optimization.equations.create("Equation")

            with pytest.raises(ixmp4.optimization.Equation.NotUnique):
                run.optimization.equations.create("Equation")


class TestEquationNotFound(OptimizationEquationTest):
    def test_equation_not_found(
        self,
        run: ixmp4.Run,
    ) -> None:
        with pytest.raises(ixmp4.optimization.Equation.NotFound):
            run.optimization.equations.get_by_name("Equation")


class TestEquationCreateInvalidArguments(OptimizationEquationTest):
    def test_equation_create_invalid_args(
        self,
        run: ixmp4.Run,
        indexset: ixmp4.optimization.IndexSet,
        fake_time: datetime.datetime,
    ) -> None:
        with run.transact("Invalid arguments"):
            with pytest.raises(
                OptimizationItemUsageError,
                match="Received `column_names` to name columns, but no "
                "`constrained_to_indexsets`",
            ):
                run.optimization.equations.create("Equation", column_names=["Column 1"])

            with pytest.raises(OptimizationItemUsageError, match="not equal in length"):
                run.optimization.equations.create(
                    "Equation",
                    constrained_to_indexsets=[indexset.name],
                    column_names=["Column 1", "Column 2"],
                )

            with pytest.raises(
                OptimizationItemUsageError, match="`column_names` are not unique"
            ):
                run.optimization.equations.create(
                    "Equation",
                    constrained_to_indexsets=[indexset.name, indexset.name],
                    column_names=["Column 1", "Column 1"],
                )


class EquationDataTest(OptimizationEquationTest):
    @pytest.fixture(scope="class")
    def test_data_indexsets(self, run: ixmp4.Run) -> list[ixmp4.optimization.IndexSet]:
        with run.transact("Create indexsets"):
            indexset1 = run.optimization.indexsets.create("IndexSet 1")
            indexset2 = run.optimization.indexsets.create("IndexSet 2")
            indexset1.add_data(["do", "re", "mi", "fa", "so", "la", "ti"])
            indexset2.add_data([3, 1, 4])

        return [indexset1, indexset2]

    def test_equation_add_data(
        self,
        run: ixmp4.Run,
        test_data_indexsets: list[ixmp4.optimization.IndexSet],
        column_names: list[str] | None,
        test_data: dict[str, list[Any]] | pd.DataFrame,
        fake_time: datetime.datetime,
    ) -> None:
        with run.transact("Create equation and add data"):
            equation = run.optimization.equations.create(
                "Equation",
                constrained_to_indexsets=[i.name for i in test_data_indexsets],
                column_names=column_names,
            )
            equation.add_data(test_data)

        if isinstance(test_data, pd.DataFrame):
            test_data = test_data.to_dict(orient="list")

        assert equation.data == test_data

    def test_equation_remove_data_partial(
        self,
        run: ixmp4.Run,
        partial_test_data: dict[str, list[Any]] | pd.DataFrame,
        remaining_test_data: dict[str, list[Any]],
        fake_time: datetime.datetime,
    ) -> None:
        with run.transact("Remove equation data partial"):
            equation = run.optimization.equations.get_by_name("Equation")
            equation.remove_data(partial_test_data)

        assert equation.data == remaining_test_data

    def test_equation_remove_data_full(
        self,
        run: ixmp4.Run,
        fake_time: datetime.datetime,
    ) -> None:
        with run.transact("Remove equation data full"):
            equation = run.optimization.equations.get_by_name("Equation")
            equation.remove_data()

        assert equation.data == {}


class TestEquationData(EquationDataTest):
    @pytest.fixture(scope="class")
    def column_names(
        self,
    ) -> list[str] | None:
        return None

    @pytest.fixture(scope="class")
    def test_data(self) -> dict[str, list[Any]] | pd.DataFrame:
        return {
            "marginals": [-2, 1, 1],
            "levels": [2, 1, 3],
            "IndexSet 1": ["do", "re", "mi"],
            "IndexSet 2": [3, 3, 1],
        }

    @pytest.fixture(scope="class")
    def partial_test_data(self) -> dict[str, list[Any]] | pd.DataFrame:
        return {
            "IndexSet 1": ["re", "mi"],
            "IndexSet 2": [3, 1],
        }

    @pytest.fixture(scope="class")
    def remaining_test_data(self) -> dict[str, list[Any]] | pd.DataFrame:
        return {
            "marginals": [-2],
            "levels": [2],
            "IndexSet 1": ["do"],
            "IndexSet 2": [3],
        }


class TestEquationDataWithColumnNames(EquationDataTest):
    @pytest.fixture(scope="class")
    def column_names(self) -> list[str] | None:
        return ["Column 1", "Column 2"]

    @pytest.fixture(scope="class")
    def test_data(self) -> dict[str, list[Any]] | pd.DataFrame:
        return {
            "marginals": [-2, 1, 1],
            "levels": [2, 1, 3],
            "Column 1": ["do", "re", "mi"],
            "Column 2": [3, 3, 1],
        }

    @pytest.fixture(scope="class")
    def partial_test_data(self) -> dict[str, list[Any]] | pd.DataFrame:
        return {
            "Column 1": ["re", "mi"],
            "Column 2": [3, 1],
        }

    @pytest.fixture(scope="class")
    def remaining_test_data(self) -> dict[str, list[Any]] | pd.DataFrame:
        return {
            "marginals": [-2],
            "levels": [2],
            "Column 1": ["do"],
            "Column 2": [3],
        }


class TestEquationDataDataFrame(EquationDataTest):
    @pytest.fixture(scope="class")
    def column_names(
        self,
    ) -> list[str] | None:
        return None

    @pytest.fixture(scope="class")
    def test_data(self) -> dict[str, list[Any]] | pd.DataFrame:
        return pd.DataFrame(
            [
                [-2, 2, "do", 3],
                [1, 1, "re", 3],
                [1, 3, "mi", 1],
            ],
            columns=["marginals", "levels", "IndexSet 1", "IndexSet 2"],
        )

    @pytest.fixture(scope="class")
    def partial_test_data(self) -> dict[str, list[Any]] | pd.DataFrame:
        return pd.DataFrame(
            [
                ["re", 3],
                ["mi", 1],
            ],
            columns=["IndexSet 1", "IndexSet 2"],
        )

    @pytest.fixture(scope="class")
    def remaining_test_data(self) -> dict[str, list[Any]]:
        return {
            "marginals": [-2],
            "levels": [2],
            "IndexSet 1": ["do"],
            "IndexSet 2": [3],
        }


class TestEquationInvalidData(OptimizationEquationTest):
    def test_equations_create(self, run: ixmp4.Run):
        with run.transact("Create indexsets"):
            indexset1 = run.optimization.indexsets.create("IndexSet 1")
            indexset2 = run.optimization.indexsets.create("IndexSet 2")
            indexset1.add_data(["do", "re", "mi", "fa", "so", "la", "ti"])
            indexset2.add_data([3, 1, 4])

        with run.transact("Create equations"):
            equation1 = run.optimization.equations.create(
                "Equation 1",
                constrained_to_indexsets=["IndexSet 1", "IndexSet 2"],
            )
            assert equation1.id == 1

            equation2 = run.optimization.equations.create(
                "Equation 2",
            )
            assert equation2.id == 2

    def test_equation_add_invalid_data(self, run: ixmp4.Run) -> None:
        equation1 = run.optimization.equations.get_by_name("Equation 1")

        with pytest.raises(
            OptimizationItemUsageError,
            match=r"Equation.data must include the column\(s\): levels!",
        ):
            with run.transact("Add invalid data"):
                equation1.add_data(
                    {
                        "marginals": [-2],
                        "IndexSet 1": ["do"],
                        "IndexSet 2": [3],
                    }
                )
        with pytest.raises(
            OptimizationItemUsageError,
            match=r"Equation.data must include the column\(s\): marginals!",
        ):
            with run.transact("Add invalid data"):
                equation1.add_data(
                    {
                        "levels": [2],
                        "IndexSet 1": ["do"],
                        "IndexSet 2": [3],
                    }
                )

        with pytest.raises(
            OptimizationItemUsageError,
            match=r"Equation.data must include the column\(s\): levels, marginals!",
        ):
            with run.transact("Add invalid data"):
                equation1.add_data(
                    {
                        "IndexSet 1": ["do"],
                        "IndexSet 2": [3],
                    }
                )

        with pytest.raises(
            ixmp4.optimization.Equation.DataInvalid,
            match="All arrays must be of the same length",
        ):
            with run.transact("Add invalid data"):
                equation1.add_data(
                    {
                        "marginals": [-2, 1, 1],
                        "levels": [2, 1, 3],
                        "IndexSet 1": ["do", "re"],  # missing "mi"
                        "IndexSet 2": [3, 3, 1],
                    }
                )
        with pytest.raises(
            ixmp4.optimization.Equation.DataInvalid,
            match="All arrays must be of the same length",
        ):
            with run.transact("Add invalid data"):
                equation1.add_data(
                    {
                        "marginals": [
                            -2,
                        ],  # missing 1,1
                        "levels": [2, 1, 3],
                        "IndexSet 1": ["do", "re", "mi"],
                        "IndexSet 2": [3, 3, 1],
                    },
                )

        with pytest.raises(
            ixmp4.optimization.Equation.DataInvalid, match="contains duplicate rows"
        ):
            with run.transact("Add invalid data"):
                equation1.add_data(
                    {
                        "marginals": [-2, 1, 1],
                        "levels": [2, 1, 3],
                        "IndexSet 1": ["do", "do", "mi"],
                        "IndexSet 2": [3, 3, 1],
                    },
                )

    def test_equation_remove_invalid_data(self, run: ixmp4.Run) -> None:
        equation1 = run.optimization.equations.get_by_name("Equation 1")

        with pytest.raises(
            OptimizationItemUsageError, match="data to be removed must specify"
        ):
            with run.transact("Remove invalid data"):
                equation1.remove_data({"IndexSet 1": ["do"]})
        with pytest.raises(
            OptimizationItemUsageError, match="data to be removed must specify"
        ):
            with run.transact("Remove invalid data"):
                equation1.remove_data({"levels": [2]})

        # move tests and logic to facade TODO
        # caplog.clear()
        # with caplog.at_level("WARNING"):
        #     service.remove_data(
        #         2,
        #         {
        #             "levels": [2],
        #             "marginals": [-2],
        #         },
        #     )

        # expected = [
        #     (
        #         "Trying to remove {'levels': [2], 'marginals': [-2]} from `Equation` "
        #         "'Equation 2', but that is not indexed; not removing anything!"
        #     )
        # ]
        # assert caplog.messages == expected


class TestEquationRunLock(OptimizationEquationTest):
    def test_equation_requires_lock(
        self, run: ixmp4.Run, indexset: ixmp4.optimization.IndexSet
    ) -> None:
        with pytest.raises(ixmp4.Run.LockRequired):
            run.optimization.equations.create(
                "Equation", constrained_to_indexsets=[indexset.name]
            )

        with run.transact("Create equation"):
            equation = run.optimization.equations.create(
                "Equation", constrained_to_indexsets=[indexset.name]
            )

        with pytest.raises(ixmp4.Run.LockRequired):
            equation.add_data({"marginals": [1], "levels": [2], "IndexSet": ["fa"]})

        with pytest.raises(ixmp4.Run.LockRequired):
            equation.remove_data({"IndexSet": ["fa"]})

        with pytest.raises(ixmp4.Run.LockRequired):
            equation.delete()

        with pytest.raises(ixmp4.Run.LockRequired):
            run.optimization.equations.delete(equation.id)


class TestEquationDocs(OptimizationEquationTest):
    def test_create_docs_via_func(self, run: ixmp4.Run) -> None:
        with run.transact("Create equation 1"):
            equation1 = run.optimization.equations.create("Equation 1")

        equation1_docs1 = run.optimization.equations.set_docs(
            "Equation 1", "Description of Equation 1"
        )
        equation1_docs2 = run.optimization.equations.get_docs("Equation 1")

        assert equation1_docs1 == equation1_docs2
        assert equation1.docs == equation1_docs1

    def test_create_docs_via_object(self, run: ixmp4.Run) -> None:
        with run.transact("Create equation 2"):
            equation2 = run.optimization.equations.create("Equation 2")
        equation2.docs = "Description of Equation 2"

        assert run.optimization.equations.get_docs("Equation 2") == equation2.docs

    def test_create_docs_via_setattr(self, run: ixmp4.Run) -> None:
        with run.transact("Create equation 3"):
            equation3 = run.optimization.equations.create("Equation 3")
        setattr(equation3, "docs", "Description of Equation 3")

        assert run.optimization.equations.get_docs("Equation 3") == equation3.docs

    def test_list_docs(self, run: ixmp4.Run) -> None:
        assert run.optimization.equations.list_docs() == [
            "Description of Equation 1",
            "Description of Equation 2",
            "Description of Equation 3",
        ]

        assert run.optimization.equations.list_docs(id=3) == [
            "Description of Equation 3"
        ]

        assert run.optimization.equations.list_docs(id__in=[1]) == [
            "Description of Equation 1"
        ]

    def test_delete_docs_via_func(self, run: ixmp4.Run) -> None:
        equation1 = run.optimization.equations.get_by_name("Equation 1")
        run.optimization.equations.delete_docs("Equation 1")
        equation1 = run.optimization.equations.get_by_name("Equation 1")
        assert equation1.docs is None

    def test_delete_docs_set_none(self, run: ixmp4.Run) -> None:
        equation2 = run.optimization.equations.get_by_name("Equation 2")
        equation2.docs = None
        equation2 = run.optimization.equations.get_by_name("Equation 2")
        assert equation2.docs is None

    def test_delete_docs_del(self, run: ixmp4.Run) -> None:
        equation3 = run.optimization.equations.get_by_name("Equation 3")
        del equation3.docs
        equation3 = run.optimization.equations.get_by_name("Equation 3")
        assert equation3.docs is None

    def test_docs_empty(self, run: ixmp4.Run) -> None:
        assert len(run.optimization.equations.list_docs()) == 0


class TestEquationRollback(OptimizationEquationTest):
    def test_equation_add_data_failure(
        self, run: ixmp4.Run, indexset: ixmp4.optimization.IndexSet
    ):
        with run.transact("Add equation data"):
            equation = run.optimization.equations.create(
                "Equation", constrained_to_indexsets=[indexset.name]
            )
            equation.add_data(
                {
                    "marginals": [-2, 1, 1],
                    "levels": [2, 1, 3],
                    "IndexSet": ["do", "re", "mi"],
                }
            )

        try:
            with run.transact("Update equation data failure"):
                equation.add_data(
                    {
                        "marginals": [1, 1],
                        "levels": [4, 5],
                        "IndexSet": ["fa", "so"],
                    }
                )
                raise CustomException
        except CustomException:
            pass

    def test_equation_versioning_after_add_data_failure(
        self, versioning_platform: ixmp4.Platform, run: ixmp4.Run
    ):
        equation = run.optimization.equations.get_by_name("Equation")
        assert equation.data == {
            "marginals": [-2, 1, 1],
            "levels": [2, 1, 3],
            "IndexSet": ["do", "re", "mi"],
        }

    def test_equation_non_versioning_after_add_data_failure(
        self, non_versioning_platform: ixmp4.Platform, run: ixmp4.Run
    ):
        equation = run.optimization.equations.get_by_name("Equation")
        assert equation.data == {
            "marginals": [-2, 1, 1, 1, 1],
            "levels": [2, 4, 3, 1, 5],
            "IndexSet": ["do", "fa", "mi", "re", "so"],
        }

    def test_equation_remove_data_failure(self, run: ixmp4.Run):
        equation = run.optimization.equations.get_by_name("Equation")

        with run.transact("Remove equation data"):
            equation.remove_data(
                {
                    "marginals": [1, 1],
                    "levels": [4, 5],
                    "IndexSet": ["fa", "so"],
                }
            )

        try:
            with run.transact("Remove equation data failure"):
                equation.remove_data({"IndexSet": ["do", "re"]})
                raise CustomException
        except CustomException:
            pass

    def test_equation_versioning_after_remove_data_failure(
        self, versioning_platform: ixmp4.Platform, run: ixmp4.Run
    ):
        equation = run.optimization.equations.get_by_name("Equation")
        assert equation.data == {
            "marginals": [-2, 1, 1],
            "levels": [2, 1, 3],
            "IndexSet": ["do", "re", "mi"],
        }

    def test_equation_non_versioning_after_remove_data_failure(
        self, non_versioning_platform: ixmp4.Platform, run: ixmp4.Run
    ):
        equation = run.optimization.equations.get_by_name("Equation")
        assert equation.data == {
            "marginals": [1],
            "levels": [3],
            "IndexSet": ["mi"],
        }

    def test_equation_docs_failure(self, run: ixmp4.Run):
        equation = run.optimization.equations.get_by_name("Equation")

        try:
            with run.transact("Set equation docs failure"):
                equation.docs = "These docs should persist!"
                raise CustomException
        except CustomException:
            pass

    def test_equation_after_docs_failure(
        self, platform: ixmp4.Platform, run: ixmp4.Run
    ):
        equation = run.optimization.equations.get_by_name("Equation")
        assert equation.docs == "These docs should persist!"

    def test_equation_delete_failure(
        self, run: ixmp4.Run, indexset: ixmp4.optimization.IndexSet
    ):
        equation = run.optimization.equations.get_by_name("Equation")

        try:
            with run.transact("Delete equation failure"):
                equation.delete()
                indexset.delete()
                raise CustomException
        except CustomException:
            pass

    def test_equation_versioning_after_delete_failure(
        self, versioning_platform: ixmp4.Platform, run: ixmp4.Run
    ):
        equation = run.optimization.equations.get_by_name("Equation")
        assert equation.id == 1

    def test_equation_non_versioning_after_delete_failure(
        self, non_versioning_platform: ixmp4.Platform, run: ixmp4.Run
    ):
        with pytest.raises(ixmp4.optimization.Equation.NotFound):
            run.optimization.equations.get_by_name("Equation")
