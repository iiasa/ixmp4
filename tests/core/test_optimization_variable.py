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


class OptimizationVariableTest(PlatformTest):
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


class TestVariable(OptimizationVariableTest):
    def test_create_variable(
        self,
        run: ixmp4.Run,
        indexset: ixmp4.optimization.IndexSet,
        fake_time: datetime.datetime,
    ) -> None:
        with run.transact("Create variables"):
            variable1 = run.optimization.variables.create("Variable 1")

            variable2 = run.optimization.variables.create(
                "Variable 2", constrained_to_indexsets=["IndexSet"]
            )
            variable3 = run.optimization.variables.create(
                "Variable 3",
                constrained_to_indexsets=["IndexSet"],
                column_names=["Column"],
            )

            variable4 = run.optimization.variables.create(
                "Variable 4",
                constrained_to_indexsets=["IndexSet", "IndexSet"],
                column_names=["Column 1", "Column 2"],
            )

        assert variable1.id == 1
        assert variable1.run_id == run.id
        assert variable1.name == "Variable 1"
        assert variable1.data == {}
        assert variable1.indexset_names is None
        assert variable1.column_names is None
        assert variable1.levels == []
        assert variable1.marginals == []
        assert variable1.created_by == "@unknown"
        assert variable1.created_at == fake_time.replace(tzinfo=None)

        assert variable2.id == 2
        assert variable2.indexset_names == ["IndexSet"]
        assert variable2.column_names is None

        assert variable3.id == 3
        assert variable3.indexset_names == ["IndexSet"]
        assert variable3.column_names == ["Column"]

        assert variable4.id == 4
        assert variable4.indexset_names == ["IndexSet", "IndexSet"]
        assert variable4.column_names == ["Column 1", "Column 2"]

    def test_tabulate_variable(self, run: ixmp4.Run) -> None:
        ret_df = run.optimization.variables.tabulate()
        assert len(ret_df) == 4
        assert "id" in ret_df.columns
        assert "name" in ret_df.columns
        assert "data" in ret_df.columns
        assert "created_at" in ret_df.columns
        assert "created_by" in ret_df.columns

        assert "run__id" not in ret_df.columns

    def test_list_variable(self, run: ixmp4.Run) -> None:
        assert len(run.optimization.variables.list()) == 4

    def test_delete_variable_via_func_obj(self, run: ixmp4.Run) -> None:
        with run.transact("Delete variable 1"):
            variable1 = run.optimization.variables.get_by_name("Variable 1")
            run.optimization.variables.delete(variable1)

    def test_delete_variable_via_func_id(self, run: ixmp4.Run) -> None:
        with run.transact("Delete variable 2"):
            run.optimization.variables.delete(2)

    def test_delete_variable_via_func_name(self, run: ixmp4.Run) -> None:
        with run.transact("Delete variable 3"):
            run.optimization.variables.delete("Variable 3")

    def test_delete_variable_via_obj(self, run: ixmp4.Run) -> None:
        variable4 = run.optimization.variables.get_by_name("Variable 4")
        with run.transact("Delete variable 4"):
            variable4.delete()

    def test_variable_empty(self, run: ixmp4.Run) -> None:
        assert run.optimization.variables.tabulate().empty
        assert len(run.optimization.variables.list()) == 0


class TestVariableUnique(OptimizationVariableTest):
    def test_variable_unique(
        self,
        run: ixmp4.Run,
    ) -> None:
        with run.transact("Variable not unique"):
            run.optimization.variables.create("Variable")

            with pytest.raises(ixmp4.optimization.Variable.NotUnique):
                run.optimization.variables.create("Variable")


class TestVariableNotFound(OptimizationVariableTest):
    def test_variable_not_found(
        self,
        run: ixmp4.Run,
    ) -> None:
        with pytest.raises(ixmp4.optimization.Variable.NotFound):
            run.optimization.variables.get_by_name("Variable")


class TestVariableCreateInvalidArguments(OptimizationVariableTest):
    def test_variable_create_invalid_args(
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
                run.optimization.variables.create("Variable", column_names=["Column 1"])

            with pytest.raises(OptimizationItemUsageError, match="not equal in length"):
                run.optimization.variables.create(
                    "Variable",
                    constrained_to_indexsets=[indexset.name],
                    column_names=["Column 1", "Column 2"],
                )

            with pytest.raises(
                OptimizationItemUsageError, match="`column_names` are not unique"
            ):
                run.optimization.variables.create(
                    "Variable",
                    constrained_to_indexsets=[indexset.name, indexset.name],
                    column_names=["Column 1", "Column 1"],
                )


class VariableDataTest(OptimizationVariableTest):
    @pytest.fixture(scope="class")
    def test_data_indexsets(self, run: ixmp4.Run) -> list[ixmp4.optimization.IndexSet]:
        with run.transact("Create indexsets"):
            indexset1 = run.optimization.indexsets.create("IndexSet 1")
            indexset2 = run.optimization.indexsets.create("IndexSet 2")
            indexset1.add_data(["do", "re", "mi", "fa", "so", "la", "ti"])
            indexset2.add_data([3, 1, 4])

        return [indexset1, indexset2]

    def test_variable_add_data(
        self,
        run: ixmp4.Run,
        test_data_indexsets: list[ixmp4.optimization.IndexSet],
        column_names: list[str] | None,
        test_data: dict[str, list[Any]] | pd.DataFrame,
        fake_time: datetime.datetime,
    ) -> None:
        with run.transact("Create variable and add data"):
            variable = run.optimization.variables.create(
                "Variable",
                constrained_to_indexsets=[i.name for i in test_data_indexsets],
                column_names=column_names,
            )
            variable.add_data(test_data)

        if isinstance(test_data, pd.DataFrame):
            test_data = test_data.to_dict(orient="list")

        assert variable.data == test_data

    def test_variable_remove_data_partial(
        self,
        run: ixmp4.Run,
        partial_test_data: dict[str, list[Any]] | pd.DataFrame,
        remaining_test_data: dict[str, list[Any]],
        fake_time: datetime.datetime,
    ) -> None:
        with run.transact("Remove variable data partial"):
            variable = run.optimization.variables.get_by_name("Variable")
            variable.remove_data(partial_test_data)

        assert variable.data == remaining_test_data

    def test_variable_remove_data_full(
        self,
        run: ixmp4.Run,
        fake_time: datetime.datetime,
    ) -> None:
        with run.transact("Remove variable data full"):
            variable = run.optimization.variables.get_by_name("Variable")
            variable.remove_data()

        assert variable.data == {}


class TestVariableData(VariableDataTest):
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


class TestVariableDataWithColumnNames(VariableDataTest):
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


class TestVariableDataDataFrame(VariableDataTest):
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


class TestVariableInvalidData(OptimizationVariableTest):
    def test_variables_create(self, run: ixmp4.Run) -> None:
        with run.transact("Create indexsets"):
            indexset1 = run.optimization.indexsets.create("IndexSet 1")
            indexset2 = run.optimization.indexsets.create("IndexSet 2")
            indexset1.add_data(["do", "re", "mi", "fa", "so", "la", "ti"])
            indexset2.add_data([3, 1, 4])

        with run.transact("Create variables"):
            variable1 = run.optimization.variables.create(
                "Variable 1",
                constrained_to_indexsets=["IndexSet 1", "IndexSet 2"],
            )
            assert variable1.id == 1

            variable2 = run.optimization.variables.create(
                "Variable 2",
            )
            assert variable2.id == 2

    def test_variable_add_invalid_data(self, run: ixmp4.Run) -> None:
        variable1 = run.optimization.variables.get_by_name("Variable 1")

        with pytest.raises(
            OptimizationItemUsageError,
            match=r"Variable.data must include the column\(s\): levels!",
        ):
            with run.transact("Add invalid data"):
                variable1.add_data(
                    {
                        "marginals": [-2],
                        "IndexSet 1": ["do"],
                        "IndexSet 2": [3],
                    }
                )
        with pytest.raises(
            OptimizationItemUsageError,
            match=r"Variable.data must include the column\(s\): marginals!",
        ):
            with run.transact("Add invalid data"):
                variable1.add_data(
                    {
                        "levels": [2],
                        "IndexSet 1": ["do"],
                        "IndexSet 2": [3],
                    }
                )

        with pytest.raises(
            OptimizationItemUsageError,
            match=r"Variable.data must include the column\(s\): levels, marginals!",
        ):
            with run.transact("Add invalid data"):
                variable1.add_data(
                    {
                        "IndexSet 1": ["do"],
                        "IndexSet 2": [3],
                    }
                )

        with pytest.raises(
            ixmp4.optimization.Variable.DataInvalid,
            match="All arrays must be of the same length",
        ):
            with run.transact("Add invalid data"):
                variable1.add_data(
                    {
                        "marginals": [-2, 1, 1],
                        "levels": [2, 1, 3],
                        "IndexSet 1": ["do", "re"],  # missing "mi"
                        "IndexSet 2": [3, 3, 1],
                    }
                )
        with pytest.raises(
            ixmp4.optimization.Variable.DataInvalid,
            match="All arrays must be of the same length",
        ):
            with run.transact("Add invalid data"):
                variable1.add_data(
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
            ixmp4.optimization.Variable.DataInvalid, match="contains duplicate rows"
        ):
            with run.transact("Add invalid data"):
                variable1.add_data(
                    {
                        "marginals": [-2, 1, 1],
                        "levels": [2, 1, 3],
                        "IndexSet 1": ["do", "do", "mi"],
                        "IndexSet 2": [3, 3, 1],
                    },
                )

    def test_variable_remove_invalid_data(self, run: ixmp4.Run) -> None:
        variable1 = run.optimization.variables.get_by_name("Variable 1")

        with pytest.raises(
            OptimizationItemUsageError, match="data to be removed must specify"
        ):
            with run.transact("Remove invalid data"):
                variable1.remove_data({"IndexSet 1": ["do"]})
        with pytest.raises(
            OptimizationItemUsageError, match="data to be removed must specify"
        ):
            with run.transact("Remove invalid data"):
                variable1.remove_data({"levels": [2]})

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
        #         "Trying to remove {'levels': [2], 'marginals': [-2]} from `Variable` "
        #         "'Variable 2', but that is not indexed; not removing anything!"
        #     )
        # ]
        # assert caplog.messages == expected


class TestVariableRunLock(OptimizationVariableTest):
    def test_variable_requires_lock(
        self, run: ixmp4.Run, indexset: ixmp4.optimization.IndexSet
    ) -> None:
        with pytest.raises(ixmp4.Run.LockRequired):
            run.optimization.variables.create(
                "Variable", constrained_to_indexsets=[indexset.name]
            )

        with run.transact("Create variable"):
            variable = run.optimization.variables.create(
                "Variable", constrained_to_indexsets=[indexset.name]
            )

        with pytest.raises(ixmp4.Run.LockRequired):
            variable.add_data({"marginals": [1], "levels": [2], "IndexSet": ["fa"]})

        with pytest.raises(ixmp4.Run.LockRequired):
            variable.remove_data({"IndexSet": ["fa"]})

        with pytest.raises(ixmp4.Run.LockRequired):
            variable.delete()

        with pytest.raises(ixmp4.Run.LockRequired):
            run.optimization.variables.delete(variable.id)


class TestVariableDocs(OptimizationVariableTest):
    def test_create_docs_via_func(self, run: ixmp4.Run) -> None:
        with run.transact("Create variable 1"):
            variable1 = run.optimization.variables.create("Variable 1")

        variable1_docs1 = run.optimization.variables.set_docs(
            "Variable 1", "Description of Variable 1"
        )
        variable1_docs2 = run.optimization.variables.get_docs("Variable 1")

        assert variable1_docs1 == variable1_docs2
        assert variable1.docs == variable1_docs1

    def test_create_docs_via_object(self, run: ixmp4.Run) -> None:
        with run.transact("Create variable 2"):
            variable2 = run.optimization.variables.create("Variable 2")
        variable2.docs = "Description of Variable 2"

        assert run.optimization.variables.get_docs("Variable 2") == variable2.docs

    def test_create_docs_via_setattr(self, run: ixmp4.Run) -> None:
        with run.transact("Create variable 3"):
            variable3 = run.optimization.variables.create("Variable 3")
        setattr(variable3, "docs", "Description of Variable 3")

        assert run.optimization.variables.get_docs("Variable 3") == variable3.docs

    def test_list_docs(self, run: ixmp4.Run) -> None:
        assert run.optimization.variables.list_docs() == [
            "Description of Variable 1",
            "Description of Variable 2",
            "Description of Variable 3",
        ]

        assert run.optimization.variables.list_docs(id=3) == [
            "Description of Variable 3"
        ]

        assert run.optimization.variables.list_docs(id__in=[1]) == [
            "Description of Variable 1"
        ]

    def test_delete_docs_via_func(self, run: ixmp4.Run) -> None:
        variable1 = run.optimization.variables.get_by_name("Variable 1")
        run.optimization.variables.delete_docs("Variable 1")
        variable1 = run.optimization.variables.get_by_name("Variable 1")
        assert variable1.docs is None

    def test_delete_docs_set_none(self, run: ixmp4.Run) -> None:
        variable2 = run.optimization.variables.get_by_name("Variable 2")
        variable2.docs = None
        variable2 = run.optimization.variables.get_by_name("Variable 2")
        assert variable2.docs is None

    def test_delete_docs_del(self, run: ixmp4.Run) -> None:
        variable3 = run.optimization.variables.get_by_name("Variable 3")
        del variable3.docs
        variable3 = run.optimization.variables.get_by_name("Variable 3")
        assert variable3.docs is None

    def test_docs_empty(self, run: ixmp4.Run) -> None:
        assert len(run.optimization.variables.list_docs()) == 0


class TestVariableRollback(OptimizationVariableTest):
    def test_variable_add_data_failure(
        self, run: ixmp4.Run, indexset: ixmp4.optimization.IndexSet
    ) -> None:
        with run.transact("Add variable data"):
            variable = run.optimization.variables.create(
                "Variable", constrained_to_indexsets=[indexset.name]
            )
            variable.add_data(
                {
                    "marginals": [-2, 1, 1],
                    "levels": [2, 1, 3],
                    "IndexSet": ["do", "re", "mi"],
                }
            )

        try:
            with run.transact("Update variable data failure"):
                variable.add_data(
                    {
                        "marginals": [1, 1],
                        "levels": [4, 5],
                        "IndexSet": ["fa", "so"],
                    }
                )
                raise CustomException
        except CustomException:
            pass

    def test_variable_versioning_after_add_data_failure(
        self, versioning_platform: ixmp4.Platform, run: ixmp4.Run
    ) -> None:
        variable = run.optimization.variables.get_by_name("Variable")
        assert variable.data == {
            "marginals": [-2, 1, 1],
            "levels": [2, 1, 3],
            "IndexSet": ["do", "re", "mi"],
        }

    def test_variable_non_versioning_after_add_data_failure(
        self, non_versioning_platform: ixmp4.Platform, run: ixmp4.Run
    ) -> None:
        variable = run.optimization.variables.get_by_name("Variable")
        assert variable.data == {
            "marginals": [-2, 1, 1, 1, 1],
            "levels": [2, 4, 3, 1, 5],
            "IndexSet": ["do", "fa", "mi", "re", "so"],
        }

    def test_variable_remove_data_failure(self, run: ixmp4.Run) -> None:
        variable = run.optimization.variables.get_by_name("Variable")

        with run.transact("Remove variable data"):
            variable.remove_data(
                {
                    "marginals": [1, 1],
                    "levels": [4, 5],
                    "IndexSet": ["fa", "so"],
                }
            )

        try:
            with run.transact("Remove variable data failure"):
                variable.remove_data({"IndexSet": ["do", "re"]})
                raise CustomException
        except CustomException:
            pass

    def test_variable_versioning_after_remove_data_failure(
        self, versioning_platform: ixmp4.Platform, run: ixmp4.Run
    ) -> None:
        variable = run.optimization.variables.get_by_name("Variable")
        assert variable.data == {
            "marginals": [-2, 1, 1],
            "levels": [2, 1, 3],
            "IndexSet": ["do", "re", "mi"],
        }

    def test_variable_non_versioning_after_remove_data_failure(
        self, non_versioning_platform: ixmp4.Platform, run: ixmp4.Run
    ) -> None:
        variable = run.optimization.variables.get_by_name("Variable")
        assert variable.data == {
            "marginals": [1],
            "levels": [3],
            "IndexSet": ["mi"],
        }

    def test_variable_docs_failure(self, run: ixmp4.Run) -> None:
        variable = run.optimization.variables.get_by_name("Variable")

        try:
            with run.transact("Set variable docs failure"):
                variable.docs = "These docs should persist!"
                raise CustomException
        except CustomException:
            pass

    def test_variable_after_docs_failure(
        self, platform: ixmp4.Platform, run: ixmp4.Run
    ) -> None:
        variable = run.optimization.variables.get_by_name("Variable")
        assert variable.docs == "These docs should persist!"

    def test_variable_delete_failure(
        self, run: ixmp4.Run, indexset: ixmp4.optimization.IndexSet
    ) -> None:
        variable = run.optimization.variables.get_by_name("Variable")

        try:
            with run.transact("Delete variable failure"):
                variable.delete()
                indexset.delete()
                raise CustomException
        except CustomException:
            pass

    def test_variable_versioning_after_delete_failure(
        self, versioning_platform: ixmp4.Platform, run: ixmp4.Run
    ) -> None:
        variable = run.optimization.variables.get_by_name("Variable")
        assert variable.id == 1

    def test_variable_non_versioning_after_delete_failure(
        self, non_versioning_platform: ixmp4.Platform, run: ixmp4.Run
    ) -> None:
        with pytest.raises(ixmp4.optimization.Variable.NotFound):
            run.optimization.variables.get_by_name("Variable")
