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


class OptimizationParameterTest(PlatformTest):
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


class TestParameter(OptimizationParameterTest):
    def test_create_parameter(
        self,
        run: ixmp4.Run,
        indexset: ixmp4.optimization.IndexSet,
        fake_time: datetime.datetime,
    ) -> None:
        with run.transact("Create parameters"):
            parameter1 = run.optimization.parameters.create(
                "Parameter 1", constrained_to_indexsets=["IndexSet"]
            )

            parameter2 = run.optimization.parameters.create(
                "Parameter 2", constrained_to_indexsets=["IndexSet"]
            )
            parameter3 = run.optimization.parameters.create(
                "Parameter 3",
                constrained_to_indexsets=["IndexSet"],
                column_names=["Column"],
            )

            parameter4 = run.optimization.parameters.create(
                "Parameter 4",
                constrained_to_indexsets=["IndexSet", "IndexSet"],
                column_names=["Column 1", "Column 2"],
            )

        assert parameter1.id == 1
        assert parameter1.run_id == run.id
        assert parameter1.name == "Parameter 1"
        assert parameter1.data == {}
        assert parameter1.indexset_names == ["IndexSet"]
        assert parameter1.column_names is None
        assert parameter1.values == []
        assert parameter1.units == []
        assert parameter1.created_by == "@unknown"
        assert parameter1.created_at == fake_time.replace(tzinfo=None)

        assert parameter2.id == 2
        assert parameter2.indexset_names == ["IndexSet"]
        assert parameter2.column_names is None

        assert parameter3.id == 3
        assert parameter3.indexset_names == ["IndexSet"]
        assert parameter3.column_names == ["Column"]

        assert parameter4.id == 4
        assert parameter4.indexset_names == ["IndexSet", "IndexSet"]
        assert parameter4.column_names == ["Column 1", "Column 2"]

    def test_tabulate_parameter(self, run: ixmp4.Run) -> None:
        ret_df = run.optimization.parameters.tabulate()
        assert len(ret_df) == 4
        assert "id" in ret_df.columns
        assert "name" in ret_df.columns
        assert "data" in ret_df.columns
        assert "created_at" in ret_df.columns
        assert "created_by" in ret_df.columns

        assert "run__id" not in ret_df.columns

    def test_list_parameter(self, run: ixmp4.Run) -> None:
        assert len(run.optimization.parameters.list()) == 4

    def test_delete_parameter_via_func_obj(self, run: ixmp4.Run) -> None:
        with run.transact("Delete parameter 1"):
            parameter1 = run.optimization.parameters.get_by_name("Parameter 1")
            run.optimization.parameters.delete(parameter1)

    def test_delete_parameter_via_func_id(self, run: ixmp4.Run) -> None:
        with run.transact("Delete parameter 2"):
            run.optimization.parameters.delete(2)

    def test_delete_parameter_via_func_name(self, run: ixmp4.Run) -> None:
        with run.transact("Delete parameter 3"):
            run.optimization.parameters.delete("Parameter 3")

    def test_delete_parameter_via_obj(self, run: ixmp4.Run) -> None:
        parameter4 = run.optimization.parameters.get_by_name("Parameter 4")
        with run.transact("Delete parameter 4"):
            parameter4.delete()

    def test_parameter_empty(self, run: ixmp4.Run) -> None:
        assert run.optimization.parameters.tabulate().empty
        assert len(run.optimization.parameters.list()) == 0


class TestParameterUnique(OptimizationParameterTest):
    def test_parameter_unique(
        self,
        run: ixmp4.Run,
        indexset: ixmp4.optimization.IndexSet,
    ) -> None:
        with run.transact("Parameter not unique"):
            run.optimization.parameters.create(
                "Parameter", constrained_to_indexsets=[indexset.name]
            )

            with pytest.raises(ixmp4.optimization.Parameter.NotUnique):
                run.optimization.parameters.create(
                    "Parameter", constrained_to_indexsets=[indexset.name]
                )


class TestParameterNotFound(OptimizationParameterTest):
    def test_parameter_not_found(
        self,
        run: ixmp4.Run,
    ) -> None:
        with pytest.raises(ixmp4.optimization.Parameter.NotFound):
            run.optimization.parameters.get_by_name("Parameter")


class TestParameterCreateInvalidArguments(OptimizationParameterTest):
    def test_parameter_create_invalid_args(
        self,
        run: ixmp4.Run,
        indexset: ixmp4.optimization.IndexSet,
        fake_time: datetime.datetime,
    ) -> None:
        with run.transact("Invalid arguments"):
            with pytest.raises(OptimizationItemUsageError, match="not equal in length"):
                run.optimization.parameters.create(
                    "Parameter",
                    constrained_to_indexsets=[indexset.name],
                    column_names=["Column 1", "Column 2"],
                )

            with pytest.raises(
                OptimizationItemUsageError, match="`column_names` are not unique"
            ):
                run.optimization.parameters.create(
                    "Parameter",
                    constrained_to_indexsets=[indexset.name, indexset.name],
                    column_names=["Column 1", "Column 1"],
                )


class ParameterDataTest(OptimizationParameterTest):
    @pytest.fixture(scope="class")
    def test_data_indexsets(self, run: ixmp4.Run) -> list[ixmp4.optimization.IndexSet]:
        with run.transact("Create indexsets"):
            indexset1 = run.optimization.indexsets.create("IndexSet 1")
            indexset2 = run.optimization.indexsets.create("IndexSet 2")
            indexset1.add_data(["do", "re", "mi", "fa", "so", "la", "ti"])
            indexset2.add_data([3, 1, 4])

        return [indexset1, indexset2]

    @pytest.fixture(scope="class")
    def test_data_units(self, platform: ixmp4.Platform) -> list[ixmp4.Unit]:
        return [
            platform.units.create("Unit 1"),
            platform.units.create("Unit 2"),
        ]

    def test_parameter_add_data(
        self,
        run: ixmp4.Run,
        test_data_indexsets: list[ixmp4.optimization.IndexSet],
        test_data_units: list[ixmp4.Unit],
        column_names: list[str] | None,
        test_data: dict[str, list[Any]] | pd.DataFrame,
        fake_time: datetime.datetime,
    ) -> None:
        with run.transact("Create parameter and add data"):
            parameter = run.optimization.parameters.create(
                "Parameter",
                constrained_to_indexsets=[i.name for i in test_data_indexsets],
                column_names=column_names,
            )
            parameter.add_data(test_data)

        if isinstance(test_data, pd.DataFrame):
            test_data = test_data.to_dict(orient="list")

        assert parameter.data == test_data

    def test_parameter_remove_data_partial(
        self,
        run: ixmp4.Run,
        partial_test_data: dict[str, list[Any]] | pd.DataFrame,
        remaining_test_data: dict[str, list[Any]],
        fake_time: datetime.datetime,
    ) -> None:
        with run.transact("Remove parameter data partial"):
            parameter = run.optimization.parameters.get_by_name("Parameter")
            parameter.remove_data(partial_test_data)

        assert parameter.data == remaining_test_data

    def test_parameter_remove_data_full(
        self,
        run: ixmp4.Run,
        fake_time: datetime.datetime,
    ) -> None:
        with run.transact("Remove parameter data full"):
            parameter = run.optimization.parameters.get_by_name("Parameter")
            parameter.remove_data()

        assert parameter.data == {}


class TestParameterData(ParameterDataTest):
    @pytest.fixture(scope="class")
    def column_names(
        self,
    ) -> list[str] | None:
        return None

    @pytest.fixture(scope="class")
    def test_data(self) -> dict[str, list[Any]] | pd.DataFrame:
        return {
            "units": ["Unit 1", "Unit 1", "Unit 2"],
            "values": [1.2, 1.5, -3],
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
            "units": ["Unit 1"],
            "values": [1.2],
            "IndexSet 1": ["do"],
            "IndexSet 2": [3],
        }


class TestParameterDataWithColumnNames(ParameterDataTest):
    @pytest.fixture(scope="class")
    def column_names(self) -> list[str] | None:
        return ["Column 1", "Column 2"]

    @pytest.fixture(scope="class")
    def test_data(self) -> dict[str, list[Any]] | pd.DataFrame:
        return {
            "units": ["Unit 1", "Unit 1", "Unit 2"],
            "values": [1.2, 1.5, -3],
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
            "units": ["Unit 1"],
            "values": [1.2],
            "Column 1": ["do"],
            "Column 2": [3],
        }


class TestParameterDataDataFrame(ParameterDataTest):
    @pytest.fixture(scope="class")
    def column_names(
        self,
    ) -> list[str] | None:
        return None

    @pytest.fixture(scope="class")
    def test_data(self) -> dict[str, list[Any]] | pd.DataFrame:
        return pd.DataFrame(
            [
                ["Unit 1", 1.2, "do", 3],
                ["Unit 1", 1.5, "re", 3],
                ["Unit 2", -3, "mi", 1],
            ],
            columns=["units", "values", "IndexSet 1", "IndexSet 2"],
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
            "units": ["Unit 1"],
            "values": [1.2],
            "IndexSet 1": ["do"],
            "IndexSet 2": [3],
        }


class TestParameterInvalidData(OptimizationParameterTest):
    def test_parameters_create(self, platform: ixmp4.Platform, run: ixmp4.Run) -> None:
        platform.units.create("Unit 1")
        platform.units.create("Unit 2")

        with run.transact("Create indexsets"):
            indexset1 = run.optimization.indexsets.create("IndexSet 1")
            indexset2 = run.optimization.indexsets.create("IndexSet 2")
            indexset1.add_data(["do", "re", "mi", "fa", "so", "la", "ti"])
            indexset2.add_data([3, 1, 4])

        with run.transact("Create parameters"):
            parameter1 = run.optimization.parameters.create(
                "Parameter 1",
                constrained_to_indexsets=["IndexSet 1", "IndexSet 2"],
            )
            assert parameter1.id == 1

    def test_parameter_add_invalid_data(self, run: ixmp4.Run) -> None:
        parameter1 = run.optimization.parameters.get_by_name("Parameter 1")

        with pytest.raises(
            OptimizationItemUsageError,
            match=r"Parameter.data must include the column\(s\): values!",
        ):
            with run.transact("Add invalid data"):
                parameter1.add_data(
                    {
                        "units": ["Unit 1"],
                        "IndexSet 1": ["do"],
                        "IndexSet 2": [3],
                    }
                )
        with pytest.raises(
            OptimizationItemUsageError,
            match=r"Parameter.data must include the column\(s\): units!",
        ):
            with run.transact("Add invalid data"):
                parameter1.add_data(
                    {
                        "values": [1.2],
                        "IndexSet 1": ["do"],
                        "IndexSet 2": [3],
                    }
                )

        with pytest.raises(
            OptimizationItemUsageError,
            match=r"Parameter.data must include the column\(s\): units, values!",
        ):
            with run.transact("Add invalid data"):
                parameter1.add_data(
                    {
                        "IndexSet 1": ["do"],
                        "IndexSet 2": [3],
                    }
                )

        with pytest.raises(
            ixmp4.optimization.Parameter.DataInvalid,
            match="All arrays must be of the same length",
        ):
            with run.transact("Add invalid data"):
                parameter1.add_data(
                    {
                        "units": ["Unit 1", "Unit 1", "Unit 2"],
                        "values": [1.2, 1.5, -3],
                        "IndexSet 1": ["do", "re"],  # missing "mi"
                        "IndexSet 2": [3, 3, 1],
                    }
                )
        with pytest.raises(
            ixmp4.optimization.Parameter.DataInvalid,
            match="All arrays must be of the same length",
        ):
            with run.transact("Add invalid data"):
                parameter1.add_data(
                    {
                        "units": ["Unit 1"],  # missing Unit 1, Unit 2
                        "values": [1.2, 1.5, -3],
                        "IndexSet 1": ["do", "re", "mi"],
                        "IndexSet 2": [3, 3, 1],
                    },
                )

        with pytest.raises(
            ixmp4.optimization.Parameter.DataInvalid, match="contains duplicate rows"
        ):
            with run.transact("Add invalid data"):
                parameter1.add_data(
                    {
                        "units": ["Unit 1", "Unit 1", "Unit 2"],
                        "values": [1.2, 1.5, -3],
                        "IndexSet 1": ["do", "do", "mi"],
                        "IndexSet 2": [3, 3, 1],
                    },
                )

    def test_parameter_remove_invalid_data(self, run: ixmp4.Run) -> None:
        parameter1 = run.optimization.parameters.get_by_name("Parameter 1")

        with pytest.raises(
            OptimizationItemUsageError, match="data to be removed must specify"
        ):
            with run.transact("Remove invalid data"):
                parameter1.remove_data({"IndexSet 1": ["do"]})
        with pytest.raises(
            OptimizationItemUsageError, match="data to be removed must specify"
        ):
            with run.transact("Remove invalid data"):
                parameter1.remove_data({"values": [2]})


class TestParameterRunLock(OptimizationParameterTest):
    def test_parameter_requires_lock(
        self,
        platform: ixmp4.Platform,
        run: ixmp4.Run,
        indexset: ixmp4.optimization.IndexSet,
    ) -> None:
        platform.units.create("Unit 1")

        with pytest.raises(ixmp4.Run.LockRequired):
            run.optimization.parameters.create(
                "Parameter", constrained_to_indexsets=[indexset.name]
            )

        with run.transact("Create parameter"):
            parameter = run.optimization.parameters.create(
                "Parameter", constrained_to_indexsets=[indexset.name]
            )

        with pytest.raises(ixmp4.Run.LockRequired):
            parameter.add_data({"units": ["Unit 1"], "values": [2], "IndexSet": ["fa"]})

        with pytest.raises(ixmp4.Run.LockRequired):
            parameter.remove_data({"IndexSet": ["fa"]})

        with pytest.raises(ixmp4.Run.LockRequired):
            parameter.delete()

        with pytest.raises(ixmp4.Run.LockRequired):
            run.optimization.parameters.delete(parameter.id)


class TestParameterDocs(OptimizationParameterTest):
    def test_create_docs_via_func(
        self, run: ixmp4.Run, indexset: ixmp4.optimization.IndexSet
    ) -> None:
        with run.transact("Create parameter 1"):
            parameter1 = run.optimization.parameters.create(
                "Parameter 1", constrained_to_indexsets=[indexset.name]
            )

        parameter1_docs1 = run.optimization.parameters.set_docs(
            "Parameter 1", "Description of Parameter 1"
        )
        parameter1_docs2 = run.optimization.parameters.get_docs("Parameter 1")

        assert parameter1_docs1 == parameter1_docs2
        assert parameter1.docs == parameter1_docs1

    def test_create_docs_via_object(
        self, run: ixmp4.Run, indexset: ixmp4.optimization.IndexSet
    ) -> None:
        with run.transact("Create parameter 2"):
            parameter2 = run.optimization.parameters.create(
                "Parameter 2", constrained_to_indexsets=[indexset.name]
            )
        parameter2.docs = "Description of Parameter 2"

        assert run.optimization.parameters.get_docs("Parameter 2") == parameter2.docs

    def test_create_docs_via_setattr(
        self, run: ixmp4.Run, indexset: ixmp4.optimization.IndexSet
    ) -> None:
        with run.transact("Create parameter 3"):
            parameter3 = run.optimization.parameters.create(
                "Parameter 3", constrained_to_indexsets=[indexset.name]
            )
        setattr(parameter3, "docs", "Description of Parameter 3")

        assert run.optimization.parameters.get_docs("Parameter 3") == parameter3.docs

    def test_list_docs(self, run: ixmp4.Run) -> None:
        assert run.optimization.parameters.list_docs() == [
            "Description of Parameter 1",
            "Description of Parameter 2",
            "Description of Parameter 3",
        ]

        assert run.optimization.parameters.list_docs(id=3) == [
            "Description of Parameter 3"
        ]

        assert run.optimization.parameters.list_docs(id__in=[1]) == [
            "Description of Parameter 1"
        ]

    def test_delete_docs_via_func(self, run: ixmp4.Run) -> None:
        parameter1 = run.optimization.parameters.get_by_name("Parameter 1")
        run.optimization.parameters.delete_docs("Parameter 1")
        parameter1 = run.optimization.parameters.get_by_name("Parameter 1")
        assert parameter1.docs is None

    def test_delete_docs_set_none(self, run: ixmp4.Run) -> None:
        parameter2 = run.optimization.parameters.get_by_name("Parameter 2")
        parameter2.docs = None
        parameter2 = run.optimization.parameters.get_by_name("Parameter 2")
        assert parameter2.docs is None

    def test_delete_docs_del(self, run: ixmp4.Run) -> None:
        parameter3 = run.optimization.parameters.get_by_name("Parameter 3")
        del parameter3.docs
        parameter3 = run.optimization.parameters.get_by_name("Parameter 3")
        assert parameter3.docs is None

    def test_docs_empty(self, run: ixmp4.Run) -> None:
        assert len(run.optimization.parameters.list_docs()) == 0


class TestParameterRollback(OptimizationParameterTest):
    def test_parameter_add_data_failure(
        self,
        platform: ixmp4.Platform,
        run: ixmp4.Run,
        indexset: ixmp4.optimization.IndexSet,
    ) -> None:
        platform.units.create("Unit 1")
        platform.units.create("Unit 2")

        with run.transact("Add parameter data"):
            parameter = run.optimization.parameters.create(
                "Parameter", constrained_to_indexsets=[indexset.name]
            )
            parameter.add_data(
                {
                    "units": ["Unit 1", "Unit 1", "Unit 2"],
                    "values": [1.2, 1.5, -3],
                    "IndexSet": ["do", "re", "mi"],
                }
            )

        try:
            with run.transact("Update parameter data failure"):
                parameter.add_data(
                    {
                        "units": ["Unit 2", "Unit 2"],
                        "values": [-2.2, -9.59],
                        "IndexSet": ["fa", "so"],
                    }
                )
                raise CustomException
        except CustomException:
            pass

    def test_parameter_versioning_after_add_data_failure(
        self, versioning_platform: ixmp4.Platform, run: ixmp4.Run
    ) -> None:
        parameter = run.optimization.parameters.get_by_name("Parameter")
        assert parameter.data == {
            "units": ["Unit 1", "Unit 1", "Unit 2"],
            "values": [1.2, 1.5, -3],
            "IndexSet": ["do", "re", "mi"],
        }

    def test_parameter_non_versioning_after_add_data_failure(
        self, non_versioning_platform: ixmp4.Platform, run: ixmp4.Run
    ) -> None:
        parameter = run.optimization.parameters.get_by_name("Parameter")
        # NOTE: order is not preserved
        assert parameter.data == {
            "units": ["Unit 1", "Unit 2", "Unit 2", "Unit 1", "Unit 2"],
            "values": [1.2, -2.2, -3, 1.5, -9.59],
            "IndexSet": ["do", "fa", "mi", "re", "so"],
        }

    def test_parameter_remove_data_failure(self, run: ixmp4.Run) -> None:
        parameter = run.optimization.parameters.get_by_name("Parameter")

        with run.transact("Remove parameter data"):
            parameter.remove_data({"IndexSet": ["fa", "so"]})

        try:
            with run.transact("Remove parameter data failure"):
                parameter.remove_data({"IndexSet": ["do", "re"]})
                raise CustomException
        except CustomException:
            pass

    def test_parameter_versioning_after_remove_data_failure(
        self, versioning_platform: ixmp4.Platform, run: ixmp4.Run
    ) -> None:
        parameter = run.optimization.parameters.get_by_name("Parameter")
        assert parameter.data == {
            "units": ["Unit 1", "Unit 1", "Unit 2"],
            "values": [1.2, 1.5, -3],
            "IndexSet": ["do", "re", "mi"],
        }

    def test_parameter_non_versioning_after_remove_data_failure(
        self, non_versioning_platform: ixmp4.Platform, run: ixmp4.Run
    ) -> None:
        parameter = run.optimization.parameters.get_by_name("Parameter")
        assert parameter.data == {
            "units": ["Unit 2"],
            "values": [-3],
            "IndexSet": ["mi"],
        }

    def test_parameter_docs_failure(self, run: ixmp4.Run) -> None:
        parameter = run.optimization.parameters.get_by_name("Parameter")

        try:
            with run.transact("Set parameter docs failure"):
                parameter.docs = "These docs should persist!"
                raise CustomException
        except CustomException:
            pass

    def test_parameter_after_docs_failure(
        self, platform: ixmp4.Platform, run: ixmp4.Run
    ) -> None:
        parameter = run.optimization.parameters.get_by_name("Parameter")
        assert parameter.docs == "These docs should persist!"

    def test_parameter_delete_failure(
        self, run: ixmp4.Run, indexset: ixmp4.optimization.IndexSet
    ) -> None:
        parameter = run.optimization.parameters.get_by_name("Parameter")

        try:
            with run.transact("Delete parameter failure"):
                parameter.delete()
                indexset.delete()
                raise CustomException
        except CustomException:
            pass

    def test_parameter_versioning_after_delete_failure(
        self, versioning_platform: ixmp4.Platform, run: ixmp4.Run
    ) -> None:
        parameter = run.optimization.parameters.get_by_name("Parameter")
        assert parameter.id == 1

    def test_parameter_non_versioning_after_delete_failure(
        self, non_versioning_platform: ixmp4.Platform, run: ixmp4.Run
    ) -> None:
        with pytest.raises(ixmp4.optimization.Parameter.NotFound):
            run.optimization.parameters.get_by_name("Parameter")
