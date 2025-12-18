import datetime
from typing import Any

import pandas as pd
import pandas.testing as pdt
import pytest

from ixmp4.base_exceptions import Forbidden, OptimizationItemUsageError
from ixmp4.data.optimization.indexset.service import IndexSet, IndexSetService
from ixmp4.data.optimization.variable.repositories import (
    VariableDataInvalid,
    VariableNotFound,
    VariableNotUnique,
)
from ixmp4.data.optimization.variable.service import VariableService
from ixmp4.data.run.dto import Run
from ixmp4.data.run.service import RunService
from ixmp4.transport import Transport
from tests import auth, backends
from tests.data.base import ServiceTest

transport = backends.get_transport_fixture(scope="class")


class VariableServiceTest(ServiceTest[VariableService]):
    service_class = VariableService

    @pytest.fixture(scope="class")
    def runs(self, transport: Transport) -> RunService:
        return RunService(transport)

    @pytest.fixture(scope="class")
    def run(
        self,
        runs: RunService,
    ) -> Run:
        run = runs.create("Model", "Scenario")
        assert run.id == 1
        return run

    @pytest.fixture(scope="class")
    def indexsets(self, transport: Transport) -> IndexSetService:
        return IndexSetService(transport)


class TestVariableCreate(VariableServiceTest):
    def test_variable_create(
        self, service: VariableService, run: Run, fake_time: datetime.datetime
    ) -> None:
        variable = service.create(run.id, "Variable")
        assert variable.run__id == run.id
        assert variable.name == "Variable"
        assert variable.data == {}
        assert variable.indexset_names is None
        assert variable.column_names is None

        assert variable.created_at == fake_time.replace(tzinfo=None)
        assert variable.created_by == "@unknown"

    def test_variable_create_versioning(
        self,
        versioning_service: VariableService,
        run: Run,
        fake_time: datetime.datetime,
    ) -> None:
        expected_versions = pd.DataFrame(
            [
                [
                    1,
                    run.id,
                    "Variable",
                    {},
                    fake_time.replace(tzinfo=None),
                    "@unknown",
                    4,
                    None,
                    0,
                ],
            ],
            columns=[
                "id",
                "run__id",
                "name",
                "data",
                "created_at",
                "created_by",
                "transaction_id",
                "end_transaction_id",
                "operation_type",
            ],
        )
        vdf = versioning_service.versions.tabulate()
        pdt.assert_frame_equal(expected_versions, vdf, check_like=True)


class TestVariableCreateInvalidArguments(VariableServiceTest):
    def test_variable_create_invalid_args(
        self,
        service: VariableService,
        run: Run,
        indexsets: IndexSetService,
        fake_time: datetime.datetime,
    ) -> None:
        with pytest.raises(
            OptimizationItemUsageError,
            match="Received `column_names` to name columns, but no "
            "`constrained_to_indexsets`",
        ):
            service.create(run.id, "Variable", column_names=["Column 1"])

        indexsets.create(run.id, "IndexSet")
        with pytest.raises(OptimizationItemUsageError, match="not equal in length"):
            service.create(
                run.id,
                "Variable",
                constrained_to_indexsets=["IndexSet"],
                column_names=["Column 1", "Column 2"],
            )

        with pytest.raises(
            OptimizationItemUsageError, match="`column_names` are not unique"
        ):
            service.create(
                run.id,
                "Variable",
                constrained_to_indexsets=["IndexSet", "IndexSet"],
                column_names=["Column 1", "Column 1"],
            )


class TestVariableDeleteById(VariableServiceTest):
    def test_variable_delete_by_id(
        self,
        service: VariableService,
        run: Run,
        indexsets: IndexSetService,
        fake_time: datetime.datetime,
    ) -> None:
        indexsets.create(run.id, "IndexSet")
        variable = service.create(
            run.id,
            "Variable",
            constrained_to_indexsets=["IndexSet"],
            column_names=["Column"],
        )
        service.delete_by_id(variable.id)
        assert service.tabulate().empty

    def test_variable_delete_by_id_versioning(
        self,
        versioning_service: VariableService,
        run: Run,
        fake_time: datetime.datetime,
    ) -> None:
        expected_versions = pd.DataFrame(
            [
                [
                    1,
                    run.id,
                    "Variable",
                    {},
                    fake_time.replace(tzinfo=None),
                    "@unknown",
                    5,
                    8,
                    0,
                ],
                [
                    1,
                    run.id,
                    "Variable",
                    {},
                    fake_time.replace(tzinfo=None),
                    "@unknown",
                    8,
                    None,
                    2,
                ],
            ],
            columns=[
                "id",
                "run__id",
                "name",
                "data",
                "created_at",
                "created_by",
                "transaction_id",
                "end_transaction_id",
                "operation_type",
            ],
        )
        vdf = versioning_service.versions.tabulate()
        pdt.assert_frame_equal(
            expected_versions,
            vdf,
            check_like=True,
        )
        # TODO Association Versions


class TestVariableUnique(VariableServiceTest):
    def test_variable_unique(self, service: VariableService, run: Run) -> None:
        service.create(run.id, "Variable")

        with pytest.raises(VariableNotUnique):
            service.create(run.id, "Variable")


class TestVariableGetByName(VariableServiceTest):
    def test_variable_get(self, service: VariableService, run: Run) -> None:
        variable1 = service.create(run.id, "Variable")
        variable2 = service.get(run.id, "Variable")
        assert variable1 == variable2


class TestVariableGetById(VariableServiceTest):
    def test_variable_get_by_id(self, service: VariableService, run: Run) -> None:
        variable1 = service.create(run.id, "Variable")
        variable2 = service.get_by_id(1)
        assert variable1 == variable2


class TestVariableNotFound(VariableServiceTest):
    def test_variable_not_found(self, service: VariableService, run: Run) -> None:
        with pytest.raises(VariableNotFound):
            service.get(run.id, "Variable")

        with pytest.raises(VariableNotFound):
            service.get_by_id(1)


class VariableDataTest(VariableServiceTest):
    def test_variable_add_data(
        self,
        service: VariableService,
        run: Run,
        test_data_indexsets: list[IndexSet],
        column_names: list[str] | None,
        test_data: dict[str, list[Any]] | pd.DataFrame,
        fake_time: datetime.datetime,
    ) -> None:
        variable = service.create(
            run.id,
            "Variable",
            constrained_to_indexsets=[i.name for i in test_data_indexsets],
            column_names=column_names,
        )
        service.add_data(variable.id, test_data)
        variable = service.get_by_id(variable.id)

        if isinstance(test_data, pd.DataFrame):
            test_data = test_data.to_dict(orient="list")

        assert variable.data == test_data

    def test_variable_remove_data_partial(
        self,
        service: VariableService,
        run: Run,
        partial_test_data: dict[str, list[Any]] | pd.DataFrame,
        remaining_test_data: dict[str, list[Any]],
        fake_time: datetime.datetime,
    ) -> None:
        variable = service.get(run.id, "Variable")
        service.remove_data(variable.id, partial_test_data)
        variable = service.get_by_id(variable.id)
        assert variable.data == remaining_test_data

    def test_variable_remove_data_all(
        self,
        service: VariableService,
        run: Run,
        fake_time: datetime.datetime,
    ) -> None:
        variable = service.get(run.id, "Variable")
        service.remove_data(variable.id)
        variable = service.get_by_id(variable.id)
        assert variable.data == {}

    def test_variable_data_versioning(
        self,
        versioning_service: IndexSetService,
        run: Run,
        test_data_indexsets: list[IndexSet],
        test_data: dict[str, list[Any]] | pd.DataFrame,
        remaining_test_data: dict[str, list[Any]] | pd.DataFrame,
        fake_time: datetime.datetime,
    ) -> None:
        if isinstance(test_data, pd.DataFrame):
            test_data = test_data.to_dict(orient="list")

        if isinstance(remaining_test_data, pd.DataFrame):
            remaining_test_data = remaining_test_data.to_dict(orient="list")

        # compute transaction ids
        is_tx = (
            5 + len(test_data_indexsets) + sum(len(i.data) for i in test_data_indexsets)
        )
        create_tx = is_tx + 1
        add_data_tx = create_tx + 3
        rm_data_partial_tx = add_data_tx + 1
        rm_data_full_tx = rm_data_partial_tx + 1

        expected_versions = pd.DataFrame(
            [
                [
                    {},
                    create_tx,
                    add_data_tx,
                    0,
                ],
                [
                    test_data,
                    add_data_tx,
                    rm_data_partial_tx,
                    1,
                ],
                [
                    remaining_test_data,
                    rm_data_partial_tx,
                    rm_data_full_tx,
                    1,
                ],
                [
                    {},
                    rm_data_full_tx,
                    None,
                    1,
                ],
            ],
            columns=[
                "data",
                "transaction_id",
                "end_transaction_id",
                "operation_type",
            ],
        )

        expected_versions["id"] = 1
        expected_versions["run__id"] = run.id
        expected_versions["name"] = "Variable"
        expected_versions["created_at"] = pd.Timestamp(
            fake_time.replace(tzinfo=None)
        ).as_unit("ns")
        expected_versions["created_by"] = "@unknown"

        vdf = versioning_service.versions.tabulate()
        pdt.assert_frame_equal(expected_versions, vdf, check_like=True)


class TestVariableData(VariableDataTest):
    @pytest.fixture(scope="class")
    def test_data_indexsets(
        self, run: Run, indexsets: IndexSetService
    ) -> list[IndexSet]:
        indexset1 = indexsets.create(run.id, "IndexSet 1")
        indexset2 = indexsets.create(run.id, "IndexSet 2")
        indexsets.add_data(indexset1.id, ["do", "re", "mi", "fa", "so", "la", "ti"])
        indexsets.add_data(indexset2.id, [3, 1, 4])
        return [indexsets.get_by_id(indexset1.id), indexsets.get_by_id(indexset2.id)]

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
    def test_data_indexsets(
        self, run: Run, indexsets: IndexSetService
    ) -> list[IndexSet]:
        indexset1 = indexsets.create(run.id, "IndexSet 1")
        indexset2 = indexsets.create(run.id, "IndexSet 2")
        indexsets.add_data(indexset1.id, ["do", "re", "mi", "fa", "so", "la", "ti"])
        indexsets.add_data(indexset2.id, [3, 1, 4])
        return [indexsets.get_by_id(indexset1.id), indexsets.get_by_id(indexset2.id)]

    @pytest.fixture(scope="class")
    def column_names(
        self,
    ) -> list[str] | None:
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
    def test_data_indexsets(
        self, run: Run, indexsets: IndexSetService
    ) -> list[IndexSet]:
        indexset1 = indexsets.create(run.id, "IndexSet 1")
        indexset2 = indexsets.create(run.id, "IndexSet 2")
        indexsets.add_data(indexset1.id, ["do", "re", "mi", "fa", "so", "la", "ti"])
        indexsets.add_data(indexset2.id, [3, 1, 4])
        return [indexsets.get_by_id(indexset1.id), indexsets.get_by_id(indexset2.id)]

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


class TestVariableInvalidData(VariableServiceTest):
    def test_variables_create(
        self, service: VariableService, indexsets: IndexSetService, run: Run
    ) -> None:
        indexset1 = indexsets.create(run.id, "IndexSet 1")
        indexset2 = indexsets.create(run.id, "IndexSet 2")
        indexsets.add_data(indexset1.id, ["do", "re", "mi", "fa", "so", "la", "ti"])
        indexsets.add_data(indexset2.id, [3, 1, 4])

        variable = service.create(
            run.id,
            "Variable 1",
            constrained_to_indexsets=["IndexSet 1", "IndexSet 2"],
        )
        assert variable.id == 1

        variable = service.create(
            run.id,
            "Variable 2",
        )
        assert variable.id == 2

    def test_variable_add_invalid_data(
        self,
        service: VariableService,
    ) -> None:
        with pytest.raises(
            OptimizationItemUsageError,
            match=r"Variable.data must include the column\(s\): levels!",
        ):
            service.add_data(
                1,
                {
                    "marginals": [-2],
                    "IndexSet 1": ["do"],
                    "IndexSet 2": [3],
                },
            )
        with pytest.raises(
            OptimizationItemUsageError,
            match=r"Variable.data must include the column\(s\): marginals!",
        ):
            service.add_data(
                1,
                {
                    "levels": [2],
                    "IndexSet 1": ["do"],
                    "IndexSet 2": [3],
                },
            )

        with pytest.raises(
            OptimizationItemUsageError,
            match=r"Variable.data must include the column\(s\): levels, marginals!",
        ):
            service.add_data(
                1,
                {
                    "IndexSet 1": ["do"],
                    "IndexSet 2": [3],
                },
            )

        with pytest.raises(
            VariableDataInvalid,
            match="All arrays must be of the same length",
        ):
            service.add_data(
                1,
                {
                    "marginals": [-2, 1, 1],
                    "levels": [2, 1, 3],
                    "IndexSet 1": ["do", "re"],  # missing "mi"
                    "IndexSet 2": [3, 3, 1],
                },
            )
        with pytest.raises(
            VariableDataInvalid,
            match="All arrays must be of the same length",
        ):
            service.add_data(
                1,
                {
                    "marginals": [
                        -2,
                    ],  # missing 1,1
                    "levels": [2, 1, 3],
                    "IndexSet 1": ["do", "re", "mi"],
                    "IndexSet 2": [3, 3, 1],
                },
            )

        with pytest.raises(VariableDataInvalid, match="contains duplicate rows"):
            service.add_data(
                1,
                {
                    "marginals": [-2, 1, 1],
                    "levels": [2, 1, 3],
                    "IndexSet 1": ["do", "do", "mi"],
                    "IndexSet 2": [3, 3, 1],
                },
            )

    def test_variable_remove_invalid_data(
        self,
        service: VariableService,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        with pytest.raises(
            OptimizationItemUsageError, match="data to be removed must specify"
        ):
            service.remove_data(
                1,
                {
                    "IndexSet 1": ["do"],
                },
            )
        with pytest.raises(
            OptimizationItemUsageError, match="data to be removed must specify"
        ):
            service.remove_data(
                1,
                {
                    "levels": [2],
                },
            )

        caplog.clear()
        with caplog.at_level("WARNING"):
            service.remove_data(
                2,
                {
                    "levels": [2],
                    "marginals": [-2],
                },
            )

        expected = [
            (
                "Trying to remove {'levels': [2], 'marginals': [-2]} from `Variable` "
                "'Variable 2', but that is not indexed; not removing anything!"
            )
        ]
        assert caplog.messages == expected


class TestVariableList(VariableServiceTest):
    def test_variable_list(
        self,
        service: VariableService,
        run: Run,
        indexsets: IndexSetService,
        fake_time: datetime.datetime,
    ) -> None:
        indexset1 = indexsets.create(run.id, "IndexSet 1")
        indexset2 = indexsets.create(run.id, "IndexSet 2")
        indexsets.add_data(indexset1.id, ["do", "re", "mi", "fa", "so", "la", "ti"])
        indexsets.add_data(indexset2.id, [3, 1, 4])

        service.create(
            run.id, "Variable 1", constrained_to_indexsets=["IndexSet 1", "IndexSet 2"]
        )
        service.create(run.id, "Variable 2")

        test_data1 = {
            "marginals": [-2, 1, 1],
            "levels": [2, 1, 3],
            "IndexSet 1": ["do", "re", "mi"],
            "IndexSet 2": [3, 3, 1],
        }
        service.add_data(1, test_data1)

        variables = service.list()

        assert variables[0].id == 1
        assert variables[0].run__id == run.id
        assert variables[0].name == "Variable 1"
        assert variables[0].data == test_data1
        assert variables[0].created_by == "@unknown"
        assert variables[0].created_at == fake_time.replace(tzinfo=None)

        assert variables[1].id == 2
        assert variables[1].run__id == run.id
        assert variables[1].name == "Variable 2"
        assert variables[1].data == {}
        assert variables[1].created_by == "@unknown"
        assert variables[1].created_at == fake_time.replace(tzinfo=None)


class TestVariableTabulate(VariableServiceTest):
    def test_variable_tabulate(
        self,
        service: VariableService,
        run: Run,
        indexsets: IndexSetService,
        fake_time: datetime.datetime,
    ) -> None:
        indexset1 = indexsets.create(run.id, "IndexSet 1")
        indexset2 = indexsets.create(run.id, "IndexSet 2")
        indexsets.add_data(indexset1.id, ["do", "re", "mi", "fa", "so", "la", "ti"])
        indexsets.add_data(indexset2.id, [3, 1, 4])

        service.create(
            run.id, "Variable 1", constrained_to_indexsets=["IndexSet 1", "IndexSet 2"]
        )
        service.create(run.id, "Variable 2")

        test_data1 = {
            "marginals": [-2, 1, 1],
            "levels": [2, 1, 3],
            "IndexSet 1": ["do", "re", "mi"],
            "IndexSet 2": [3, 3, 1],
        }
        service.add_data(1, test_data1)

        expected_variables = pd.DataFrame(
            [
                [1, "Variable 1", test_data1],
                [2, "Variable 2", {}],
            ],
            columns=["id", "name", "data"],
        )
        expected_variables["run__id"] = run.id
        expected_variables["created_at"] = pd.Timestamp(
            fake_time.replace(tzinfo=None)
        ).as_unit("ns")
        expected_variables["created_by"] = "@unknown"

        variables = service.tabulate()
        pdt.assert_frame_equal(variables, expected_variables, check_like=True)


class VariableAuthTest(VariableServiceTest):
    @pytest.fixture(scope="class")
    def runs(self, transport: Transport) -> RunService:
        direct = self.get_unauthorized_direct_or_skip(transport)
        return RunService(direct)

    @pytest.fixture(scope="class")
    def indexsets(self, transport: Transport) -> IndexSetService:
        direct = self.get_unauthorized_direct_or_skip(transport)
        return IndexSetService(direct)

    @pytest.fixture(scope="class")
    def run(self, runs: RunService, indexsets: IndexSetService) -> Run:
        run = runs.create("Model", "Scenario")
        self.create_indexsets(run, indexsets)
        return run

    @pytest.fixture(scope="class")
    def run1(self, runs: RunService, indexsets: IndexSetService) -> Run:
        run = runs.create("Model 1", "Scenario")
        self.create_indexsets(run, indexsets)
        return run

    @pytest.fixture(scope="class")
    def run2(self, runs: RunService, indexsets: IndexSetService) -> Run:
        run = runs.create("Model 2", "Scenario")
        self.create_indexsets(run, indexsets)
        return run

    @pytest.fixture(scope="class")
    def test_data(self) -> dict[str, list[Any]]:
        return {
            "marginals": [-2, 1, 1],
            "levels": [2, 1, 3],
            "IndexSet 1": ["do", "re", "mi"],
            "IndexSet 2": [3, 3, 1],
        }

    def create_indexsets(self, run: Run, indexsets: IndexSetService) -> list[IndexSet]:
        indexset1 = indexsets.create(run.id, "IndexSet 1")
        indexset2 = indexsets.create(run.id, "IndexSet 2")
        indexsets.add_data(indexset1.id, ["do", "re", "mi", "fa", "so", "la", "ti"])
        indexsets.add_data(indexset2.id, [3, 1, 4])
        return [indexsets.get_by_id(indexset1.id), indexsets.get_by_id(indexset2.id)]

    @pytest.fixture(scope="class")
    def indexset_names(self) -> list[str]:
        return ["IndexSet 1", "IndexSet 2"]


class TestVariableAuthSarahPrivate(
    auth.SarahTest, auth.PrivatePlatformTest, VariableAuthTest
):
    def test_variable_create(
        self, service: VariableService, run: Run, indexset_names: list[str]
    ) -> None:
        ret = service.create(
            run.id,
            "Variable",
            constrained_to_indexsets=indexset_names,
        )
        assert ret.id == 1

    def test_variable_get(self, service: VariableService, run: Run) -> None:
        ret = service.get(run.id, "Variable")
        assert ret.id == 1

    def test_variable_get_by_id(self, service: VariableService) -> None:
        ret = service.get_by_id(1)
        assert ret.id == 1

    def test_variable_add_data(
        self,
        service: VariableService,
        test_data: dict[str, list[Any]],
    ) -> None:
        service.add_data(1, test_data)

    def test_variable_remove_data(
        self,
        service: VariableService,
        test_data: dict[str, list[Any]],
    ) -> None:
        service.remove_data(1, test_data)

    def test_variable_list(self, service: VariableService) -> None:
        ret = service.list()
        assert len(ret) == 1

    def test_variable_tabulate(self, service: VariableService) -> None:
        ret = service.tabulate()
        assert len(ret) == 1

    def test_variable_delete(self, service: VariableService) -> None:
        service.delete_by_id(1)


class TestVariableAuthAlicePrivate(
    auth.AliceTest, auth.PrivatePlatformTest, VariableAuthTest
):
    def test_variable_create(
        self,
        service: VariableService,
        unauthorized_service: VariableService,
        run: Run,
        indexset_names: list[str],
    ) -> None:
        with pytest.raises(Forbidden):
            service.create(
                run.id,
                "Variable",
                constrained_to_indexsets=indexset_names,
            )
        ret = unauthorized_service.create(
            run.id,
            "Variable",
            constrained_to_indexsets=indexset_names,
        )
        assert ret.id == 1

    def test_table_get(self, service: VariableService, run: Run) -> None:
        with pytest.raises(Forbidden):
            service.get(run.id, "Variable")

    def test_table_get_by_id(self, service: VariableService) -> None:
        with pytest.raises(Forbidden):
            service.get_by_id(1)

    def test_variable_add_data(
        self,
        service: VariableService,
        unauthorized_service: VariableService,
        test_data: dict[str, list[Any]],
    ) -> None:
        with pytest.raises(Forbidden):
            service.add_data(1, test_data)
        unauthorized_service.add_data(1, test_data)

    def test_variable_remove_data(
        self,
        service: VariableService,
        unauthorized_service: VariableService,
        test_data: dict[str, list[Any]],
    ) -> None:
        with pytest.raises(Forbidden):
            service.remove_data(1, test_data)
        unauthorized_service.remove_data(1, test_data)

    def test_variable_list(self, service: VariableService) -> None:
        with pytest.raises(Forbidden):
            service.list()

    def test_variable_tabulate(self, service: VariableService) -> None:
        with pytest.raises(Forbidden):
            service.tabulate()

    def test_variable_delete(
        self,
        service: VariableService,
        unauthorized_service: VariableService,
    ) -> None:
        with pytest.raises(Forbidden):
            service.delete_by_id(1)
        unauthorized_service.delete_by_id(1)


class TestVariableAuthBobPrivate(
    auth.BobTest, auth.PrivatePlatformTest, VariableAuthTest
):
    def test_variable_create(
        self,
        service: VariableService,
        run: Run,
        indexset_names: list[str],
    ) -> None:
        ret = service.create(
            run.id,
            "Variable",
            constrained_to_indexsets=indexset_names,
        )
        assert ret.id == 1

    def test_variable_get(self, service: VariableService, run: Run) -> None:
        ret = service.get(run.id, "Variable")
        assert ret.id == 1

    def test_variable_get_by_id(self, service: VariableService) -> None:
        ret = service.get_by_id(1)
        assert ret.id == 1

    def test_variable_add_data(
        self,
        service: VariableService,
        test_data: dict[str, list[Any]],
    ) -> None:
        service.add_data(1, test_data)

    def test_variable_remove_data(
        self,
        service: VariableService,
        test_data: dict[str, list[Any]],
    ) -> None:
        service.remove_data(1, test_data)

    def test_variable_list(self, service: VariableService) -> None:
        ret = service.list()
        assert len(ret) == 1

    def test_variable_tabulate(self, service: VariableService) -> None:
        ret = service.tabulate()
        assert len(ret) == 1

    def test_variable_delete(self, service: VariableService) -> None:
        service.delete_by_id(1)


class TestVariableAuthCarinaPrivate(
    auth.CarinaTest, auth.PrivatePlatformTest, VariableAuthTest
):
    def test_variable_create(
        self,
        service: VariableService,
        unauthorized_service: VariableService,
        run: Run,
        run1: Run,
        run2: Run,
        indexset_names: list[str],
    ) -> None:
        with pytest.raises(Forbidden):
            service.create(
                run.id,
                "Variable",
                constrained_to_indexsets=indexset_names,
            )
        ret = unauthorized_service.create(
            run.id,
            "Variable",
            constrained_to_indexsets=indexset_names,
        )
        assert ret.id == 1

        ret2 = service.create(
            run1.id,
            "Variable 1",
            constrained_to_indexsets=indexset_names,
        )
        assert ret2.id == 2

        with pytest.raises(Forbidden):
            service.create(
                run2.id,
                "Variable 2",
                constrained_to_indexsets=indexset_names,
            )
        ret3 = unauthorized_service.create(
            run2.id,
            "Variable 2",
            constrained_to_indexsets=indexset_names,
        )
        assert ret3.id == 3

    def test_variable_get(
        self, service: VariableService, run: Run, run1: Run, run2: Run
    ) -> None:
        ret = service.get(run.id, "Variable")
        assert ret.id == 1

        ret = service.get(run1.id, "Variable 1")
        assert ret.id == 2

        with pytest.raises(VariableNotFound):
            service.get(run2.id, "Variable 2")

    def test_variable_get_by_id(self, service: VariableService) -> None:
        ret = service.get_by_id(1)
        assert ret.id == 1
        ret2 = service.get_by_id(2)
        assert ret2.id == 2

        with pytest.raises(VariableNotFound):
            service.get_by_id(3)

    def test_variable_add_data(
        self,
        service: VariableService,
        unauthorized_service: VariableService,
        test_data: dict[str, list[Any]],
    ) -> None:
        with pytest.raises(Forbidden):
            service.add_data(1, test_data)
        unauthorized_service.add_data(1, test_data)

        service.add_data(2, test_data)

        with pytest.raises(VariableNotFound):
            service.add_data(3, test_data)
        unauthorized_service.add_data(3, test_data)

    def test_variable_remove_data(
        self,
        service: VariableService,
        unauthorized_service: VariableService,
        test_data: dict[str, list[Any]],
    ) -> None:
        with pytest.raises(Forbidden):
            service.remove_data(1, test_data)
        unauthorized_service.remove_data(1, test_data)

        service.remove_data(2, test_data)

        with pytest.raises(VariableNotFound):
            service.remove_data(3, test_data)
        unauthorized_service.remove_data(3, test_data)

    def test_variable_list(self, service: VariableService) -> None:
        ret = service.list()
        assert len(ret) == 2

    def test_variable_tabulate(self, service: VariableService) -> None:
        ret = service.tabulate()
        assert len(ret) == 2

    def test_variable_delete(
        self,
        service: VariableService,
        unauthorized_service: VariableService,
    ) -> None:
        with pytest.raises(Forbidden):
            service.delete_by_id(1)
        unauthorized_service.delete_by_id(1)

        service.delete_by_id(2)

        with pytest.raises(VariableNotFound):
            service.delete_by_id(3)
        unauthorized_service.delete_by_id(3)


class TestVariableAuthNonePrivate(
    auth.NoneTest, auth.PrivatePlatformTest, VariableAuthTest
):
    def test_variable_create(
        self,
        service: VariableService,
        unauthorized_service: VariableService,
        run: Run,
        indexset_names: list[str],
    ) -> None:
        with pytest.raises(Forbidden):
            service.create(
                run.id,
                "Variable",
                constrained_to_indexsets=indexset_names,
            )
        ret = unauthorized_service.create(
            run.id,
            "Variable",
            constrained_to_indexsets=indexset_names,
        )
        assert ret.id == 1

    def test_variable_get(self, service: VariableService, run: Run) -> None:
        with pytest.raises(Forbidden):
            service.get(run.id, "Variable")

    def test_variable_get_by_id(self, service: VariableService) -> None:
        with pytest.raises(Forbidden):
            service.get_by_id(1)

    def test_variable_add_data(
        self,
        service: VariableService,
        unauthorized_service: VariableService,
        test_data: dict[str, list[Any]],
    ) -> None:
        with pytest.raises(Forbidden):
            service.add_data(1, test_data)
        unauthorized_service.add_data(1, test_data)

    def test_variable_remove_data(
        self,
        service: VariableService,
        unauthorized_service: VariableService,
        test_data: dict[str, list[Any]],
    ) -> None:
        with pytest.raises(Forbidden):
            service.remove_data(1, test_data)
        unauthorized_service.add_data(1, test_data)

    def test_variable_list(self, service: VariableService) -> None:
        with pytest.raises(Forbidden):
            service.list()

    def test_variable_tabulate(self, service: VariableService) -> None:
        with pytest.raises(Forbidden):
            service.tabulate()

    def test_variable_delete(
        self,
        service: VariableService,
        unauthorized_service: VariableService,
    ) -> None:
        with pytest.raises(Forbidden):
            service.delete_by_id(1)
        unauthorized_service.delete_by_id(1)


class TestRunAuthDaveGated(auth.DaveTest, auth.GatedPlatformTest, VariableAuthTest):
    def test_variable_create(
        self,
        service: VariableService,
        run: Run,
        indexset_names: list[str],
    ) -> None:
        ret = service.create(
            run.id,
            "Variable",
            constrained_to_indexsets=indexset_names,
        )
        assert ret.id == 1

    def test_variable_get(self, service: VariableService, run: Run) -> None:
        ret = service.get(run.id, "Variable")
        assert ret.id == 1

    def test_variable_get_by_id(self, service: VariableService) -> None:
        ret = service.get_by_id(1)
        assert ret.id == 1

    def test_variable_add_data(
        self,
        service: VariableService,
        test_data: dict[str, list[Any]],
    ) -> None:
        service.add_data(1, test_data)

    def test_variable_remove_data(
        self,
        service: VariableService,
        test_data: dict[str, list[Any]],
    ) -> None:
        service.remove_data(1, test_data)

    def test_variable_list(self, service: VariableService) -> None:
        ret = service.list()
        assert len(ret) == 1

    def test_variable_tabulate(self, service: VariableService) -> None:
        ret = service.tabulate()
        assert len(ret) == 1

    def test_variable_delete(self, service: VariableService) -> None:
        service.delete_by_id(1)
