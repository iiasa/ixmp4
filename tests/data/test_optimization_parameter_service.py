import datetime
from typing import Any

import pandas as pd
import pandas.testing as pdt
import pytest

from ixmp4.base_exceptions import Forbidden, OptimizationItemUsageError
from ixmp4.data.optimization.indexset.service import IndexSet, IndexSetService
from ixmp4.data.optimization.parameter.repositories import (
    ParameterDataInvalid,
    ParameterNotFound,
    ParameterNotUnique,
)
from ixmp4.data.optimization.parameter.service import ParameterService
from ixmp4.data.run.dto import Run
from ixmp4.data.run.service import RunService
from ixmp4.data.unit.service import Unit, UnitService
from ixmp4.transport import Transport
from tests import auth, backends
from tests.data.base import ServiceTest

transport = backends.get_transport_fixture(scope="class")


class ParameterServiceTest(ServiceTest[ParameterService]):
    service_class = ParameterService

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

    @pytest.fixture(scope="class")
    def indexset(self, run: Run, indexsets: IndexSetService) -> IndexSet:
        return indexsets.create(run.id, "IndexSet")

    @pytest.fixture(scope="class")
    def units(self, transport: Transport) -> UnitService:
        return UnitService(transport)


class TestParameterCreate(ParameterServiceTest):
    def test_parameter_create(
        self,
        service: ParameterService,
        run: Run,
        fake_time: datetime.datetime,
        indexset: IndexSet,
    ) -> None:
        parameter = service.create(
            run.id, "Parameter", constrained_to_indexsets=["IndexSet"]
        )
        assert parameter.run__id == run.id
        assert parameter.name == "Parameter"
        assert parameter.data == {}
        assert parameter.indexset_names == ["IndexSet"]
        assert parameter.column_names is None

        assert parameter.created_at == fake_time.replace(tzinfo=None)
        assert parameter.created_by == "@unknown"

    def test_parameter_create_versioning(
        self,
        versioning_service: ParameterService,
        run: Run,
        fake_time: datetime.datetime,
    ) -> None:
        expected_versions = pd.DataFrame(
            [
                [
                    1,
                    run.id,
                    "Parameter",
                    {},
                    fake_time.replace(tzinfo=None),
                    "@unknown",
                    5,
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


class TestParameterCreateInvalidArguments(ParameterServiceTest):
    def test_parameter_create_invalid_args(
        self,
        service: ParameterService,
        run: Run,
        indexset: IndexSet,
        fake_time: datetime.datetime,
    ) -> None:
        with pytest.raises(OptimizationItemUsageError, match="not equal in length"):
            service.create(
                run.id,
                "Parameter",
                constrained_to_indexsets=["IndexSet"],
                column_names=["Column 1", "Column 2"],
            )

        with pytest.raises(
            OptimizationItemUsageError, match="`column_names` are not unique"
        ):
            service.create(
                run.id,
                "Parameter",
                constrained_to_indexsets=["IndexSet", "IndexSet"],
                column_names=["Column 1", "Column 1"],
            )


class TestParameterDeleteById(ParameterServiceTest):
    def test_parameter_delete_by_id(
        self,
        service: ParameterService,
        run: Run,
        indexset: IndexSet,
        fake_time: datetime.datetime,
    ) -> None:
        parameter = service.create(
            run.id,
            "Parameter",
            constrained_to_indexsets=["IndexSet"],
        )
        service.delete_by_id(parameter.id)
        assert service.tabulate().empty

    def test_parameter_delete_by_id_versioning(
        self,
        versioning_service: ParameterService,
        run: Run,
        fake_time: datetime.datetime,
    ) -> None:
        expected_versions = pd.DataFrame(
            [
                [
                    1,
                    run.id,
                    "Parameter",
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
                    "Parameter",
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


class TestParameterUnique(ParameterServiceTest):
    def test_parameter_unique(
        self, service: ParameterService, run: Run, indexset: IndexSet
    ) -> None:
        service.create(run.id, "Parameter", constrained_to_indexsets=["IndexSet"])

        with pytest.raises(ParameterNotUnique):
            service.create(run.id, "Parameter", constrained_to_indexsets=["IndexSet"])


class TestParameterGetByName(ParameterServiceTest):
    def test_parameter_get(
        self, service: ParameterService, run: Run, indexset: IndexSet
    ) -> None:
        parameter1 = service.create(
            run.id, "Parameter", constrained_to_indexsets=["IndexSet"]
        )
        parameter2 = service.get(run.id, "Parameter")
        assert parameter1 == parameter2


class TestParameterGetById(ParameterServiceTest):
    def test_parameter_get_by_id(
        self, service: ParameterService, run: Run, indexset: IndexSet
    ) -> None:
        parameter1 = service.create(
            run.id, "Parameter", constrained_to_indexsets=["IndexSet"]
        )
        parameter2 = service.get_by_id(1)
        assert parameter1 == parameter2


class TestParameterNotFound(ParameterServiceTest):
    def test_parameter_not_found(self, service: ParameterService, run: Run) -> None:
        with pytest.raises(ParameterNotFound):
            service.get(run.id, "Parameter")

        with pytest.raises(ParameterNotFound):
            service.get_by_id(1)


class ParameterDataTest(ParameterServiceTest):
    def test_parameter_add_data(
        self,
        service: ParameterService,
        run: Run,
        test_data_indexsets: list[IndexSet],
        test_data_units: list[Unit],
        column_names: list[str] | None,
        test_data: dict[str, list[Any]] | pd.DataFrame,
        fake_time: datetime.datetime,
    ) -> None:
        parameter = service.create(
            run.id,
            "Parameter",
            constrained_to_indexsets=[i.name for i in test_data_indexsets],
            column_names=column_names,
        )
        service.add_data(parameter.id, test_data)
        parameter = service.get_by_id(parameter.id)

        if isinstance(test_data, pd.DataFrame):
            test_data = test_data.to_dict(orient="list")

        assert parameter.data == test_data

    def test_parameter_remove_data_partial(
        self,
        service: ParameterService,
        run: Run,
        partial_test_data: dict[str, list[Any]] | pd.DataFrame,
        remaining_test_data: dict[str, list[Any]],
        fake_time: datetime.datetime,
    ) -> None:
        parameter = service.get(run.id, "Parameter")
        service.remove_data(parameter.id, partial_test_data)
        parameter = service.get_by_id(parameter.id)
        assert parameter.data == remaining_test_data

    def test_parameter_remove_data_all(
        self,
        service: ParameterService,
        run: Run,
        fake_time: datetime.datetime,
    ) -> None:
        parameter = service.get(run.id, "Parameter")
        service.remove_data(parameter.id)
        parameter = service.get_by_id(parameter.id)
        assert parameter.data == {}

    def test_parameter_data_versioning(
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
            7 + len(test_data_indexsets) + sum(len(i.data) for i in test_data_indexsets)
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
        expected_versions["name"] = "Parameter"
        expected_versions["created_at"] = pd.Timestamp(
            fake_time.replace(tzinfo=None)
        ).as_unit("ns")
        expected_versions["created_by"] = "@unknown"

        vdf = versioning_service.versions.tabulate()
        pdt.assert_frame_equal(expected_versions, vdf, check_like=True)


class TestParameterData(ParameterDataTest):
    @pytest.fixture(scope="class")
    def test_data_units(self, run: Run, units: UnitService) -> list[Unit]:
        return [units.create("Unit 1"), units.create("Unit 2")]

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
    def test_data_units(self, run: Run, units: UnitService) -> list[Unit]:
        return [units.create("Unit 1"), units.create("Unit 2")]

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
    def test_data_units(self, run: Run, units: UnitService) -> list[Unit]:
        return [units.create("Unit 1"), units.create("Unit 2")]

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


class TestParameterInvalidData(ParameterServiceTest):
    def test_parameters_create(
        self,
        service: ParameterService,
        indexsets: IndexSetService,
        units: UnitService,
        run: Run,
    ) -> None:
        indexset1 = indexsets.create(run.id, "IndexSet 1")
        indexset2 = indexsets.create(run.id, "IndexSet 2")
        indexsets.add_data(indexset1.id, ["do", "re", "mi", "fa", "so", "la", "ti"])
        indexsets.add_data(indexset2.id, [3, 1, 4])

        units.create("Unit 1")
        units.create("Unit 2")

        parameter = service.create(
            run.id,
            "Parameter 1",
            constrained_to_indexsets=["IndexSet 1", "IndexSet 2"],
        )
        assert parameter.id == 1

    def test_parameter_add_invalid_data(
        self,
        service: ParameterService,
    ) -> None:
        with pytest.raises(
            OptimizationItemUsageError,
            match=r"Parameter.data must include the column\(s\): values!",
        ):
            service.add_data(
                1,
                {
                    "units": ["Unit 1"],
                    "IndexSet 1": ["do"],
                    "IndexSet 2": [3],
                },
            )
        with pytest.raises(
            OptimizationItemUsageError,
            match=r"Parameter.data must include the column\(s\): units!",
        ):
            service.add_data(
                1,
                {
                    "values": [1.2],
                    "IndexSet 1": ["do"],
                    "IndexSet 2": [3],
                },
            )

        with pytest.raises(
            OptimizationItemUsageError,
            match=r"Parameter.data must include the column\(s\): units, values!",
        ):
            service.add_data(
                1,
                {
                    "IndexSet 1": ["do"],
                    "IndexSet 2": [3],
                },
            )

        with pytest.raises(
            ParameterDataInvalid,
            match="All arrays must be of the same length",
        ):
            service.add_data(
                1,
                {
                    "units": ["Unit 1", "Unit 1", "Unit 2"],
                    "values": [1.2, 1.5, -3],
                    "IndexSet 1": ["do", "re"],  # missing "mi"
                    "IndexSet 2": [3, 3, 1],
                },
            )
        with pytest.raises(
            ParameterDataInvalid,
            match="All arrays must be of the same length",
        ):
            service.add_data(
                1,
                {
                    "units": [
                        "Unit 1",
                    ],  # missing some units
                    "values": [1.2, 1.5, -3],
                    "IndexSet 1": ["do", "re", "mi"],
                    "IndexSet 2": [3, 3, 1],
                },
            )

        with pytest.raises(ParameterDataInvalid, match="contains duplicate rows"):
            service.add_data(
                1,
                {
                    "units": ["Unit 1", "Unit 1", "Unit 2"],
                    "values": [1.2, 1.5, -3],
                    "IndexSet 1": ["do", "do", "mi"],
                    "IndexSet 2": [3, 3, 1],
                },
            )

    def test_parameter_remove_invalid_data(
        self,
        service: ParameterService,
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
                    "units": ["Unit 1"],
                    "values": [1.2],
                },
            )


class TestParameterList(ParameterServiceTest):
    def test_parameter_list(
        self,
        service: ParameterService,
        run: Run,
        indexsets: IndexSetService,
        units: UnitService,
        fake_time: datetime.datetime,
    ) -> None:
        units.create("Unit 1")
        units.create("Unit 2")

        indexset1 = indexsets.create(run.id, "IndexSet 1")
        indexset2 = indexsets.create(run.id, "IndexSet 2")
        indexsets.add_data(indexset1.id, ["do", "re", "mi", "fa", "so", "la", "ti"])
        indexsets.add_data(indexset2.id, [3, 1, 4])

        service.create(
            run.id, "Parameter 1", constrained_to_indexsets=["IndexSet 1", "IndexSet 2"]
        )
        service.create(
            run.id,
            "Parameter 2",
            constrained_to_indexsets=["IndexSet 1"],
            column_names=["Column 1"],
        )

        test_data1 = {
            "units": ["Unit 1", "Unit 1", "Unit 2"],
            "values": [1.2, 1.5, -3],
            "IndexSet 1": ["do", "re", "mi"],
            "IndexSet 2": [3, 3, 1],
        }
        service.add_data(1, test_data1)

        parameters = service.list()

        assert parameters[0].id == 1
        assert parameters[0].run__id == run.id
        assert parameters[0].name == "Parameter 1"
        assert parameters[0].data == test_data1
        assert parameters[0].created_by == "@unknown"
        assert parameters[0].created_at == fake_time.replace(tzinfo=None)

        assert parameters[1].id == 2
        assert parameters[1].run__id == run.id
        assert parameters[1].name == "Parameter 2"
        assert parameters[1].data == {}
        assert parameters[1].created_by == "@unknown"
        assert parameters[1].created_at == fake_time.replace(tzinfo=None)


class TestParameterTabulate(ParameterServiceTest):
    def test_parameter_tabulate(
        self,
        service: ParameterService,
        run: Run,
        indexsets: IndexSetService,
        units: UnitService,
        fake_time: datetime.datetime,
    ) -> None:
        units.create("Unit 1")
        units.create("Unit 2")

        indexset1 = indexsets.create(run.id, "IndexSet 1")
        indexset2 = indexsets.create(run.id, "IndexSet 2")
        indexsets.add_data(indexset1.id, ["do", "re", "mi", "fa", "so", "la", "ti"])
        indexsets.add_data(indexset2.id, [3, 1, 4])

        service.create(
            run.id, "Parameter 1", constrained_to_indexsets=["IndexSet 1", "IndexSet 2"]
        )
        service.create(
            run.id,
            "Parameter 2",
            constrained_to_indexsets=["IndexSet 1"],
            column_names=["Column 1"],
        )

        test_data1 = {
            "units": ["Unit 1", "Unit 1", "Unit 2"],
            "values": [1.2, 1.5, -3],
            "IndexSet 1": ["do", "re", "mi"],
            "IndexSet 2": [3, 3, 1],
        }
        service.add_data(1, test_data1)

        expected_parameters = pd.DataFrame(
            [
                [1, "Parameter 1", test_data1],
                [2, "Parameter 2", {}],
            ],
            columns=["id", "name", "data"],
        )
        expected_parameters["run__id"] = run.id
        expected_parameters["created_at"] = pd.Timestamp(
            fake_time.replace(tzinfo=None)
        ).as_unit("ns")
        expected_parameters["created_by"] = "@unknown"

        parameters = service.tabulate()
        pdt.assert_frame_equal(parameters, expected_parameters, check_like=True)


class ParameterAuthTest(ParameterServiceTest):
    @pytest.fixture(scope="class")
    def runs(self, transport: Transport) -> RunService:
        direct = self.get_unauthorized_direct_or_skip(transport)
        return RunService(direct)

    @pytest.fixture(scope="class")
    def indexsets(self, transport: Transport) -> IndexSetService:
        direct = self.get_unauthorized_direct_or_skip(transport)
        return IndexSetService(direct)

    @pytest.fixture(scope="class")
    def units(self, transport: Transport) -> UnitService:
        direct = self.get_unauthorized_direct_or_skip(transport)
        return UnitService(direct)

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
    def test_data(self, units: UnitService) -> dict[str, list[Any]]:
        units.create("Unit 1")
        units.create("Unit 2")

        return {
            "units": ["Unit 1", "Unit 1", "Unit 2"],
            "values": [1.2, 1.5, -3],
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


class TestParameterAuthSarahPrivate(
    auth.SarahTest, auth.PrivatePlatformTest, ParameterAuthTest
):
    def test_parameter_create(
        self, service: ParameterService, run: Run, indexset_names: list[str]
    ) -> None:
        ret = service.create(
            run.id,
            "Parameter",
            constrained_to_indexsets=indexset_names,
        )
        assert ret.id == 1

    def test_parameter_get(self, service: ParameterService, run: Run) -> None:
        ret = service.get(run.id, "Parameter")
        assert ret.id == 1

    def test_parameter_get_by_id(self, service: ParameterService) -> None:
        ret = service.get_by_id(1)
        assert ret.id == 1

    def test_parameter_add_data(
        self,
        service: ParameterService,
        test_data: dict[str, list[Any]],
    ) -> None:
        service.add_data(1, test_data)

    def test_parameter_remove_data(
        self,
        service: ParameterService,
        test_data: dict[str, list[Any]],
    ) -> None:
        service.remove_data(1, test_data)

    def test_parameter_list(self, service: ParameterService) -> None:
        ret = service.list()
        assert len(ret) == 1

    def test_parameter_tabulate(self, service: ParameterService) -> None:
        ret = service.tabulate()
        assert len(ret) == 1

    def test_parameter_delete(self, service: ParameterService) -> None:
        service.delete_by_id(1)


class TestParameterAuthAlicePrivate(
    auth.AliceTest, auth.PrivatePlatformTest, ParameterAuthTest
):
    def test_parameter_create(
        self,
        service: ParameterService,
        unauthorized_service: ParameterService,
        run: Run,
        indexset_names: list[str],
    ) -> None:
        with pytest.raises(Forbidden):
            service.create(
                run.id,
                "Parameter",
                constrained_to_indexsets=indexset_names,
            )
        ret = unauthorized_service.create(
            run.id,
            "Parameter",
            constrained_to_indexsets=indexset_names,
        )
        assert ret.id == 1

    def test_parameter_get(self, service: ParameterService, run: Run) -> None:
        with pytest.raises(Forbidden):
            service.get(run.id, "Parameter")

    def test_parameter_get_by_id(self, service: ParameterService) -> None:
        with pytest.raises(Forbidden):
            service.get_by_id(1)

    def test_parameter_add_data(
        self,
        service: ParameterService,
        unauthorized_service: ParameterService,
        test_data: dict[str, list[Any]],
    ) -> None:
        with pytest.raises(Forbidden):
            service.add_data(1, test_data)
        unauthorized_service.add_data(1, test_data)

    def test_parameter_remove_data(
        self,
        service: ParameterService,
        unauthorized_service: ParameterService,
        test_data: dict[str, list[Any]],
    ) -> None:
        with pytest.raises(Forbidden):
            service.remove_data(1, test_data)
        unauthorized_service.remove_data(1, test_data)

    def test_parameter_list(self, service: ParameterService) -> None:
        with pytest.raises(Forbidden):
            service.list()

    def test_parameter_tabulate(self, service: ParameterService) -> None:
        with pytest.raises(Forbidden):
            service.tabulate()

    def test_parameter_delete(
        self,
        service: ParameterService,
        unauthorized_service: ParameterService,
    ) -> None:
        with pytest.raises(Forbidden):
            service.delete_by_id(1)
        unauthorized_service.delete_by_id(1)


class TestParameterAuthBobPrivate(
    auth.BobTest, auth.PrivatePlatformTest, ParameterAuthTest
):
    def test_parameter_create(
        self,
        service: ParameterService,
        run: Run,
        indexset_names: list[str],
    ) -> None:
        ret = service.create(
            run.id,
            "Parameter",
            constrained_to_indexsets=indexset_names,
        )
        assert ret.id == 1

    def test_parameter_get(self, service: ParameterService, run: Run) -> None:
        ret = service.get(run.id, "Parameter")
        assert ret.id == 1

    def test_parameter_get_by_id(self, service: ParameterService) -> None:
        ret = service.get_by_id(1)
        assert ret.id == 1

    def test_parameter_add_data(
        self,
        service: ParameterService,
        test_data: dict[str, list[Any]],
    ) -> None:
        service.add_data(1, test_data)

    def test_parameter_remove_data(
        self,
        service: ParameterService,
        test_data: dict[str, list[Any]],
    ) -> None:
        service.remove_data(1, test_data)

    def test_parameter_list(self, service: ParameterService) -> None:
        ret = service.list()
        assert len(ret) == 1

    def test_parameter_tabulate(self, service: ParameterService) -> None:
        ret = service.tabulate()
        assert len(ret) == 1

    def test_parameter_delete(self, service: ParameterService) -> None:
        service.delete_by_id(1)


class TestParameterAuthCarinaPrivate(
    auth.CarinaTest, auth.PrivatePlatformTest, ParameterAuthTest
):
    def test_parameter_create(
        self,
        service: ParameterService,
        unauthorized_service: ParameterService,
        run: Run,
        run1: Run,
        run2: Run,
        indexset_names: list[str],
    ) -> None:
        with pytest.raises(Forbidden):
            service.create(
                run.id,
                "Parameter",
                constrained_to_indexsets=indexset_names,
            )
        ret = unauthorized_service.create(
            run.id,
            "Parameter",
            constrained_to_indexsets=indexset_names,
        )
        assert ret.id == 1

        ret2 = service.create(
            run1.id,
            "Parameter 1",
            constrained_to_indexsets=indexset_names,
        )
        assert ret2.id == 2

        with pytest.raises(Forbidden):
            service.create(
                run2.id,
                "Parameter 2",
                constrained_to_indexsets=indexset_names,
            )
        ret3 = unauthorized_service.create(
            run2.id,
            "Parameter 2",
            constrained_to_indexsets=indexset_names,
        )
        assert ret3.id == 3

    def test_indexset_get(
        self, service: ParameterService, run: Run, run1: Run, run2: Run
    ) -> None:
        ret = service.get(run.id, "Parameter")
        assert ret.id == 1

        ret = service.get(run1.id, "Parameter 1")
        assert ret.id == 2

        with pytest.raises(ParameterNotFound):
            service.get(run2.id, "Parameter 2")

    def test_parameter_get_by_id(self, service: ParameterService) -> None:
        ret = service.get_by_id(1)
        assert ret.id == 1
        ret2 = service.get_by_id(2)
        assert ret2.id == 2

        with pytest.raises(ParameterNotFound):
            service.get_by_id(3)

    def test_parameter_add_data(
        self,
        service: ParameterService,
        unauthorized_service: ParameterService,
        test_data: dict[str, list[Any]],
    ) -> None:
        with pytest.raises(Forbidden):
            service.add_data(1, test_data)
        unauthorized_service.add_data(1, test_data)

        service.add_data(2, test_data)

        with pytest.raises(ParameterNotFound):
            service.add_data(3, test_data)
        unauthorized_service.add_data(3, test_data)

    def test_parameter_remove_data(
        self,
        service: ParameterService,
        unauthorized_service: ParameterService,
        test_data: dict[str, list[Any]],
    ) -> None:
        with pytest.raises(Forbidden):
            service.remove_data(1, test_data)
        unauthorized_service.remove_data(1, test_data)

        service.remove_data(2, test_data)

        with pytest.raises(ParameterNotFound):
            service.remove_data(3, test_data)
        unauthorized_service.remove_data(3, test_data)

    def test_parameter_list(self, service: ParameterService) -> None:
        ret = service.list()
        assert len(ret) == 2

    def test_parameter_tabulate(self, service: ParameterService) -> None:
        ret = service.tabulate()
        assert len(ret) == 2

    def test_parameter_delete(
        self,
        service: ParameterService,
        unauthorized_service: ParameterService,
    ) -> None:
        with pytest.raises(Forbidden):
            service.delete_by_id(1)
        unauthorized_service.delete_by_id(1)

        service.delete_by_id(2)

        with pytest.raises(ParameterNotFound):
            service.delete_by_id(3)
        unauthorized_service.delete_by_id(3)


class TestParameterAuthNonePrivate(
    auth.NoneTest, auth.PrivatePlatformTest, ParameterAuthTest
):
    def test_parameter_create(
        self,
        service: ParameterService,
        unauthorized_service: ParameterService,
        run: Run,
        indexset_names: list[str],
    ) -> None:
        with pytest.raises(Forbidden):
            service.create(
                run.id,
                "Parameter",
                constrained_to_indexsets=indexset_names,
            )
        ret = unauthorized_service.create(
            run.id,
            "Parameter",
            constrained_to_indexsets=indexset_names,
        )
        assert ret.id == 1

    def test_parameter_get(self, service: ParameterService, run: Run) -> None:
        with pytest.raises(Forbidden):
            service.get(run.id, "Parameter")

    def test_parameter_get_by_id(self, service: ParameterService) -> None:
        with pytest.raises(Forbidden):
            service.get_by_id(1)

    def test_parameter_add_data(
        self,
        service: ParameterService,
        unauthorized_service: ParameterService,
        test_data: dict[str, list[Any]],
    ) -> None:
        with pytest.raises(Forbidden):
            service.add_data(1, test_data)
        unauthorized_service.add_data(1, test_data)

    def test_parameter_remove_data(
        self,
        service: ParameterService,
        unauthorized_service: ParameterService,
        test_data: dict[str, list[Any]],
    ) -> None:
        with pytest.raises(Forbidden):
            service.remove_data(1, test_data)
        unauthorized_service.add_data(1, test_data)

    def test_parameter_list(self, service: ParameterService) -> None:
        with pytest.raises(Forbidden):
            service.list()

    def test_parameter_tabulate(self, service: ParameterService) -> None:
        with pytest.raises(Forbidden):
            service.tabulate()

    def test_parameter_delete(
        self,
        service: ParameterService,
        unauthorized_service: ParameterService,
    ) -> None:
        with pytest.raises(Forbidden):
            service.delete_by_id(1)
        unauthorized_service.delete_by_id(1)


class TestRunAuthDaveGated(auth.DaveTest, auth.GatedPlatformTest, ParameterAuthTest):
    def test_parameter_create(
        self,
        service: ParameterService,
        run: Run,
        indexset_names: list[str],
    ) -> None:
        ret = service.create(
            run.id,
            "Parameter",
            constrained_to_indexsets=indexset_names,
        )
        assert ret.id == 1

    def test_parameter_get(self, service: ParameterService, run: Run) -> None:
        ret = service.get(run.id, "Parameter")
        assert ret.id == 1

    def test_parameter_get_by_id(self, service: ParameterService) -> None:
        ret = service.get_by_id(1)
        assert ret.id == 1

    def test_parameter_add_data(
        self,
        service: ParameterService,
        test_data: dict[str, list[Any]],
    ) -> None:
        service.add_data(1, test_data)

    def test_parameter_remove_data(
        self,
        service: ParameterService,
        test_data: dict[str, list[Any]],
    ) -> None:
        service.remove_data(1, test_data)

    def test_parameter_list(self, service: ParameterService) -> None:
        ret = service.list()
        assert len(ret) == 1

    def test_parameter_tabulate(self, service: ParameterService) -> None:
        ret = service.tabulate()
        assert len(ret) == 1

    def test_parameter_delete(self, service: ParameterService) -> None:
        service.delete_by_id(1)
