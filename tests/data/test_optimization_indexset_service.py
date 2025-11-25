import datetime

import pandas as pd
import pandas.testing as pdt
import pytest

from ixmp4.rewrite.data.optimization.equation.service import EquationService
from ixmp4.rewrite.data.optimization.indexset.repositories import (
    IndexSetDataInvalid,
    IndexSetNotFound,
    IndexSetNotUnique,
)
from ixmp4.rewrite.data.optimization.indexset.service import IndexSetService
from ixmp4.rewrite.data.optimization.indexset.type import Type
from ixmp4.rewrite.data.optimization.parameter.service import ParameterService
from ixmp4.rewrite.data.optimization.table.service import TableService
from ixmp4.rewrite.data.optimization.variable.service import VariableService
from ixmp4.rewrite.data.run.dto import Run
from ixmp4.rewrite.data.run.service import RunService
from ixmp4.rewrite.data.unit.service import UnitService
from ixmp4.rewrite.exceptions import InvalidArguments
from ixmp4.rewrite.transport import Transport
from tests import backends
from tests.data.base import ServiceTest

transport = backends.get_transport_fixture(scope="class")


class IndexSetServiceTest(ServiceTest[IndexSetService]):
    service_class = IndexSetService

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


class TestIndexSetCreate(IndexSetServiceTest):
    def test_indexset_create(
        self, service: IndexSetService, run: Run, fake_time: datetime.datetime
    ) -> None:
        indexset = service.create(run.id, "IndexSet")
        assert indexset.run__id == run.id
        assert indexset.name == "IndexSet"
        assert indexset.data_type is None
        assert indexset.created_at == fake_time.replace(tzinfo=None)
        assert indexset.created_by == "@unknown"

    def test_indexset_create_versioning(
        self,
        versioning_service: IndexSetService,
        run: Run,
        fake_time: datetime.datetime,
    ) -> None:
        expected_versions = pd.DataFrame(
            [
                [
                    1,
                    run.id,
                    "IndexSet",
                    None,
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
                "data_type",
                "created_at",
                "created_by",
                "transaction_id",
                "end_transaction_id",
                "operation_type",
            ],
        )
        vdf = versioning_service.pandas_versions.tabulate()
        pdt.assert_frame_equal(expected_versions, vdf, check_like=True)


class TestIndexSetDeleteById(IndexSetServiceTest):
    def test_indexset_delete_by_id(
        self, service: IndexSetService, run: Run, fake_time: datetime.datetime
    ) -> None:
        indexset = service.create(run.id, "IndexSet")
        service.delete_by_id(indexset.id)
        assert service.tabulate().empty

    def test_indexset_delete_by_id_versioning(
        self,
        versioning_service: IndexSetService,
        run: Run,
        fake_time: datetime.datetime,
    ) -> None:
        expected_versions = pd.DataFrame(
            [
                [
                    1,
                    run.id,
                    "IndexSet",
                    None,
                    fake_time.replace(tzinfo=None),
                    "@unknown",
                    4,
                    5,
                    0,
                ],
                [
                    1,
                    run.id,
                    "IndexSet",
                    None,
                    fake_time.replace(tzinfo=None),
                    "@unknown",
                    5,
                    None,
                    2,
                ],
            ],
            columns=[
                "id",
                "run__id",
                "name",
                "data_type",
                "created_at",
                "created_by",
                "transaction_id",
                "end_transaction_id",
                "operation_type",
            ],
        )
        vdf = versioning_service.pandas_versions.tabulate()
        pdt.assert_frame_equal(
            expected_versions,
            vdf,
            check_like=True,
        )


class TestIndexSetUnique(IndexSetServiceTest):
    def test_indexset_unique(self, service: IndexSetService, run: Run) -> None:
        service.create(run.id, "IndexSet")

        with pytest.raises(IndexSetNotUnique):
            service.create(run.id, "IndexSet")


class TestIndexSetGetByName(IndexSetServiceTest):
    def test_indexset_get(self, service: IndexSetService, run: Run) -> None:
        indexset1 = service.create(run.id, "IndexSet")
        indexset2 = service.get(run.id, "IndexSet")
        assert indexset1 == indexset2


class TestIndexSetGetById(IndexSetServiceTest):
    def test_indexset_get_by_id(self, service: IndexSetService, run: Run) -> None:
        indexset1 = service.create(run.id, "IndexSet")
        indexset2 = service.get_by_id(1)
        assert indexset1 == indexset2


class TestIndexSetNotFound(IndexSetServiceTest):
    def test_indexset_not_found(self, service: IndexSetService, run: Run) -> None:
        with pytest.raises(IndexSetNotFound):
            service.get(run.id, "IndexSet")

        with pytest.raises(IndexSetNotFound):
            service.get_by_id(1)


class IndexSetAddRemoveDataTest(IndexSetServiceTest):
    def test_indexset_add_data(
        self,
        service: IndexSetService,
        run: Run,
        test_data: str | int | float | list[str] | list[int] | list[float],
        fake_time: datetime.datetime,
    ) -> None:
        indexset = service.create(run.id, "IndexSet")
        service.add_data(indexset.id, test_data)
        indexset = service.get_by_id(indexset.id)
        if isinstance(test_data, list):
            assert indexset.data == test_data
        else:
            assert indexset.data == [test_data]

    def test_indexset_remove_data(
        self,
        service: IndexSetService,
        run: Run,
        test_data: str | int | float | list[str] | list[int] | list[float],
        fake_time: datetime.datetime,
    ) -> None:
        indexset = service.get(run.id, "IndexSet")
        service.remove_data(indexset.id, test_data)
        indexset = service.get(run.id, "IndexSet")
        assert indexset.data == []
        assert indexset.data_type is None

    def test_indexset_add_remove_versioning(
        self,
        versioning_service: IndexSetService,
        run: Run,
        test_data: str | int | float | list[str] | list[int] | list[float],
        test_data_type: Type,
        fake_time: datetime.datetime,
    ) -> None:
        # compute transaction ids
        data_transactions = len(test_data) if isinstance(test_data, list) else 1
        is_create_tx = 4
        first_is_update_tx = is_create_tx + 1 + data_transactions
        second_is_update_tx = first_is_update_tx + 2

        expected_versions = pd.DataFrame(
            [
                [
                    1,
                    run.id,
                    "IndexSet",
                    None,
                    fake_time.replace(tzinfo=None),
                    "@unknown",
                    is_create_tx,
                    first_is_update_tx,
                    0,
                ],
                [
                    1,
                    run.id,
                    "IndexSet",
                    test_data_type.value,
                    fake_time.replace(tzinfo=None),
                    "@unknown",
                    first_is_update_tx,
                    second_is_update_tx,
                    1,
                ],
                [
                    1,
                    run.id,
                    "IndexSet",
                    None,
                    fake_time.replace(tzinfo=None),
                    "@unknown",
                    second_is_update_tx,
                    None,
                    1,
                ],
            ],
            columns=[
                "id",
                "run__id",
                "name",
                "data_type",
                "created_at",
                "created_by",
                "transaction_id",
                "end_transaction_id",
                "operation_type",
            ],
        )
        vdf = versioning_service.pandas_versions.tabulate()
        pdt.assert_frame_equal(expected_versions, vdf, check_like=True)


class TestIndexSetComplexData(IndexSetServiceTest):
    @pytest.fixture(scope="class")
    def equations(self, transport: Transport) -> EquationService:
        return EquationService(transport)

    @pytest.fixture(scope="class")
    def parameters(self, transport: Transport) -> ParameterService:
        return ParameterService(transport)

    @pytest.fixture(scope="class")
    def units(self, transport: Transport) -> UnitService:
        return UnitService(transport)

    @pytest.fixture(scope="class")
    def tables(self, transport: Transport) -> TableService:
        return TableService(transport)

    @pytest.fixture(scope="class")
    def variables(self, transport: Transport) -> VariableService:
        return VariableService(transport)

    def test_create_indexsets(
        self,
        service: IndexSetService,
        run: Run,
        caplog: pytest.LogCaptureFixture,
    ):
        test_data1 = ["do", "re", "mi", "fa", "so", "la", "ti"]
        test_data2 = [3, 1, 4]
        indexset1 = service.create(run.id, "IndexSet 1")
        indexset2 = service.create(run.id, "IndexSet 2")

        assert indexset1.id == 1
        assert indexset2.id == 2

        service.add_data(indexset1.id, test_data1)
        service.add_data(indexset2.id, test_data2)

    def test_link_equations(
        self,
        run: Run,
        equations: EquationService,
    ):
        equation = equations.create(
            run.id,
            "Equation 1",
            constrained_to_indexsets=["IndexSet 1", "IndexSet 2"],
        )
        assert equation.id == 1

        equations.add_data(
            equation.id,
            {
                "marginals": [-2, 1, 1],
                "levels": [2, 1, 3],
                "IndexSet 1": ["do", "re", "mi"],
                "IndexSet 2": [3, 3, 1],
            },
        )

    def test_link_parameters(
        self,
        run: Run,
        parameters: ParameterService,
        units: UnitService,
    ):
        units.get_or_create("Unit 1")
        units.get_or_create("Unit 2")
        parameter = parameters.create(
            run.id,
            "Equation 1",
            constrained_to_indexsets=["IndexSet 1", "IndexSet 2"],
        )
        assert parameter.id == 1

        parameters.add_data(
            parameter.id,
            {
                "units": ["Unit 1", "Unit 1", "Unit 2", "Unit 2"],
                "values": [11.2, 2.2, 3.13, 42.1],
                "IndexSet 1": ["mi", "fa", "so", "la"],
                "IndexSet 2": [4, 3, 1, 4],
            },
        )

    def test_link_tables(
        self,
        run: Run,
        tables: TableService,
    ):
        table = tables.create(
            run.id,
            "Table 1",
            constrained_to_indexsets=["IndexSet 1", "IndexSet 2"],
        )
        assert table.id == 1

        tables.add_data(
            table.id,
            {
                "IndexSet 1": ["ti", "do", "fa"],
                "IndexSet 2": [1, 3, 4],
            },
        )

    def test_link_variables(
        self,
        run: Run,
        variables: VariableService,
    ):
        variable = variables.create(
            run.id,
            "Variable 1",
            constrained_to_indexsets=["IndexSet 1", "IndexSet 2"],
        )
        assert variable.id == 1

        variables.add_data(
            variable.id,
            {
                "marginals": [1, 2, 3],
                "levels": [3, 2, 1],
                "IndexSet 1": ["mi", "so", "la"],
                "IndexSet 2": [4, 3, 1],
            },
        )

    def test_remove_linked_data(
        self,
        service: IndexSetService,
        equations: EquationService,
        parameters: ParameterService,
        tables: TableService,
        variables: VariableService,
    ):
        service.remove_data(1, ["mi", "fa"])

        equation = equations.get_by_id(1)
        assert equation.data == {
            "marginals": [-2, 1],
            "levels": [2, 1],
            "IndexSet 1": ["do", "re"],
            "IndexSet 2": [3, 3],
        }

        parameter = parameters.get_by_id(1)
        assert parameter.data == {
            "units": ["Unit 2", "Unit 2"],
            "values": [3.13, 42.1],
            "IndexSet 1": ["so", "la"],
            "IndexSet 2": [1, 4],
        }
        table = tables.get_by_id(1)
        assert table.data == {
            "IndexSet 1": ["ti", "do"],
            "IndexSet 2": [1, 3],
        }

        variable = variables.get_by_id(1)
        assert variable.data == {
            "marginals": [2, 3],
            "levels": [2, 1],
            "IndexSet 1": ["so", "la"],
            "IndexSet 2": [3, 1],
        }

        service.remove_data(2, 3)

        equation = equations.get_by_id(1)
        assert equation.data == {}

        parameter = parameters.get_by_id(1)
        assert parameter.data == {
            "units": ["Unit 2", "Unit 2"],
            "values": [3.13, 42.1],
            "IndexSet 1": ["so", "la"],
            "IndexSet 2": [1, 4],
        }
        table = tables.get_by_id(1)
        assert table.data == {
            "IndexSet 1": ["ti"],
            "IndexSet 2": [1],
        }

        variable = variables.get_by_id(1)
        assert variable.data == {
            "marginals": [3],
            "levels": [1],
            "IndexSet 1": ["la"],
            "IndexSet 2": [1],
        }


class TestIndexSetDataString(IndexSetAddRemoveDataTest):
    @pytest.fixture(scope="class")
    def test_data_type(self) -> Type:
        return Type.STR

    @pytest.fixture(scope="class")
    def test_data(self) -> str:
        return "test"


class TestIndexSetDataInteger(IndexSetAddRemoveDataTest):
    @pytest.fixture(scope="class")
    def test_data_type(self) -> Type:
        return Type.INT

    @pytest.fixture(scope="class")
    def test_data(self) -> int:
        return 13


class TestIndexSetDataFloat(IndexSetAddRemoveDataTest):
    @pytest.fixture(scope="class")
    def test_data_type(self) -> Type:
        return Type.FLOAT

    @pytest.fixture(scope="class")
    def test_data(self) -> float:
        return 2.2


class TestIndexSetDataStringList(IndexSetAddRemoveDataTest):
    @pytest.fixture(scope="class")
    def test_data_type(self) -> Type:
        return Type.STR

    @pytest.fixture(scope="class")
    def test_data(self) -> list[str]:
        return ["one", "two", "three"]


class TestIndexSetDataIntegerList(IndexSetAddRemoveDataTest):
    @pytest.fixture(scope="class")
    def test_data_type(self) -> Type:
        return Type.INT

    @pytest.fixture(scope="class")
    def test_data(self) -> list[int]:
        return [3, 2, 1]


class TestIndexSetDataFloatList(IndexSetAddRemoveDataTest):
    @pytest.fixture(scope="class")
    def test_data_type(self) -> Type:
        return Type.FLOAT

    @pytest.fixture(scope="class")
    def test_data(self) -> list[float]:
        return [1.3, 2.2, 3.1]


class TestIndexSetAddEmptyData(IndexSetServiceTest):
    def test_indexset_add_empty_data(
        self,
        service: IndexSetService,
        run: Run,
    ) -> None:
        indexset = service.create(run.id, "IndexSet")
        service.add_data(indexset.id, [])
        indexset = service.get_by_id(indexset.id)
        assert indexset.data == []
        assert indexset.data_type is None


class TestIndexSetAddInvalidType(IndexSetServiceTest):
    def test_indexset_add_invalid_type(
        self,
        service: IndexSetService,
        run: Run,
    ) -> None:
        indexset = service.create(run.id, "IndexSet")

        # NOTE: Via HTTP, arguments are validated when the call is made from the client
        # meaning add_data will raise InvalidArguments instead of IndexSetDataInvalid.
        with pytest.raises((IndexSetDataInvalid, InvalidArguments)):
            service.add_data(indexset.id, [True])

        with pytest.raises((IndexSetDataInvalid, InvalidArguments)):
            service.add_data(
                indexset.id, [datetime.datetime.now(), datetime.datetime.now()]
            )


class TestIndexSetAppendInvalidData(IndexSetServiceTest):
    def test_indexset_append_invalid_data(
        self,
        service: IndexSetService,
        run: Run,
    ) -> None:
        indexset = service.create(run.id, "IndexSet")
        service.add_data(indexset.id, [1, 2, 3])

        with pytest.raises(IndexSetDataInvalid):
            service.add_data(indexset.id, ["one", "two", "three"])

        with pytest.raises(IndexSetDataInvalid):
            service.add_data(indexset.id, 3.142)


class TestIndexSetRemoveData(IndexSetServiceTest):
    def test_indexset_append_invalid_data(
        self,
        service: IndexSetService,
        run: Run,
    ) -> None:
        indexset = service.create(run.id, "IndexSet")
        service.add_data(indexset.id, [1, 2, 3])

        with pytest.raises(IndexSetDataInvalid):
            service.add_data(indexset.id, ["one", "two", "three"])

        with pytest.raises(IndexSetDataInvalid):
            service.add_data(indexset.id, 3.142)


class TestIndexSetList(IndexSetServiceTest):
    def test_indexset_list(
        self,
        service: IndexSetService,
        fake_time: datetime.datetime,
        run: Run,
    ) -> None:
        service.create(run.id, "IndexSet 1")
        service.create(run.id, "IndexSet 2")

        idxsets = service.list()

        assert idxsets[0].id == 1
        assert idxsets[0].name == "IndexSet 1"
        assert idxsets[0].run__id == run.id
        assert idxsets[0].data_type is None
        assert idxsets[0].created_by == "@unknown"
        assert idxsets[0].created_at == fake_time.replace(tzinfo=None)

        assert idxsets[1].id == 2
        assert idxsets[1].name == "IndexSet 2"
        assert idxsets[1].run__id == run.id
        assert idxsets[1].data_type is None
        assert idxsets[1].created_by == "@unknown"
        assert idxsets[1].created_at == fake_time.replace(tzinfo=None)


class TestIndexSetTabulate(IndexSetServiceTest):
    def test_indexset_tabulate(
        self,
        service: IndexSetService,
        fake_time: datetime.datetime,
        run: Run,
    ) -> None:
        service.create(run.id, "IndexSet 1")
        service.create(run.id, "IndexSet 2")

        expected_idxsets = pd.DataFrame(
            [
                [
                    1,
                    "IndexSet 1",
                    run.id,
                    None,
                    fake_time.replace(tzinfo=None),
                    "@unknown",
                ],
                [
                    2,
                    "IndexSet 2",
                    run.id,
                    None,
                    fake_time.replace(tzinfo=None),
                    "@unknown",
                ],
            ],
            columns=["id", "name", "run__id", "data_type", "created_at", "created_by"],
        )

        idxsets = service.tabulate()
        pdt.assert_frame_equal(idxsets, expected_idxsets, check_like=True)
