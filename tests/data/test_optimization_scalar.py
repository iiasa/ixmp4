import datetime

import pandas as pd
import pandas.testing as pdt
import pytest

from ixmp4.rewrite.data.optimization.indexset.service import IndexSetService
from ixmp4.rewrite.data.optimization.scalar.repositories import (
    ScalarNotFound,
    ScalarNotUnique,
)
from ixmp4.rewrite.data.optimization.scalar.service import ScalarService
from ixmp4.rewrite.data.run.dto import Run
from ixmp4.rewrite.data.run.service import RunService
from ixmp4.rewrite.data.unit.service import Unit, UnitService
from ixmp4.rewrite.transport import Transport
from tests import backends
from tests.data.base import ServiceTest

transport = backends.get_transport_fixture(scope="class")


class ScalarServiceTest(ServiceTest[ScalarService]):
    service_class = ScalarService

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
    def units(self, transport: Transport) -> UnitService:
        return UnitService(transport)

    @pytest.fixture(scope="class")
    def unit(self, units: UnitService) -> Unit:
        unit = units.create("Unit")
        assert unit.id == 1
        return unit


class TestScalarCreate(ScalarServiceTest):
    def test_scalar_create(
        self,
        service: ScalarService,
        run: Run,
        fake_time: datetime.datetime,
        unit: Unit,
    ) -> None:
        scalar = service.create(
            run.id,
            "Scalar",
            13,
            "Unit",
        )
        assert scalar.run__id == run.id
        assert scalar.name == "Scalar"
        assert scalar.value == 13
        assert scalar.unit__id == 1
        assert scalar.unit.name == "Unit"

        assert scalar.created_at == fake_time.replace(tzinfo=None)
        assert scalar.created_by == "@unknown"

    def test_scalar_create_versioning(
        self,
        versioning_service: ScalarService,
        run: Run,
        fake_time: datetime.datetime,
    ) -> None:
        expected_versions = pd.DataFrame(
            [
                [
                    1,
                    run.id,
                    "Scalar",
                    13.0,
                    1,
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
                "value",
                "unit__id",
                "created_at",
                "created_by",
                "transaction_id",
                "end_transaction_id",
                "operation_type",
            ],
        )
        vdf = versioning_service.pandas_versions.tabulate()
        pdt.assert_frame_equal(expected_versions, vdf, check_like=True)


class TestScalarDeleteById(ScalarServiceTest):
    def test_scalar_delete_by_id(
        self,
        service: ScalarService,
        run: Run,
        unit: Unit,
        fake_time: datetime.datetime,
    ) -> None:
        scalar = service.create(
            run.id,
            "Scalar",
            13,
            "Unit",
        )
        service.delete_by_id(scalar.id)
        assert service.tabulate().empty

    def test_scalar_delete_by_id_versioning(
        self,
        versioning_service: ScalarService,
        run: Run,
        fake_time: datetime.datetime,
    ) -> None:
        expected_versions = pd.DataFrame(
            [
                [
                    1,
                    run.id,
                    "Scalar",
                    13.0,
                    1,
                    fake_time.replace(tzinfo=None),
                    "@unknown",
                    5,
                    6,
                    0,
                ],
                [
                    1,
                    run.id,
                    "Scalar",
                    13.0,
                    1,
                    fake_time.replace(tzinfo=None),
                    "@unknown",
                    6,
                    None,
                    2,
                ],
            ],
            columns=[
                "id",
                "run__id",
                "name",
                "value",
                "unit__id",
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
        # TODO Association Versions


class TestScalarUnique(ScalarServiceTest):
    def test_scalar_unique(self, service: ScalarService, run: Run, unit: Unit) -> None:
        service.create(
            run.id,
            "Scalar",
            13,
            "Unit",
        )

        with pytest.raises(ScalarNotUnique):
            service.create(
                run.id,
                "Scalar",
                42,
                "Unit",
            )


class TestScalarGetByName(ScalarServiceTest):
    def test_scalar_get(self, service: ScalarService, run: Run, unit: Unit) -> None:
        scalar1 = service.create(
            run.id,
            "Scalar",
            13,
            "Unit",
        )

        scalar2 = service.get(run.id, "Scalar")
        assert scalar1 == scalar2


class TestScalarGetById(ScalarServiceTest):
    def test_scalar_get_by_id(
        self, service: ScalarService, run: Run, unit: Unit
    ) -> None:
        scalar1 = service.create(
            run.id,
            "Scalar",
            13,
            "Unit",
        )
        scalar2 = service.get_by_id(1)
        assert scalar1 == scalar2


class TestScalarNotFound(ScalarServiceTest):
    def test_scalar_not_found(self, service: ScalarService, run: Run) -> None:
        with pytest.raises(ScalarNotFound):
            service.get(run.id, "Scalar")

        with pytest.raises(ScalarNotFound):
            service.get_by_id(1)


class ScalarDataTest(ScalarServiceTest):
    def test_scalar_update(
        self,
        service: ScalarService,
        run: Run,
        test_data: tuple[int | float, str],
        test_data_units: list[Unit],
        fake_time: datetime.datetime,
    ) -> None:
        value, unit = test_data
        scalar = service.create(
            run.id,
            "Scalar",
            value,
            unit,
        )

        assert scalar.value == value
        assert scalar.unit.name == unit

    def test_scalar_value_update(
        self,
        service: ScalarService,
        run: Run,
        test_data_value_update: int | float,
        fake_time: datetime.datetime,
    ) -> None:
        scalar = service.get(run.id, "Scalar")
        service.update_by_id(scalar.id, value=test_data_value_update)
        scalar = service.get_by_id(scalar.id)
        assert scalar.value == test_data_value_update

    def test_scalar_full_update(
        self,
        service: ScalarService,
        run: Run,
        test_data_update: tuple[int | float, str],
        fake_time: datetime.datetime,
    ) -> None:
        value, unit = test_data_update
        scalar = service.get(run.id, "Scalar")
        service.update_by_id(scalar.id, value=value, unit_name=unit)
        scalar = service.get_by_id(scalar.id)
        assert scalar.value == value

    def test_scalar_data_versioning(
        self,
        versioning_service: IndexSetService,
        run: Run,
        test_data: tuple[int | float, str],
        test_data_value_update: int | float,
        test_data_update: tuple[int | float, str],
        fake_time: datetime.datetime,
    ) -> None:
        value, _ = test_data
        full_update_value, _ = test_data_update
        # compute transaction ids
        create_tx = 6
        update_tx = create_tx + 1
        full_update_tx = update_tx + 1

        expected_versions = pd.DataFrame(
            [
                [
                    value,
                    1,
                    create_tx,
                    update_tx,
                    0,
                ],
                [
                    test_data_value_update,
                    1,
                    update_tx,
                    full_update_tx,
                    1,
                ],
                [
                    full_update_value,
                    2,
                    full_update_tx,
                    None,
                    1,
                ],
            ],
            columns=[
                "value",
                "unit__id",
                "transaction_id",
                "end_transaction_id",
                "operation_type",
            ],
        )

        expected_versions["value"] = expected_versions["value"].astype("float64")
        expected_versions["id"] = 1
        expected_versions["run__id"] = run.id
        expected_versions["name"] = "Scalar"
        expected_versions["created_at"] = pd.Timestamp(
            fake_time.replace(tzinfo=None)
        ).as_unit("ns")
        expected_versions["created_by"] = "@unknown"

        vdf = versioning_service.pandas_versions.tabulate()
        pdt.assert_frame_equal(expected_versions, vdf, check_like=True)


class TestScalarDataInteger(ScalarDataTest):
    @pytest.fixture(scope="class")
    def test_data_units(self, units: UnitService) -> list[Unit]:
        return [
            units.create("Unit 1"),
            units.create("Unit 2"),
        ]

    @pytest.fixture(scope="class")
    def test_data(self) -> tuple[int | float, str]:
        return 13, "Unit 1"

    @pytest.fixture(scope="class")
    def test_data_value_update(self) -> int | float:
        return 42

    @pytest.fixture(scope="class")
    def test_data_update(self) -> tuple[int | float, str]:
        return 1337, "Unit 2"


class TestScalarDataMixed(ScalarDataTest):
    @pytest.fixture(scope="class")
    def test_data_units(self, units: UnitService) -> list[Unit]:
        return [
            units.create("Unit 1"),
            units.create("Unit 2"),
        ]

    @pytest.fixture(scope="class")
    def test_data(self) -> tuple[int | float, str]:
        return 1.3, "Unit 1"

    @pytest.fixture(scope="class")
    def test_data_value_update(self) -> int | float:
        return 42

    @pytest.fixture(scope="class")
    def test_data_update(self) -> tuple[int | float, str]:
        return 13.37, "Unit 2"


class TestScalarDataFloat(ScalarDataTest):
    @pytest.fixture(scope="class")
    def test_data_units(self, units: UnitService) -> list[Unit]:
        return [
            units.create("Unit 1"),
            units.create("Unit 2"),
        ]

    @pytest.fixture(scope="class")
    def test_data(self) -> tuple[int | float, str]:
        return 1.3, "Unit 1"

    @pytest.fixture(scope="class")
    def test_data_value_update(self) -> int | float:
        return 42.13

    @pytest.fixture(scope="class")
    def test_data_update(self) -> tuple[int | float, str]:
        return 13.37, "Unit 2"


class TestScalarList(ScalarServiceTest):
    def test_scalar_list(
        self,
        service: ScalarService,
        run: Run,
        units: UnitService,
        fake_time: datetime.datetime,
    ) -> None:
        units.create("Unit 1")
        units.create("Unit 2")

        service.create(
            run.id,
            "Scalar 1",
            13,
            "Unit 1",
        )

        service.create(
            run.id,
            "Scalar 2",
            13.37,
            "Unit 2",
        )

        scalars = service.list()

        assert scalars[0].id == 1
        assert scalars[0].run__id == run.id
        assert scalars[0].name == "Scalar 1"
        assert scalars[0].value == 13
        assert scalars[0].unit__id == 1
        assert scalars[0].created_by == "@unknown"
        assert scalars[0].created_at == fake_time.replace(tzinfo=None)

        assert scalars[1].id == 2
        assert scalars[1].run__id == run.id
        assert scalars[1].name == "Scalar 2"
        assert scalars[1].value == 13.37
        assert scalars[1].unit__id == 2
        assert scalars[1].created_by == "@unknown"
        assert scalars[1].created_at == fake_time.replace(tzinfo=None)


class TestScalarTabulate(ScalarServiceTest):
    def test_scalar_tabulate(
        self,
        service: ScalarService,
        run: Run,
        units: UnitService,
        fake_time: datetime.datetime,
    ) -> None:
        units.create("Unit 1")
        units.create("Unit 2")

        service.create(
            run.id,
            "Scalar 1",
            13,
            "Unit 1",
        )

        service.create(
            run.id,
            "Scalar 2",
            13.37,
            "Unit 2",
        )

        expected_scalars = pd.DataFrame(
            [
                [1, "Scalar 1", 13, 1],
                [2, "Scalar 2", 13.37, 2],
            ],
            columns=["id", "name", "value", "unit__id"],
        )
        expected_scalars["run__id"] = run.id
        expected_scalars["created_at"] = pd.Timestamp(
            fake_time.replace(tzinfo=None)
        ).as_unit("ns")
        expected_scalars["created_by"] = "@unknown"

        scalars = service.tabulate()
        pdt.assert_frame_equal(scalars, expected_scalars, check_like=True)
