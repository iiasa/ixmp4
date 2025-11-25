import datetime

import pandas as pd
import pandas.testing as pdt
import pytest

from ixmp4.data.unit.repositories import UnitNotFound, UnitNotUnique
from ixmp4.data.unit.service import UnitService
from tests import backends
from tests.data.base import ServiceTest

transport = backends.get_transport_fixture(scope="class")


class UnitServiceTest(ServiceTest[UnitService]):
    service_class = UnitService


class TestUnitCreate(UnitServiceTest):
    def test_unit_create(
        self, service: UnitService, fake_time: datetime.datetime
    ) -> None:
        unit = service.create("Unit")
        assert unit.name == "Unit"
        assert unit.created_at == fake_time.replace(tzinfo=None)
        assert unit.created_by == "@unknown"

    def test_unit_create_versioning(
        self, versioning_service: UnitService, fake_time: datetime.datetime
    ) -> None:
        expected_versions = pd.DataFrame(
            [
                [
                    1,
                    "Unit",
                    fake_time.replace(tzinfo=None),
                    "@unknown",
                    1,
                    None,
                    0,
                ],
            ],
            columns=[
                "id",
                "name",
                "created_at",
                "created_by",
                "transaction_id",
                "end_transaction_id",
                "operation_type",
            ],
        )
        vdf = versioning_service.pandas_versions.tabulate()
        pdt.assert_frame_equal(expected_versions, vdf, check_like=True)


class TestUnitDeleteById(UnitServiceTest):
    def test_unit_delete_by_id(
        self, service: UnitService, fake_time: datetime.datetime
    ) -> None:
        unit = service.create("Unit")
        service.delete_by_id(unit.id)
        assert service.tabulate().empty

    def test_unit_delete_by_id_versioning(
        self, versioning_service: UnitService, fake_time: datetime.datetime
    ) -> None:
        expected_versions = pd.DataFrame(
            [
                [
                    1,
                    "Unit",
                    fake_time.replace(tzinfo=None),
                    "@unknown",
                    1,
                    2,
                    0,
                ],
                [
                    1,
                    "Unit",
                    fake_time.replace(tzinfo=None),
                    "@unknown",
                    2,
                    None,
                    2,
                ],
            ],
            columns=[
                "id",
                "name",
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


class TestUnitUnique(UnitServiceTest):
    def test_unit_unique(self, service: UnitService) -> None:
        service.create("Unit")

        with pytest.raises(UnitNotUnique):
            service.create("Unit")


class TestUnitGetByName(UnitServiceTest):
    def test_unit_get_by_name(self, service: UnitService) -> None:
        unit1 = service.create("Unit")
        unit2 = service.get_by_name("Unit")
        assert unit1 == unit2


class TestUnitGetById(UnitServiceTest):
    def test_unit_get_by_id(self, service: UnitService) -> None:
        unit1 = service.create("Unit")
        unit2 = service.get_by_id(1)
        assert unit1 == unit2


class TestUnitNotFound(UnitServiceTest):
    def test_unit_not_found(self, service: UnitService) -> None:
        with pytest.raises(UnitNotFound):
            service.get_by_name("Unit")

        with pytest.raises(UnitNotFound):
            service.get_by_id(1)


class TestUnitGetOrCreate(UnitServiceTest):
    def test_unit_get_by_id(self, service: UnitService) -> None:
        unit1 = service.get_or_create("Unit")
        assert unit1.id == 1
        assert unit1.name == "Unit"

        unit2 = service.get_or_create("Unit")
        assert unit2.id == unit1.id
        assert unit2.name == unit1.name


class TestUnitList(UnitServiceTest):
    def test_unit_list(
        self, service: UnitService, fake_time: datetime.datetime
    ) -> None:
        service.create("Unit 1")
        service.create("Unit 2")

        units = service.list()

        assert units[0].id == 1
        assert units[0].name == "Unit 1"
        assert units[0].created_by == "@unknown"
        assert units[0].created_at == fake_time.replace(tzinfo=None)

        assert units[1].id == 2
        assert units[1].name == "Unit 2"
        assert units[1].created_by == "@unknown"
        assert units[1].created_at == fake_time.replace(tzinfo=None)


class TestUnitTabulate(UnitServiceTest):
    def test_unit_tabulate(
        self, service: UnitService, fake_time: datetime.datetime
    ) -> None:
        service.create("Unit 1")
        service.create("Unit 2")

        expected_units = pd.DataFrame(
            [
                [
                    1,
                    "Unit 1",
                    fake_time.replace(tzinfo=None),
                    "@unknown",
                ],
                [
                    2,
                    "Unit 2",
                    fake_time.replace(tzinfo=None),
                    "@unknown",
                ],
            ],
            columns=["id", "name", "created_at", "created_by"],
        )

        units = service.tabulate()
        pdt.assert_frame_equal(units, expected_units, check_like=True)
