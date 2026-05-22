import httpx
import pytest

from ixmp4.data.unit.dto import Unit
from ixmp4.data.unit.service import UnitService
from tests.api.base import (
    ApiServiceTest,
    api_transport,
    assert_frame_payload,
    assert_paginated_list,
)

transport = api_transport


class UnitApiTest(ApiServiceTest[UnitService]):
    service_class = UnitService


class TestUnitCreate(UnitApiTest):
    def test_unit_create(self, client: httpx.Client) -> None:
        created = self.request(client, "POST", "/units", json={"name": "Unit"}).json()

        assert created["id"] == 1
        assert created["name"] == "Unit"


class TestUnitLookup(UnitApiTest):
    @pytest.fixture(scope="class")
    def unit(self, direct_service: UnitService) -> Unit:
        return direct_service.create("Unit")

    def test_unit_get_by_id(self, client: httpx.Client, unit: Unit) -> None:
        got = self.request(client, "GET", f"/units/{unit.id}").json()

        assert got["id"] == unit.id
        assert got["name"] == unit.name

    def test_unit_get_by_name(self, client: httpx.Client, unit: Unit) -> None:
        got = self.request(
            client, "POST", "/units/get-by-name", json={"name": unit.name}
        ).json()

        assert got["id"] == unit.id
        assert got["name"] == unit.name


class TestUnitQuery(UnitApiTest):
    @pytest.fixture(scope="class")
    def units(self, direct_service: UnitService) -> list[Unit]:
        return [direct_service.create("Unit 1"), direct_service.create("Unit 2")]

    def test_unit_list(self, client: httpx.Client, units: list[Unit]) -> None:
        listed = self.request(client, "PATCH", "/units/list", json={}).json()

        assert_paginated_list(listed, expected_count=2)
        assert [item["name"] for item in listed["results"]] == [
            unit.name for unit in units
        ]

    def test_unit_tabulate(self, client: httpx.Client, units: list[Unit]) -> None:
        tabulated = self.request(client, "PATCH", "/units/tabulate", json={}).json()

        assert tabulated["total"] == len(units)
        assert_frame_payload(
            tabulated["results"],
            expected_columns={"id", "name", "created_at", "created_by"},
        )

    def test_unit_query(self, client: httpx.Client, units: list[Unit]) -> None:
        queried = self.request(client, "PATCH", "/units", json={}).json()

        assert_paginated_list(queried, expected_count=2)
        assert [item["name"] for item in queried["results"]] == [
            unit.name for unit in units
        ]

    def test_unit_query_table(self, client: httpx.Client, units: list[Unit]) -> None:
        query_table = self.request(
            client, "PATCH", "/units", json={}, params={"table": "true"}
        ).json()

        assert query_table["total"] == len(units)
        assert_frame_payload(
            query_table["results"],
            expected_columns={"id", "name", "created_at", "created_by"},
        )


class TestUnitDocs(UnitApiTest):
    @pytest.fixture(scope="class")
    def unit(self, direct_service: UnitService) -> Unit:
        return direct_service.create("Unit")

    @pytest.fixture(scope="class")
    def documented_unit(self, direct_service: UnitService, unit: Unit) -> Unit:
        direct_service.set_docs(unit.id, "Unit docs")
        return unit

    def test_unit_set_docs(self, client: httpx.Client, unit: Unit) -> None:
        created = self.request(
            client,
            "POST",
            f"/units/{unit.id}/docs",
            json={"description": "Unit docs"},
        ).json()

        assert created["dimension__id"] == unit.id
        assert created["description"] == "Unit docs"

    def test_unit_get_docs(self, client: httpx.Client, documented_unit: Unit) -> None:
        got = self.request(client, "GET", f"/units/{documented_unit.id}/docs").json()

        assert got["dimension__id"] == documented_unit.id
        assert got["description"] == "Unit docs"

    def test_unit_list_docs(self, client: httpx.Client, documented_unit: Unit) -> None:
        listed = self.request(client, "PATCH", "/units/docs/list", json={}).json()

        assert_paginated_list(listed, expected_count=1)
        assert listed["results"][0]["dimension__id"] == documented_unit.id

    def test_unit_compat_docs(
        self, client: httpx.Client, documented_unit: Unit
    ) -> None:
        listed = self.request(
            client,
            "GET",
            "/docs/units",
            params={"dimension_id": documented_unit.id},
        ).json()

        assert_paginated_list(listed, expected_count=1)
        assert listed["results"][0]["dimension__id"] == documented_unit.id


class TestUnitDelete(UnitApiTest):
    @pytest.fixture(scope="class")
    def unit(self, direct_service: UnitService) -> Unit:
        return direct_service.create("Unit")

    def test_unit_delete(
        self,
        client: httpx.Client,
        direct_service: UnitService,
        unit: Unit,
    ) -> None:
        self.request(client, "DELETE", f"/units/{unit.id}")

        assert direct_service.tabulate().empty


class TestUnitTabulateVersionsApi(UnitApiTest):
    def test_unit_tabulate_versions(self, client: httpx.Client) -> None:
        self.request(client, "POST", "/units", json={"name": "VersionedUnit"})
        tabulated = self.request(
            client,
            "PATCH",
            "/units/versions/tabulate",
            json={},
        ).json()
        assert_frame_payload(
            tabulated["results"],
            expected_columns={
                "id",
                "name",
                "transaction_id",
                "end_transaction_id",
                "operation_type",
            },
        )
