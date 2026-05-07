import httpx
import pytest

from ixmp4.data.region.dto import Region
from ixmp4.data.region.service import RegionService
from tests.api.base import (
    ApiServiceTest,
    api_transport,
    assert_frame_payload,
    assert_paginated_list,
)

transport = api_transport


class RegionApiTest(ApiServiceTest[RegionService]):
    service_class = RegionService


class TestRegionCreate(RegionApiTest):
    def test_region_create(self, client: httpx.Client) -> None:
        created = self.request(
            client,
            "POST",
            "/regions",
            json={"name": "Region", "hierarchy": "Hierarchy"},
        ).json()

        assert created["id"] == 1
        assert created["name"] == "Region"
        assert created["hierarchy"] == "Hierarchy"


class TestRegionLookup(RegionApiTest):
    @pytest.fixture(scope="class")
    def region(self, direct_service: RegionService) -> Region:
        return direct_service.create("Region", "Hierarchy")

    def test_region_get_by_id(self, client: httpx.Client, region: Region) -> None:
        got = self.request(client, "GET", f"/regions/{region.id}").json()

        assert got["id"] == region.id
        assert got["name"] == region.name
        assert got["hierarchy"] == region.hierarchy

    def test_region_get_by_name(self, client: httpx.Client, region: Region) -> None:
        got = self.request(
            client, "POST", "/regions/get-by-name", json={"name": region.name}
        ).json()

        assert got["id"] == region.id
        assert got["name"] == region.name
        assert got["hierarchy"] == region.hierarchy


class TestRegionQuery(RegionApiTest):
    @pytest.fixture(scope="class")
    def regions(self, direct_service: RegionService) -> list[Region]:
        return [
            direct_service.create("Region 1", "Hierarchy"),
            direct_service.create("Region 2", "Hierarchy"),
        ]

    def test_region_list(self, client: httpx.Client, regions: list[Region]) -> None:
        listed = self.request(client, "PATCH", "/regions/list", json={}).json()

        assert_paginated_list(listed, expected_count=2)
        assert [item["name"] for item in listed["results"]] == [
            region.name for region in regions
        ]

    def test_region_tabulate(self, client: httpx.Client, regions: list[Region]) -> None:
        tabulated = self.request(client, "PATCH", "/regions/tabulate", json={}).json()

        assert tabulated["total"] == len(regions)
        assert_frame_payload(
            tabulated["results"],
            expected_columns={"id", "name", "hierarchy", "created_at", "created_by"},
        )

    def test_region_query(self, client: httpx.Client, regions: list[Region]) -> None:
        queried = self.request(client, "PATCH", "/regions", json={}).json()

        assert_paginated_list(queried, expected_count=2)
        assert [item["name"] for item in queried["results"]] == [
            region.name for region in regions
        ]

    def test_region_query_table(
        self, client: httpx.Client, regions: list[Region]
    ) -> None:
        query_table = self.request(
            client, "PATCH", "/regions", json={}, params={"table": "true"}
        ).json()

        assert query_table["total"] == len(regions)
        assert_frame_payload(
            query_table["results"],
            expected_columns={"id", "name", "hierarchy", "created_at", "created_by"},
        )


class TestRegionDocs(RegionApiTest):
    @pytest.fixture(scope="class")
    def region(self, direct_service: RegionService) -> Region:
        return direct_service.create("Region", "Hierarchy")

    @pytest.fixture(scope="class")
    def documented_region(
        self, direct_service: RegionService, region: Region
    ) -> Region:
        direct_service.set_docs(region.id, "Region docs")
        return region

    def test_region_set_docs(self, client: httpx.Client, region: Region) -> None:
        created = self.request(
            client,
            "POST",
            f"/regions/{region.id}/docs",
            json={"description": "Region docs"},
        ).json()

        assert created["dimension__id"] == region.id
        assert created["description"] == "Region docs"

    def test_region_get_docs(
        self, client: httpx.Client, documented_region: Region
    ) -> None:
        got = self.request(
            client, "GET", f"/regions/{documented_region.id}/docs"
        ).json()

        assert got["dimension__id"] == documented_region.id
        assert got["description"] == "Region docs"

    def test_region_list_docs(
        self, client: httpx.Client, documented_region: Region
    ) -> None:
        listed = self.request(client, "PATCH", "/regions/docs/list", json={}).json()

        assert_paginated_list(listed, expected_count=1)
        assert listed["results"][0]["dimension__id"] == documented_region.id

    def test_region_compat_docs(
        self, client: httpx.Client, documented_region: Region
    ) -> None:
        listed = self.request(
            client,
            "GET",
            "/docs/regions",
            params={"dimension_id": documented_region.id},
        ).json()

        assert_paginated_list(listed, expected_count=1)
        assert listed["results"][0]["dimension__id"] == documented_region.id


class TestRegionDelete(RegionApiTest):
    @pytest.fixture(scope="class")
    def region(self, direct_service: RegionService) -> Region:
        return direct_service.create("Region", "Hierarchy")

    def test_region_delete(
        self,
        client: httpx.Client,
        direct_service: RegionService,
        region: Region,
    ) -> None:
        self.request(client, "DELETE", f"/regions/{region.id}")

        assert direct_service.tabulate().empty
