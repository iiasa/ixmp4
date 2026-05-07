import httpx
import pytest

from ixmp4.data.optimization.indexset.dto import IndexSet
from ixmp4.data.optimization.indexset.service import IndexSetService
from ixmp4.data.run.dto import Run
from ixmp4.data.run.service import RunService
from ixmp4.transport import DirectTransport
from tests.api.base import (
    ApiServiceTest,
    api_transport,
    assert_frame_payload,
    assert_paginated_list,
)

transport = api_transport


class IndexSetApiTest(ApiServiceTest[IndexSetService]):
    service_class = IndexSetService


class TestIndexSetCreate(IndexSetApiTest):
    @pytest.fixture(scope="class")
    def run(self, direct_transport: DirectTransport) -> Run:
        return RunService(direct_transport).create("Model", "Scenario")

    def test_indexset_create(self, client: httpx.Client, run: Run) -> None:
        created = self.request(
            client,
            "POST",
            "/optimization/indexsets",
            json={"run_id": run.id, "name": "IndexSet"},
        ).json()

        assert created["id"] == 1
        assert created["name"] == "IndexSet"


class TestIndexSetDataAndDocs(IndexSetApiTest):
    @pytest.fixture(scope="class")
    def indexset(
        self,
        direct_service: IndexSetService,
        direct_transport: DirectTransport,
    ) -> IndexSet:
        run = RunService(direct_transport).create("Model", "Scenario")
        return direct_service.create(run.id, "IndexSet")

    def test_indexset_add_and_get_data(
        self, client: httpx.Client, indexset: IndexSet
    ) -> None:
        self.request(
            client,
            "POST",
            f"/optimization/indexsets/{indexset.id}/data",
            json={"data": ["node-a"]},
        )
        by_id = self.request(
            client, "GET", f"/optimization/indexsets/{indexset.id}"
        ).json()

        assert by_id["data"] == ["node-a"]

    def test_indexset_list_and_tabulate(
        self, client: httpx.Client, indexset: IndexSet
    ) -> None:
        listed = self.request(
            client, "PATCH", "/optimization/indexsets/list", json={}
        ).json()
        assert_paginated_list(listed, expected_count=1)

        tabulated = self.request(
            client, "PATCH", "/optimization/indexsets/tabulate", json={}
        ).json()
        assert tabulated["total"] == 1
        assert_frame_payload(
            tabulated["results"],
            expected_columns={"id", "run__id", "name", "data_type"},
        )

    def test_indexset_docs(self, client: httpx.Client, indexset: IndexSet) -> None:
        created = self.request(
            client,
            "POST",
            f"/optimization/indexsets/{indexset.id}/docs",
            json={"description": "indexset docs"},
        ).json()

        assert created["dimension__id"] == indexset.id
        assert created["description"] == "indexset docs"

    def test_indexset_remove_data_and_delete_docs(
        self, client: httpx.Client, indexset: IndexSet
    ) -> None:
        self.request(
            client,
            "DELETE",
            f"/optimization/indexsets/{indexset.id}/data",
            json={"data": ["node-a"]},
        )
        self.request(client, "DELETE", f"/optimization/indexsets/{indexset.id}/docs")
