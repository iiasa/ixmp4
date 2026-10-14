import httpx
import pytest

from ixmp4.data.optimization.indexset.dto import IndexSet
from ixmp4.data.optimization.indexset.service import IndexSetService
from ixmp4.data.optimization.table.service import TableService
from ixmp4.data.run.dto import Run
from ixmp4.data.run.service import RunService
from ixmp4.transport import DirectTransport
from tests.api.base import ApiServiceTest, api_transport, assert_frame_payload

transport = api_transport


class TableApiTest(ApiServiceTest[TableService]):
    service_class = TableService


class TestTableCreate(TableApiTest):
    @pytest.fixture(scope="class")
    def run(self, direct_transport: DirectTransport) -> Run:
        return RunService(direct_transport).create("Model", "Scenario")

    @pytest.fixture(scope="class")
    def indexset(self, direct_transport: DirectTransport, run: Run) -> IndexSet:
        return IndexSetService(direct_transport).create(run.id, "IndexSet")

    def test_table_create_get_and_tabulate(
        self, client: httpx.Client, run: Run, indexset: IndexSet
    ) -> None:
        created = self.request(
            client,
            "POST",
            "/optimization/tables",
            json={
                "run_id": run.id,
                "name": "Table",
                "constrained_to_indexsets": [indexset.name],
                "column_names": ["Column"],
            },
        ).json()

        assert created["id"] == 1
        assert created["name"] == "Table"

        for method in ["POST", "PATCH"]:
            got = self.request(
                client,
                method,
                "/optimization/tables/get",
                json={"run_id": run.id, "name": "Table"},
            ).json()
            assert got["id"] == created["id"]

        tabulated = self.request(
            client, "PATCH", "/optimization/tables/tabulate", json={}
        ).json()
        assert tabulated["total"] == 1
        assert_frame_payload(
            tabulated["results"],
            expected_columns={"id", "run__id", "name"},
        )
