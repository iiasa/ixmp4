import httpx
import pandas as pd
import pytest

from ixmp4.data.dataframe import serialize_df
from ixmp4.data.meta.dto import RunMetaEntry
from ixmp4.data.meta.service import RunMetaEntryService
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


class RunMetaApiTest(ApiServiceTest[RunMetaEntryService]):
    service_class = RunMetaEntryService


class TestRunMetaCreate(RunMetaApiTest):
    @pytest.fixture(scope="class")
    def run(self, direct_transport: DirectTransport) -> Run:
        runs = RunService(direct_transport)
        run = runs.create("Model", "Scenario")
        runs.set_as_default_version(run.id)
        return run

    def test_run_meta_create(self, client: httpx.Client, run: Run) -> None:
        created = self.request(
            client,
            "POST",
            "/meta",
            json={"run_id": run.id, "key": "category", "value": "demo"},
        ).json()

        assert created["id"] == 1
        assert created["run__id"] == run.id
        assert created["key"] == "category"
        assert created["value"] == "demo"


class TestRunMetaLookupAndQuery(RunMetaApiTest):
    @pytest.fixture(scope="class")
    def run(self, direct_transport: DirectTransport) -> Run:
        runs = RunService(direct_transport)
        run = runs.create("Model", "Scenario")
        runs.set_as_default_version(run.id)
        return run

    @pytest.fixture(scope="class")
    def meta_entry(self, direct_service: RunMetaEntryService, run: Run) -> RunMetaEntry:
        return direct_service.create(run.id, "category", "demo")

    @pytest.mark.parametrize("method", ["POST", "PATCH"])
    def test_run_meta_get(
        self,
        client: httpx.Client,
        meta_entry: RunMetaEntry,
        run: Run,
        method: str,
    ) -> None:
        got = self.request(
            client,
            method,
            "/meta/get",
            json={"run_id": run.id, "key": meta_entry.key},
        ).json()

        assert got["id"] == meta_entry.id

    def test_run_meta_list(
        self, client: httpx.Client, meta_entry: RunMetaEntry
    ) -> None:
        listed = self.request(client, "PATCH", "/meta/list", json={}).json()

        assert_paginated_list(listed, expected_count=1)
        assert listed["results"][0]["id"] == meta_entry.id

    def test_run_meta_tabulate(
        self, client: httpx.Client, meta_entry: RunMetaEntry
    ) -> None:
        tabulated = self.request(
            client, "PATCH", "/meta/tabulate", json={"include_run_index": True}
        ).json()

        assert tabulated["total"] == 1
        assert_frame_payload(
            tabulated["results"],
            expected_columns={"id", "key", "model", "scenario"},
        )

    def test_run_meta_query(
        self, client: httpx.Client, meta_entry: RunMetaEntry
    ) -> None:
        queried = self.request(client, "PATCH", "/meta", json={}).json()

        assert_paginated_list(queried, expected_count=1)
        assert queried["results"][0]["id"] == meta_entry.id

    def test_run_meta_query_table(
        self, client: httpx.Client, meta_entry: RunMetaEntry
    ) -> None:
        query_table = self.request(
            client, "PATCH", "/meta", json={}, params={"table": "true"}
        ).json()

        assert query_table["total"] == 1
        assert_frame_payload(query_table["results"], expected_columns={"id", "key"})


class TestRunMetaBulkOperations(RunMetaApiTest):
    @pytest.fixture(scope="class")
    def run(self, direct_transport: DirectTransport) -> Run:
        runs = RunService(direct_transport)
        run = runs.create("Model", "Scenario")
        runs.set_as_default_version(run.id)
        return run

    def test_run_meta_bulk_upsert_and_delete(
        self, client: httpx.Client, run: Run
    ) -> None:
        bulk_df = pd.DataFrame(
            [[run.id, "source", "api"]],
            columns=["run__id", "key", "value"],
        )
        self.request(
            client,
            "POST",
            "/meta/bulk-upsert",
            json={"df": serialize_df(bulk_df)},
        )

        listed = self.request(client, "PATCH", "/meta/list", json={}).json()
        assert_paginated_list(listed, expected_count=1)
        assert listed["results"][0]["key"] == "source"

        bulk_delete_df = pd.DataFrame([[run.id, "source"]], columns=["run__id", "key"])
        self.request(
            client,
            "DELETE",
            "/meta/bulk-delete",
            json={"df": serialize_df(bulk_delete_df)},
        )

        listed = self.request(client, "PATCH", "/meta/list", json={}).json()
        assert_paginated_list(listed, expected_count=0)


class TestRunMetaDelete(RunMetaApiTest):
    @pytest.fixture(scope="class")
    def meta_entry(
        self,
        direct_service: RunMetaEntryService,
        direct_transport: DirectTransport,
    ) -> RunMetaEntry:
        runs = RunService(direct_transport)
        run = runs.create("Model", "Scenario")
        runs.set_as_default_version(run.id)
        return direct_service.create(run.id, "category", "demo")

    def test_run_meta_delete(
        self,
        client: httpx.Client,
        direct_service: RunMetaEntryService,
        meta_entry: RunMetaEntry,
    ) -> None:
        self.request(client, "DELETE", f"/meta/{meta_entry.id}")

        assert direct_service.tabulate().empty
