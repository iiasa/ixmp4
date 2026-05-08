import httpx
import pytest

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


class RunApiTest(ApiServiceTest[RunService]):
    service_class = RunService


class TestRunCreate(RunApiTest):
    def test_run_create(self, client: httpx.Client) -> None:
        created = self.request(
            client,
            "POST",
            "/runs",
            json={"model_name": "Model", "scenario_name": "Scenario"},
        ).json()

        assert created["id"] == 1
        assert created["version"] == 1
        assert created["model"]["name"] == "Model"
        assert created["scenario"]["name"] == "Scenario"


class TestRunLookup(RunApiTest):
    @pytest.fixture(scope="class")
    def run(self, direct_service: RunService) -> Run:
        return direct_service.create("Model", "Scenario")

    def test_run_get(self, client: httpx.Client, run: Run) -> None:
        got = self.request(
            client,
            "POST",
            "/runs/get",
            json={"model_name": "Model", "scenario_name": "Scenario", "version": 1},
        ).json()

        assert got["id"] == run.id

    def test_run_get_by_id(self, client: httpx.Client, run: Run) -> None:
        by_id = self.request(client, "GET", f"/runs/{run.id}").json()

        assert by_id["id"] == run.id


class TestRunQuery(RunApiTest):
    @pytest.fixture(scope="class")
    def run(self, direct_service: RunService) -> Run:
        return direct_service.create("Model", "Scenario")

    def test_run_list(self, client: httpx.Client, run: Run) -> None:
        listed = self.request(
            client, "PATCH", "/runs/list", json={"default_only": False}
        ).json()

        assert_paginated_list(listed, expected_count=1)
        assert listed["results"][0]["id"] == run.id

    def test_run_tabulate(self, client: httpx.Client, run: Run) -> None:
        tabulated = self.request(
            client,
            "PATCH",
            "/runs/tabulate",
            json={"default_only": False, "include_internal_columns": True},
        ).json()

        assert tabulated["total"] == 1
        assert_frame_payload(
            tabulated["results"],
            expected_columns={
                "id",
                "model",
                "scenario",
                "version",
                "is_default",
                "model__id",
                "scenario__id",
                "lock_transaction",
            },
        )

    def test_run_query(self, client: httpx.Client, run: Run) -> None:
        queried = self.request(
            client, "PATCH", "/runs", json={"default_only": False}
        ).json()

        assert_paginated_list(queried, expected_count=1)
        assert queried["results"][0]["id"] == run.id


class TestRunState(RunApiTest):
    @pytest.fixture(scope="class")
    def run(self, direct_service: RunService) -> Run:
        return direct_service.create("Model", "Scenario")

    def test_run_default_version(self, client: httpx.Client, run: Run) -> None:
        self.request(
            client, "POST", "/runs/set-as-default-version", json={"id": run.id}
        )
        default_run = self.request(
            client,
            "POST",
            "/runs/get-default-version",
            json={"model_name": "Model", "scenario_name": "Scenario"},
        ).json()

        assert default_run["id"] == run.id
        assert default_run["is_default"] is True

        self.request(
            client, "POST", "/runs/unset-as-default-version", json={"id": run.id}
        )
        unset = self.request(client, "GET", f"/runs/{run.id}").json()
        assert unset["is_default"] is False

    def test_run_lock_and_unlock(self, client: httpx.Client, run: Run) -> None:
        locked = self.request(client, "POST", "/runs/lock", json={"id": run.id}).json()
        assert locked["id"] == run.id
        assert locked["lock_transaction"] is not None

        unlocked = self.request(
            client, "POST", "/runs/unlock", json={"id": run.id}
        ).json()
        assert unlocked["id"] == run.id
        assert unlocked["lock_transaction"] is None


class TestRunQueryByMeta(RunApiTest):
    @pytest.fixture(scope="class")
    def runs(self, direct_transport: DirectTransport) -> tuple[Run, Run]:
        runs = RunService(direct_transport)
        meta = RunMetaEntryService(direct_transport)

        run_with_meta = runs.create("Model A", "Scenario A")
        run_without_meta = runs.create("Model B", "Scenario B")

        meta.create(run_with_meta.id, "indicator", "keep")
        return run_with_meta, run_without_meta

    def test_run_list_by_meta(
        self, client: httpx.Client, runs: tuple[Run, Run]
    ) -> None:
        run_with_meta, run_without_meta = runs

        listed = self.request(
            client,
            "PATCH",
            "/runs/list",
            json={"default_only": False, "meta": {"key": "indicator"}},
        ).json()

        assert_paginated_list(listed, expected_count=1)
        assert listed["results"][0]["id"] == run_with_meta.id
        assert listed["results"][0]["id"] != run_without_meta.id
