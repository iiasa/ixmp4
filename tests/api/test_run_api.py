import httpx
import pytest

from ixmp4.data.run.dto import Run
from ixmp4.data.run.service import RunService
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

    @pytest.mark.parametrize("method", ["POST", "PATCH"])
    def test_run_get(self, client: httpx.Client, run: Run, method: str) -> None:
        got = self.request(
            client,
            method,
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
        queried_explicit_false = self.request(
            client, "PATCH", "/runs?table=false", json={"default_only": False}
        ).json()
        assert_paginated_list(queried, expected_count=1)
        assert queried["results"][0]["id"] == run.id
        assert queried == queried_explicit_false

        tabulated = self.request(
            client, "PATCH", "/runs?table=true", json={"default_only": False}
        ).json()

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


class TestRunState(RunApiTest):
    @pytest.fixture(scope="class")
    def run(self, direct_service: RunService) -> Run:
        return direct_service.create("Model", "Scenario")

    def test_run_default_version(self, client: httpx.Client, run: Run) -> None:
        self.request(client, "POST", f"/runs/{run.id}/set-as-default")
        for method in ["POST", "PATCH"]:
            default_run = self.request(
                client,
                method,
                "/runs/get-default-version",
                json={"model_name": "Model", "scenario_name": "Scenario"},
            ).json()

            assert default_run["id"] == run.id
            assert default_run["is_default"] is True

        self.request(client, "POST", f"/runs/{run.id}/unset-as-default")
        unset = self.request(client, "GET", f"/runs/{run.id}").json()
        assert unset["is_default"] is False

    def test_run_lock_and_unlock(self, client: httpx.Client, run: Run) -> None:
        locked = self.request(client, "POST", f"/runs/{run.id}/lock").json()
        assert locked["id"] == run.id
        assert locked["lock_transaction"] is not None

        unlocked = self.request(client, "POST", f"/runs/{run.id}/unlock").json()
        assert unlocked["id"] == run.id
        assert unlocked["lock_transaction"] is None
