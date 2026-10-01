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
    # the run id is taken from the URL path
    ID_IN_PATH = "id-in-path"
    # ixmp4 <=0.14.0 used a trailing slash for these routes
    ID_IN_PATH_LEGACY = "id-in-path-legacy"
    # ixmp4 <=0.16.0 took the run id from the request body
    ID_IN_BODY = "id-in-body"

    ROUTE_STYLES = [ID_IN_PATH, ID_IN_PATH_LEGACY, ID_IN_BODY]

    @pytest.fixture(scope="class")
    def run(self, direct_service: RunService) -> Run:
        return direct_service.create("Model", "Scenario")

    def post_run_action(
        self, client: httpx.Client, run: Run, action: str, style: str
    ) -> httpx.Response:
        if style == self.ID_IN_PATH:
            return self.request(client, "POST", f"/runs/{run.id}/{action}")
        elif style == self.ID_IN_PATH_LEGACY:
            return self.request(client, "POST", f"/runs/{run.id}/{action}/")
        else:
            return self.request(client, "POST", f"/runs/{action}", json={"id": run.id})

    @pytest.mark.parametrize("style", ROUTE_STYLES)
    def test_run_default_version(
        self, client: httpx.Client, run: Run, style: str
    ) -> None:
        self.post_run_action(client, run, "set-as-default-version", style)

        for method in ["POST", "PATCH"]:
            default_run = self.request(
                client,
                method,
                "/runs/get-default-version",
                json={"model_name": "Model", "scenario_name": "Scenario"},
            ).json()

            assert default_run["id"] == run.id
            assert default_run["is_default"] is True

        self.post_run_action(client, run, "unset-as-default-version", style)
        unset = self.request(client, "GET", f"/runs/{run.id}").json()
        assert unset["is_default"] is False

    @pytest.mark.parametrize("style", ROUTE_STYLES)
    def test_run_lock_and_unlock(
        self, client: httpx.Client, run: Run, style: str
    ) -> None:
        locked = self.post_run_action(client, run, "lock", style).json()
        assert locked["id"] == run.id
        assert locked["lock_transaction"] is not None

        unlocked = self.post_run_action(client, run, "unlock", style).json()
        assert unlocked["id"] == run.id
        assert unlocked["lock_transaction"] is None

    def test_run_lock_response_matches_canonical_route(
        self, client: httpx.Client, run: Run
    ) -> None:
        legacy = self.post_run_action(client, run, "lock", self.ID_IN_BODY).json()
        self.post_run_action(client, run, "unlock", self.ID_IN_PATH)
        canonical = self.post_run_action(client, run, "lock", self.ID_IN_PATH).json()
        self.post_run_action(client, run, "unlock", self.ID_IN_PATH)

        assert set(legacy) == set(canonical)
        assert legacy["lock_transaction"] is not None
        assert canonical["lock_transaction"] is not None

    @pytest.mark.parametrize(
        "action", ["lock", "unlock", "revert", "set-as-default-version"]
    )
    def test_run_compat_route_matches_canonical_error(
        self, client: httpx.Client, action: str
    ) -> None:
        """The body-id routes must run the very same procedure, auth checks included."""
        body: dict[str, object] = {"id": 9999}
        if action == "revert":
            body["transaction__id"] = 1

        canonical = client.post(
            f"runs/9999/{action}",
            json={k: v for k, v in body.items() if k != "id"},
        )
        legacy = client.post(f"runs/{action}", json=body)

        assert legacy.status_code == canonical.status_code
        assert legacy.json() == canonical.json()

    @pytest.mark.parametrize(
        "path,body",
        [
            ("/runs/lock", {}),
            ("/runs/unlock", {"other": 1}),
            ("/runs/revert", {"transaction__id": 1}),
            ("/runs/set-as-default-version", {}),
            ("/runs/unset-as-default-version", None),
        ],
    )
    def test_run_compat_route_requires_id(
        self, client: httpx.Client, path: str, body: dict[str, object] | None
    ) -> None:
        response = client.request("POST", path.lstrip("/"), json=body)

        assert response.status_code == 400
        assert response.json()["name"] == "BadRequest"
