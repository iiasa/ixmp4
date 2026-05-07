import httpx
import pytest

from ixmp4.data.checkpoint.dto import Checkpoint
from ixmp4.data.checkpoint.service import CheckpointService
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


class CheckpointApiTest(ApiServiceTest[CheckpointService]):
    service_class = CheckpointService


class TestCheckpointCreate(CheckpointApiTest):
    @pytest.fixture(scope="class")
    def run(self, direct_transport: DirectTransport) -> Run:
        return RunService(direct_transport).create("Model", "Scenario")

    def test_checkpoint_create(self, client: httpx.Client, run: Run) -> None:
        created = self.request(
            client,
            "POST",
            "/checkpoints",
            json={"run__id": run.id, "message": "Checkpoint"},
        ).json()

        assert created["id"] == 1
        assert created["run__id"] == run.id
        assert created["message"] == "Checkpoint"


class TestCheckpointLookupAndQuery(CheckpointApiTest):
    @pytest.fixture(scope="class")
    def checkpoint(
        self,
        direct_service: CheckpointService,
        direct_transport: DirectTransport,
    ) -> Checkpoint:
        run = RunService(direct_transport).create("Model", "Scenario")
        return direct_service.create(run.id, "Checkpoint")

    def test_checkpoint_get_by_id(
        self, client: httpx.Client, checkpoint: Checkpoint
    ) -> None:
        got = self.request(client, "GET", f"/checkpoints/{checkpoint.id}").json()

        assert got["id"] == checkpoint.id

    def test_checkpoint_list(
        self, client: httpx.Client, checkpoint: Checkpoint
    ) -> None:
        listed = self.request(client, "PATCH", "/checkpoints/list", json={}).json()

        assert_paginated_list(listed, expected_count=1)
        assert listed["results"][0]["id"] == checkpoint.id

    def test_checkpoint_tabulate(
        self, client: httpx.Client, checkpoint: Checkpoint
    ) -> None:
        tabulated = self.request(
            client, "PATCH", "/checkpoints/tabulate", json={}
        ).json()

        assert tabulated["total"] == 1
        assert_frame_payload(
            tabulated["results"],
            expected_columns={"id", "run__id", "transaction__id", "message"},
        )


class TestCheckpointDelete(CheckpointApiTest):
    @pytest.fixture(scope="class")
    def checkpoint(
        self,
        direct_service: CheckpointService,
        direct_transport: DirectTransport,
    ) -> Checkpoint:
        run = RunService(direct_transport).create("Model", "Scenario")
        return direct_service.create(run.id, "Checkpoint")

    def test_checkpoint_delete(
        self,
        client: httpx.Client,
        direct_service: CheckpointService,
        checkpoint: Checkpoint,
    ) -> None:
        self.request(client, "DELETE", f"/checkpoints/{checkpoint.id}")

        assert direct_service.tabulate().empty
