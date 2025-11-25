import datetime

import pandas as pd
import pandas.testing as pdt
import pytest

from ixmp4.data.checkpoint.repositories import (
    CheckpointNotFound,
)
from ixmp4.data.checkpoint.service import CheckpointService
from ixmp4.data.run.service import RunService
from ixmp4.transport import DirectTransport, HttpxTransport, Transport
from tests import backends
from tests.data.base import ServiceTest

transport = backends.get_transport_fixture(scope="class")


class CheckpointServiceTest(ServiceTest[CheckpointService]):
    service_class = CheckpointService

    @pytest.fixture(scope="class")
    def runs(self, transport: Transport) -> RunService:
        return RunService(transport)

    @pytest.fixture(scope="class")
    def transaction__id(self, transport: Transport) -> int | None:
        dialect = None

        if isinstance(transport, DirectTransport):
            dialect = transport.session.bind.engine.dialect
        elif isinstance(transport, HttpxTransport):
            if transport.direct is not None:
                dialect = transport.direct.session.bind.engine.dialect

        assert dialect is not None

        # versioning only works on pg databases
        if dialect.name == "postgresql":
            return 1
        else:
            return None


class TestCheckpointCreate(CheckpointServiceTest):
    def test_checkpoint_create(
        self,
        service: CheckpointService,
        runs: RunService,
        transaction__id: int | None,
        fake_time: datetime.datetime,
    ) -> None:
        run = runs.create("Model", "Scenario")
        assert run.id == 1

        checkpoint = service.create(1, "Checkpoint message.", transaction__id)
        assert checkpoint.run__id == 1
        assert checkpoint.message == "Checkpoint message."
        assert checkpoint.transaction__id == transaction__id


class TestCheckpointDeleteById(CheckpointServiceTest):
    def test_checkpoint_delete_by_id(
        self,
        service: CheckpointService,
        runs: RunService,
        transaction__id: int | None,
        fake_time: datetime.datetime,
    ) -> None:
        run = runs.create("Model", "Scenario")
        assert run.id == 1

        checkpoint = service.create(1, "Checkpoint message.", transaction__id)
        service.delete_by_id(checkpoint.id)
        assert service.tabulate().empty


class TestCheckpointGetById(CheckpointServiceTest):
    def test_checkpoint_get_by_id(
        self,
        service: CheckpointService,
        runs: RunService,
        transaction__id: int | None,
    ) -> None:
        run = runs.create("Model", "Scenario")
        assert run.id == 1

        checkpoint1 = service.create(1, "Checkpoint message.", transaction__id)
        checkpoint2 = service.get_by_id(1)
        assert checkpoint1 == checkpoint2


class TestCheckpointNotFound(CheckpointServiceTest):
    def test_checkpoint_not_found(self, service: CheckpointService) -> None:
        with pytest.raises(CheckpointNotFound):
            service.get_by_id(1)


class TestCheckpointList(CheckpointServiceTest):
    def test_checkpoint_list(
        self,
        service: CheckpointService,
        runs: RunService,
        transaction__id: int | None,
        fake_time: datetime.datetime,
    ) -> None:
        run = runs.create("Model", "Scenario")
        assert run.id == 1

        service.create(1, "Checkpoint message one.", transaction__id)
        service.create(1, "Checkpoint message two.", transaction__id)

        checkpoints = service.list()

        assert checkpoints[0].id == 1
        assert checkpoints[0].message == "Checkpoint message one."
        assert checkpoints[0].run__id == 1
        assert checkpoints[0].transaction__id == transaction__id

        assert checkpoints[1].id == 2
        assert checkpoints[1].message == "Checkpoint message two."
        assert checkpoints[1].run__id == 1
        assert checkpoints[1].transaction__id == transaction__id


class TestCheckpointTabulate(CheckpointServiceTest):
    def test_checkpoint_tabulate(
        self,
        service: CheckpointService,
        runs: RunService,
        transaction__id: int | None,
        fake_time: datetime.datetime,
    ) -> None:
        run = runs.create("Model", "Scenario")
        assert run.id == 1

        service.create(1, "Checkpoint message one.", transaction__id)
        service.create(1, "Checkpoint message two.", transaction__id)

        expected_checkpoints = pd.DataFrame(
            [
                [
                    1,
                    1,
                    "Checkpoint message one.",
                    transaction__id,
                ],
                [
                    2,
                    1,
                    "Checkpoint message two.",
                    transaction__id,
                ],
            ],
            columns=["id", "run__id", "message", "transaction__id"],
        )

        checkpoints = service.tabulate()
        pdt.assert_frame_equal(checkpoints, expected_checkpoints, check_like=True)


# TODO: refactor to a filter test class for efficiency
# def test_filter_checkpoint(self, platform: ixmp4.Platform) -> None:
#     run1, run2 = self.filter.load_dataset(platform)

#     res = platform.backend.checkpoints.tabulate(
#         iamc={
#             "run": {"checkpoint": {"name": "Checkpoint 1"}},
#             "unit": {"name": "Unit 1"},
#         }
#     )
#     assert sorted(res["name"].tolist()) == ["Checkpoint 1", "Checkpoint 3"]

#     run2.set_as_default()
#     res = platform.backend.checkpoints.tabulate(
#         iamc={
#             "variable": {"name__in": ["Variable 3", "Variable 5"]},
#         }
#     )
#     assert sorted(res["name"].tolist()) == ["Checkpoint 2", "Checkpoint 3"]

#     run2.unset_as_default()
#     res = platform.backend.checkpoints.tabulate(
#         iamc={
#             "variable": {"name__like": "Variable *"},
#             "unit": {"name__in": ["Unit 1", "Unit 3"]},
#             "run": {
#                 "checkpoint": {"name__in": ["Checkpoint 1", "Checkpoint 2"]},
#                 "default_only": True,
#             },
#         }
#     )
#     assert res["name"].tolist() == ["Checkpoint 1", "Checkpoint 3"]

#     res = platform.backend.checkpoints.tabulate(
#         iamc={
#             "variable": {"name__like": "Variable *"},
#             "unit": {"name__in": ["Unit 1", "Unit 3"]},
#             "run": {
#                 "checkpoint": {"name__in": ["Checkpoint 1", "Checkpoint 2"]},
#                 "default_only": False,
#             },
#         }
#     )
#     assert sorted(res["name"].tolist()) == [
#         "Checkpoint 1",
#         "Checkpoint 2",
#         "Checkpoint 3",
#         "Checkpoint 4",
#     ]

#     res = platform.backend.checkpoints.tabulate(iamc=False)

#     assert res["name"].tolist() == ["Checkpoint 5"]

#     res = platform.backend.checkpoints.tabulate()

#     assert sorted(res["name"].tolist()) == [
#         "Checkpoint 1",
#         "Checkpoint 2",
#         "Checkpoint 3",
#         "Checkpoint 4",
#         "Checkpoint 5",
#     ]
