import datetime

import pandas as pd
import pandas.testing as pdt
import pytest

from ixmp4.base_exceptions import Forbidden
from ixmp4.data.checkpoint.repositories import (
    CheckpointNotFound,
)
from ixmp4.data.checkpoint.service import CheckpointService
from ixmp4.data.run.dto import Run
from ixmp4.data.run.service import RunService
from ixmp4.transport import Transport
from tests import auth, backends
from tests.data.base import ServiceTest

transport = backends.get_transport_fixture(scope="class")


class CheckpointServiceTest(ServiceTest[CheckpointService]):
    service_class = CheckpointService

    @pytest.fixture(scope="class")
    def runs(self, transport: Transport) -> RunService:
        return RunService(transport)

    @pytest.fixture(scope="class")
    def transaction__id(self, transport: Transport) -> int | None:
        direct = self.get_direct_or_skip(transport)
        assert direct.session.bind is not None
        dialect = direct.session.bind.engine.dialect

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


class CheckpointAuthTest(CheckpointServiceTest):
    @pytest.fixture(scope="class")
    def unauthorized_runs(self, transport: Transport) -> RunService:
        direct = self.get_unauthorized_direct_or_skip(transport)
        return RunService(direct)

    @pytest.fixture(scope="class")
    def model_run(self, unauthorized_runs: RunService) -> Run:
        return unauthorized_runs.create("Model", "Scenario")

    @pytest.fixture(scope="class")
    def model10_run(self, unauthorized_runs: RunService) -> Run:
        return unauthorized_runs.create("Model 10", "Scenario")

    @pytest.fixture(scope="class")
    def model2_run(self, unauthorized_runs: RunService) -> Run:
        return unauthorized_runs.create("Model 2", "Scenario")


class TestCheckpointAuthSarahPrivate(
    auth.SarahTest, auth.PrivatePlatformTest, CheckpointAuthTest
):
    def test_checkpoint_create(
        self, service: CheckpointService, model_run: Run, transaction__id: int | None
    ) -> None:
        checkpoint1 = service.create(model_run.id, "Model Checkpoint", transaction__id)
        assert checkpoint1.id == 1

    def test_checkpoint_get_by_id(self, service: CheckpointService) -> None:
        checkpoint1 = service.get_by_id(1)
        assert checkpoint1.id == 1

    def test_checkpoint_list(self, service: CheckpointService) -> None:
        results = service.list()
        assert len(results) == 1

    def test_checkpoint_tabulate(self, service: CheckpointService) -> None:
        results = service.tabulate()
        assert len(results) == 1

    def test_checkpoint_delete_by_id(self, service: CheckpointService) -> None:
        service.delete_by_id(1)


class TestCheckpointAuthAlicePrivate(
    auth.AliceTest, auth.PrivatePlatformTest, CheckpointAuthTest
):
    def test_checkpoint_create(
        self,
        service: CheckpointService,
        unauthorized_service: CheckpointService,
        model_run: Run,
        transaction__id: int | None,
    ) -> None:
        with pytest.raises(Forbidden):
            service.create(model_run.id, "Model Checkpoint", transaction__id)
        checkpoint1 = unauthorized_service.create(
            model_run.id, "Model Checkpoint", transaction__id
        )
        assert checkpoint1.id == 1

    def test_checkpoint_get_by_id(self, service: CheckpointService) -> None:
        with pytest.raises(Forbidden):
            service.get_by_id(1)

    def test_checkpoint_list(self, service: CheckpointService) -> None:
        with pytest.raises(Forbidden):
            service.list()

    def test_checkpoint_tabulate(self, service: CheckpointService) -> None:
        with pytest.raises(Forbidden):
            service.tabulate()

    def test_checkpoint_delete_by_id(self, service: CheckpointService) -> None:
        with pytest.raises(CheckpointNotFound):
            service.delete_by_id(1)


class TestCheckpointAuthBobPrivate(
    auth.BobTest, auth.PrivatePlatformTest, CheckpointAuthTest
):
    def test_checkpoint_create(
        self, service: CheckpointService, model_run: Run, transaction__id: int | None
    ) -> None:
        checkpoint1 = service.create(model_run.id, "Model Checkpoint", transaction__id)
        assert checkpoint1.id == 1

    def test_checkpoint_get_by_id(self, service: CheckpointService) -> None:
        checkpoint1 = service.get_by_id(1)
        assert checkpoint1.id == 1

    def test_checkpoint_list(self, service: CheckpointService) -> None:
        results = service.list()
        assert len(results) == 1

    def test_checkpoint_tabulate(self, service: CheckpointService) -> None:
        results = service.tabulate()
        assert len(results) == 1

    def test_checkpoint_delete_by_id(self, service: CheckpointService) -> None:
        service.delete_by_id(1)


class TestCheckpointAuthCarinaPrivate(
    auth.CarinaTest, auth.PrivatePlatformTest, CheckpointAuthTest
):
    def test_checkpoint_create(
        self,
        service: CheckpointService,
        unauthorized_service: CheckpointService,
        model_run: Run,
        model2_run: Run,
        transaction__id: int | None,
    ) -> None:
        with pytest.raises(Forbidden):
            service.create(model_run.id, "Model Checkpoint", transaction__id)

        with pytest.raises(Forbidden):
            service.create(model2_run.id, "Model 2 Checkpoint", transaction__id)

        checkpoint1 = unauthorized_service.create(
            model_run.id, "Model Checkpoint", transaction__id
        )
        checkpoint2 = unauthorized_service.create(
            model2_run.id, "Model 2 Checkpoint", transaction__id
        )

        assert checkpoint1.id == 1
        assert checkpoint2.id == 2

    def test_checkpoint_get_by_id(self, service: CheckpointService) -> None:
        checkpoint1 = service.get_by_id(1)
        assert checkpoint1.id == 1

        with pytest.raises(CheckpointNotFound):
            service.get_by_id(2)

    def test_checkpoint_list(self, service: CheckpointService) -> None:
        results = service.list()
        assert len(results) == 1

    def test_checkpoint_tabulate(self, service: CheckpointService) -> None:
        results = service.tabulate()
        assert len(results) == 1

    def test_checkpoint_delete_by_id(self, service: CheckpointService) -> None:
        with pytest.raises(Forbidden):
            service.delete_by_id(1)

        with pytest.raises(CheckpointNotFound):
            service.delete_by_id(2)


class TestCheckpointAuthDaveGated(
    auth.DaveTest, auth.GatedPlatformTest, CheckpointAuthTest
):
    def test_checkpoint_create(
        self,
        service: CheckpointService,
        model_run: Run,
        model2_run: Run,
        transaction__id: int | None,
    ) -> None:
        checkpoint1 = service.create(model_run.id, "Model Checkpoint", transaction__id)
        checkpoint2 = service.create(
            model2_run.id, "Model 2 Checkpoint", transaction__id
        )
        assert checkpoint1.id == 1
        assert checkpoint2.id == 2

    def test_checkpoint_get_by_id(self, service: CheckpointService) -> None:
        checkpoint1 = service.get_by_id(1)
        checkpoint2 = service.get_by_id(2)
        assert checkpoint1.id == 1
        assert checkpoint2.id == 2

    def test_checkpoint_list(self, service: CheckpointService) -> None:
        results = service.list()
        assert len(results) == 2

    def test_checkpoint_tabulate(self, service: CheckpointService) -> None:
        results = service.tabulate()
        assert len(results) == 2

    def test_checkpoint_delete_by_id(self, service: CheckpointService) -> None:
        service.delete_by_id(1)
        service.delete_by_id(2)
