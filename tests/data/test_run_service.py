import datetime

import pandas as pd
import pandas.testing as pdt
import pytest

from ixmp4.base_exceptions import Forbidden
from ixmp4.data.run.exceptions import NoDefaultRunVersion, RunNotFound
from ixmp4.data.run.service import RunService
from tests import auth, backends
from tests.data.base import ServiceTest

transport = backends.get_transport_fixture(scope="class")


class RunServiceTest(ServiceTest[RunService]):
    service_class = RunService


class TestRunCreate(RunServiceTest):
    def test_run_create(
        self, service: RunService, fake_time: datetime.datetime
    ) -> None:
        run = service.create("Model", "Scenario")
        assert run.model.name == "Model"
        assert run.scenario.name == "Scenario"
        assert run.version == 1
        assert not run.is_default

    def test_run_create_versioning(
        self, versioning_service: RunService, fake_time: datetime.datetime
    ) -> None:
        expected_versions = pd.DataFrame(
            [
                [
                    1,
                    1,
                    1,
                    1,
                    False,
                    fake_time.replace(tzinfo=None),
                    "@unknown",
                    pd.NaT,
                    None,
                    None,
                    3,
                    None,
                    0,
                ],
            ],
            columns=[
                "id",
                "model__id",
                "scenario__id",
                "version",
                "is_default",
                "created_at",
                "created_by",
                "updated_at",
                "updated_by",
                "lock_transaction",
                "transaction_id",
                "end_transaction_id",
                "operation_type",
            ],
        )

        vdf = versioning_service.versions.tabulate()
        pdt.assert_frame_equal(expected_versions, vdf, check_like=True)

    def test_create_run_increment_version(
        self, service: RunService, fake_time: datetime.datetime
    ) -> None:
        inc_run = service.create("Model", "Scenario")
        assert inc_run.model.name == "Model"
        assert inc_run.scenario.name == "Scenario"
        assert inc_run.version == 2
        assert not inc_run.is_default

    def test_create_run_increment_version_versioning(
        self, versioning_service: RunService, fake_time: datetime.datetime
    ) -> None:
        expected_versions = pd.DataFrame(
            [
                [
                    1,
                    1,
                    1,
                    1,
                    False,
                    fake_time.replace(tzinfo=None),
                    "@unknown",
                    pd.NaT,
                    None,
                    None,
                    3,
                    None,
                    0,
                ],
                [
                    2,
                    1,
                    1,
                    2,
                    False,
                    fake_time.replace(tzinfo=None),
                    "@unknown",
                    pd.NaT,
                    None,
                    None,
                    4,
                    None,
                    0,
                ],
            ],
            columns=[
                "id",
                "model__id",
                "scenario__id",
                "version",
                "is_default",
                "created_at",
                "created_by",
                "updated_at",
                "updated_by",
                "lock_transaction",
                "transaction_id",
                "end_transaction_id",
                "operation_type",
            ],
        )

        vdf = versioning_service.versions.tabulate()
        pdt.assert_frame_equal(expected_versions, vdf, check_like=True)


class TestRunGetRunVersions(RunServiceTest):
    def test_run_versions(
        self, service: RunService, fake_time: datetime.datetime
    ) -> None:
        run1 = service.create("Model", "Scenario")
        run2 = service.create("Model", "Scenario")
        service.set_as_default_version(run2.id)
        run3 = service.create("Model", "Scenario")

        assert run1 == service.get("Model", "Scenario", 1)

        assert run2.id == service.get("Model", "Scenario", 2).id
        assert run2.id == service.get_default_version("Model", "Scenario").id

        assert run3 == service.get("Model", "Scenario", 3)

    def test_run_versions_versioning(
        self, versioning_service: RunService, fake_time: datetime.datetime
    ) -> None:
        expected_versions = pd.DataFrame(
            [
                [1, 1, 1, 1, False, None, 3, None, 0],
                [2, 1, 1, 2, False, None, 4, 6, 0],
                [2, 1, 1, 2, True, None, 6, None, 1],
                [3, 1, 1, 3, False, None, 7, None, 0],
            ],
            columns=[
                "id",
                "model__id",
                "scenario__id",
                "version",
                "is_default",
                "lock_transaction",
                "transaction_id",
                "end_transaction_id",
                "operation_type",
            ],
        )

        vdf = versioning_service.versions.tabulate(
            columns=expected_versions.columns.to_list()
        )
        pdt.assert_frame_equal(expected_versions, vdf, check_like=True)


class TestRunGetRunNoDefaultVersion(RunServiceTest):
    def test_run_no_default_version(
        self, service: RunService, fake_time: datetime.datetime
    ) -> None:
        service.create("Model", "Scenario")
        with pytest.raises(NoDefaultRunVersion):
            service.get_default_version("Model", "Scenario")


class TestRunGetOrCreate(RunServiceTest):
    def test_run_get_or_create(
        self, service: RunService, fake_time: datetime.datetime
    ) -> None:
        run1 = service.create("Model", "Scenario")
        run2 = service.get_or_create("Model", "Scenario")

        assert run1 != run2
        assert run2.version == 2

        service.set_as_default_version(run1.id)

        run3 = service.get_or_create("Model", "Scenario")

        # is_default has changed, so the exact equality check will fail
        assert run1.id == run3.id


class TestRunGetById(RunServiceTest):
    def test_run_get_by_id(
        self, service: RunService, fake_time: datetime.datetime
    ) -> None:
        expected = service.create("Model", "Scenario")
        result = service.get_by_id(expected.id)
        assert expected == result


class TestRunNotFound(RunServiceTest):
    def test_run_not_found(
        self, service: RunService, fake_time: datetime.datetime
    ) -> None:
        with pytest.raises(RunNotFound):
            service.get_by_id(1)


class TestRunList(RunServiceTest):
    def test_run_list(self, service: RunService, fake_time: datetime.datetime) -> None:
        run = service.create("Model", "Scenario")
        service.set_as_default_version(run.id)

        service.create("Model", "Scenario")
        service.create("Other Model", "Other Scenario")

        runs = service.list()

        assert runs[0].id == 1
        assert runs[0].model.name == "Model"
        assert runs[0].scenario.name == "Scenario"
        assert runs[0].version == 1
        assert runs[0].lock_transaction is None
        assert runs[0].is_default
        assert runs[0].created_by == "@unknown"
        assert runs[0].created_at == fake_time.replace(tzinfo=None)
        assert runs[0].updated_by == "@unknown"
        assert runs[0].updated_at == fake_time.replace(tzinfo=None)

        assert runs[1].id == 2
        assert runs[1].model.name == "Model"
        assert runs[1].scenario.name == "Scenario"
        assert runs[1].version == 2
        assert runs[1].lock_transaction is None
        assert not runs[1].is_default
        assert runs[1].created_by == "@unknown"
        assert runs[1].created_at == fake_time.replace(tzinfo=None)
        assert runs[1].updated_by is None
        assert runs[1].updated_at is None

        assert runs[2].id == 3
        assert runs[2].model.name == "Other Model"
        assert runs[2].scenario.name == "Other Scenario"
        assert runs[2].version == 1
        assert runs[2].lock_transaction is None
        assert not runs[2].is_default
        assert runs[2].created_by == "@unknown"
        assert runs[2].created_at == fake_time.replace(tzinfo=None)
        assert runs[2].updated_by is None
        assert runs[2].updated_at is None


class TestRunTabulate(RunServiceTest):
    def test_run_tabulate(
        self, service: RunService, fake_time: datetime.datetime
    ) -> None:
        run = service.create("Model", "Scenario")
        service.set_as_default_version(run.id)

        service.create("Model", "Scenario")
        service.create("Other Model", "Other Scenario")

        expected_runs = pd.DataFrame(
            [
                [
                    1,
                    "Model",
                    "Scenario",
                    1,
                    None,
                    True,
                    "@unknown",
                    fake_time.replace(tzinfo=None),
                    "@unknown",
                    fake_time.replace(tzinfo=None),
                ],
                [
                    2,
                    "Model",
                    "Scenario",
                    2,
                    None,
                    False,
                    "@unknown",
                    fake_time.replace(tzinfo=None),
                    None,
                    pd.NaT,
                ],
                [
                    3,
                    "Other Model",
                    "Other Scenario",
                    1,
                    None,
                    False,
                    "@unknown",
                    fake_time.replace(tzinfo=None),
                    None,
                    pd.NaT,
                ],
            ],
            columns=[
                "id",
                "model",
                "scenario",
                "version",
                "lock_transaction",
                "is_default",
                "created_by",
                "created_at",
                "updated_by",
                "updated_at",
            ],
        )

        runs = service.tabulate().drop(columns=["model__id", "scenario__id"])
        pdt.assert_frame_equal(expected_runs, runs, check_like=True)


# TODO: lock, unlock, clone


class TestRunAuthSarahPrivate(auth.SarahTest, auth.PrivatePlatformTest, RunServiceTest):
    def test_run_create(self, service: RunService) -> None:
        run = service.create("Model", "Scenario")
        assert run.id == 1
        assert run.version == 1
        assert run.created_by == "superuser_sarah"

    def test_run_get(self, service: RunService) -> None:
        run = service.get("Model", "Scenario", 1)
        assert run.id == 1

    def test_run_set_as_default_version(self, service: RunService) -> None:
        service.set_as_default_version(1)

    def test_run_get_default_version(self, service: RunService) -> None:
        run = service.get_default_version("Model", "Scenario")
        assert run.id == 1

    def test_run_unset_as_default_version(self, service: RunService) -> None:
        service.unset_as_default_version(1)

    def test_run_get_by_id(self, service: RunService) -> None:
        run = service.get_by_id(1)
        assert run.id == 1

    def test_run_lock(self, service: RunService) -> None:
        service.lock(1)

    def test_run_unlock(self, service: RunService) -> None:
        service.unlock(1)

    def test_run_clone(self, service: RunService) -> None:
        cloned_run = service.clone(1)
        assert cloned_run.id == 2

    def test_run_revert(self, versioning_service: RunService) -> None:
        versioning_service.revert(1, 1)

    def test_run_list(self, service: RunService) -> None:
        results = service.list()
        assert len(results) == 1

    def test_run_tabulate(self, service: RunService) -> None:
        results = service.tabulate()
        assert len(results) == 1

    def test_run_delete(self, service: RunService) -> None:
        service.delete_by_id(1)


class TestRunAuthAlicePrivate(auth.AliceTest, auth.PrivatePlatformTest, RunServiceTest):
    def test_run_create(
        self, service: RunService, unauthorized_service: RunService
    ) -> None:
        with pytest.raises(Forbidden):
            service.create("Model", "Scenario")
        run = unauthorized_service.create("Model", "Scenario")
        assert run.id == 1

    def test_run_get(self, service: RunService) -> None:
        with pytest.raises(Forbidden):
            service.get("Model", "Scenario", 1)

    def test_run_set_as_default_version(
        self, service: RunService, unauthorized_service: RunService
    ) -> None:
        with pytest.raises(RunNotFound):
            service.set_as_default_version(1)
        unauthorized_service.set_as_default_version(1)

    def test_run_get_default_version(self, service: RunService) -> None:
        with pytest.raises(Forbidden):
            service.get_default_version("Model", "Scenario")

    def test_run_unset_as_default_version(
        self, service: RunService, unauthorized_service: RunService
    ) -> None:
        with pytest.raises(RunNotFound):
            service.unset_as_default_version(1)
        unauthorized_service.unset_as_default_version(1)

    def test_run_get_by_id(self, service: RunService) -> None:
        with pytest.raises(Forbidden):
            service.get_by_id(1)

    def test_run_lock(
        self, service: RunService, unauthorized_service: RunService
    ) -> None:
        with pytest.raises(RunNotFound):
            service.lock(1)
        unauthorized_service.lock(1)

    def test_run_unlock(
        self, service: RunService, unauthorized_service: RunService
    ) -> None:
        with pytest.raises(RunNotFound):
            service.unlock(1)
        unauthorized_service.unlock(1)

    def test_run_clone(self, service: RunService) -> None:
        with pytest.raises(RunNotFound):
            service.clone(1)

    def test_run_revert(self, versioning_service: RunService) -> None:
        with pytest.raises(RunNotFound):
            versioning_service.revert(1, 12)

    def test_run_list(self, service: RunService) -> None:
        with pytest.raises(Forbidden):
            service.list()

    def test_run_tabulate(self, service: RunService) -> None:
        with pytest.raises(Forbidden):
            service.tabulate()

    def test_run_delete(self, service: RunService) -> None:
        with pytest.raises(Forbidden):
            service.delete_by_id(1)


class TestRunAuthBobPrivate(auth.BobTest, auth.PrivatePlatformTest, RunServiceTest):
    def test_run_create(self, service: RunService) -> None:
        run = service.create("Model", "Scenario")
        assert run.id == 1
        assert run.version == 1
        assert run.created_by == "staffuser_bob"

    def test_run_get(self, service: RunService) -> None:
        run = service.get("Model", "Scenario", 1)
        assert run.id == 1

    def test_run_set_as_default_version(self, service: RunService) -> None:
        service.set_as_default_version(1)

    def test_run_get_default_version(self, service: RunService) -> None:
        run = service.get_default_version("Model", "Scenario")
        assert run.id == 1

    def test_run_unset_as_default_version(self, service: RunService) -> None:
        service.unset_as_default_version(1)

    def test_run_get_by_id(self, service: RunService) -> None:
        run = service.get_by_id(1)
        assert run.id == 1

    def test_run_lock(self, service: RunService) -> None:
        service.lock(1)

    def test_run_unlock(self, service: RunService) -> None:
        service.unlock(1)

    def test_run_clone(self, service: RunService) -> None:
        cloned_run = service.clone(1)
        assert cloned_run.id == 2

    def test_run_revert(self, versioning_service: RunService) -> None:
        versioning_service.revert(1, 1)

    def test_run_list(self, service: RunService) -> None:
        results = service.list()
        assert len(results) == 1

    def test_run_tabulate(self, service: RunService) -> None:
        results = service.tabulate()
        assert len(results) == 1

    def test_run_delete(self, service: RunService) -> None:
        service.delete_by_id(1)


class TestRunAuthCarinaPrivate(
    auth.CarinaTest, auth.PrivatePlatformTest, RunServiceTest
):
    def test_run_create(
        self, service: RunService, unauthorized_service: RunService
    ) -> None:
        with pytest.raises(Forbidden):
            service.create("Model", "Scenario")

        run1 = unauthorized_service.create("Model", "Scenario")
        run2 = unauthorized_service.create("Other Model", "Scenario")

        run3 = service.create("Model 10", "Scenario")

        assert run1.id == 1
        assert run2.id == 2
        assert run3.id == 3

    def test_run_get(self, service: RunService) -> None:
        run1 = service.get("Model", "Scenario", 1)
        assert run1.id == 1

        with pytest.raises(Forbidden):
            service.get("Other Model", "Scenario", 1)

        run3 = service.get("Model 10", "Scenario", 1)
        assert run3.id == 3

    def test_run_set_as_default_version(
        self, service: RunService, unauthorized_service: RunService
    ) -> None:
        with pytest.raises(Forbidden):
            service.set_as_default_version(1)
        unauthorized_service.set_as_default_version(1)

        service.set_as_default_version(3)

    def test_run_get_default_version(self, service: RunService) -> None:
        run1 = service.get_default_version("Model", "Scenario")
        assert run1.id == 1

        with pytest.raises(Forbidden):
            service.get_default_version("Other Model", "Scenario")

        run3 = service.get_default_version("Model 10", "Scenario")
        assert run3.id == 3

    def test_run_unset_as_default_version(
        self, service: RunService, unauthorized_service: RunService
    ) -> None:
        with pytest.raises(Forbidden):
            service.unset_as_default_version(1)
        unauthorized_service.unset_as_default_version(1)

        service.unset_as_default_version(3)

    def test_run_get_by_id(self, service: RunService) -> None:
        run1 = service.get_by_id(1)
        assert run1.id == 1

        with pytest.raises(RunNotFound):
            service.get_by_id(2)

        run3 = service.get_by_id(3)
        assert run3.id == 3

    def test_run_lock(
        self, service: RunService, unauthorized_service: RunService
    ) -> None:
        with pytest.raises(Forbidden):
            service.lock(1)
        unauthorized_service.lock(1)

        service.lock(3)

    def test_run_unlock(
        self, service: RunService, unauthorized_service: RunService
    ) -> None:
        with pytest.raises(Forbidden):
            service.unlock(1)
        unauthorized_service.unlock(1)

        service.unlock(3)

    def test_run_clone(
        self, service: RunService, unauthorized_service: RunService
    ) -> None:
        with pytest.raises(Forbidden):
            service.clone(1)
        cloned_run = unauthorized_service.clone(1)
        assert cloned_run.id == 4

    def test_run_revert(self, versioning_service: RunService) -> None:
        with pytest.raises(Forbidden):
            versioning_service.revert(1, 1)

        with pytest.raises(RunNotFound):
            versioning_service.revert(2, 1)

        versioning_service.revert(3, 1)

    def test_run_list(self, service: RunService) -> None:
        results = service.list()
        assert len(results) == 2

    def test_run_tabulate(self, service: RunService) -> None:
        results = service.tabulate()
        assert len(results) == 2

    def test_run_delete(self, service: RunService) -> None:
        with pytest.raises(Forbidden):
            service.delete_by_id(1)

        with pytest.raises(Forbidden):
            service.delete_by_id(2)

        with pytest.raises(Forbidden):
            service.delete_by_id(3)


class TestRunAuthNonePrivate(auth.NoneTest, auth.PrivatePlatformTest, RunServiceTest):
    def test_run_create(
        self, service: RunService, unauthorized_service: RunService
    ) -> None:
        with pytest.raises(Forbidden):
            service.create("Model", "Scenario")
        unauthorized_service.create("Model", "Scenario")

    def test_run_get(self, service: RunService) -> None:
        with pytest.raises(Forbidden):
            service.get("Model", "Scenario", 1)

    def test_run_set_as_default_version(
        self, service: RunService, unauthorized_service: RunService
    ) -> None:
        with pytest.raises(RunNotFound):
            service.set_as_default_version(1)
        unauthorized_service.set_as_default_version(1)

    def test_run_get_default_version(self, service: RunService) -> None:
        with pytest.raises(Forbidden):
            service.get_default_version("Model", "Scenario")

    def test_run_unset_as_default_version(
        self, service: RunService, unauthorized_service: RunService
    ) -> None:
        with pytest.raises(RunNotFound):
            service.unset_as_default_version(1)
        unauthorized_service.unset_as_default_version(1)

    def test_run_get_by_id(self, service: RunService) -> None:
        with pytest.raises(Forbidden):
            service.get_by_id(1)

    def test_run_lock(
        self, service: RunService, unauthorized_service: RunService
    ) -> None:
        with pytest.raises(RunNotFound):
            service.lock(1)
        unauthorized_service.lock(1)

    def test_run_unlock(
        self, service: RunService, unauthorized_service: RunService
    ) -> None:
        with pytest.raises(RunNotFound):
            service.unlock(1)
        unauthorized_service.unlock(1)

    def test_run_clone(self, service: RunService) -> None:
        with pytest.raises(RunNotFound):
            service.clone(1)

    def test_run_revert(
        self, versioning_service: RunService, unauthorized_service: RunService
    ) -> None:
        with pytest.raises(RunNotFound):
            versioning_service.revert(1, 1)
        unauthorized_service.revert(1, 1)

    def test_run_list(self, service: RunService) -> None:
        with pytest.raises(Forbidden):
            service.list()

    def test_run_tabulate(self, service: RunService) -> None:
        with pytest.raises(Forbidden):
            service.tabulate()

    def test_run_delete(self, service: RunService) -> None:
        with pytest.raises(Forbidden):
            service.delete_by_id(1)


class TestRunAuthDaveGated(auth.DaveTest, auth.GatedPlatformTest, RunServiceTest):
    def test_run_create(self, service: RunService) -> None:
        run = service.create("Model", "Scenario")
        assert run.id == 1
        assert run.version == 1
        assert run.created_by == "user_dave"

    def test_run_get(self, service: RunService) -> None:
        run = service.get("Model", "Scenario", 1)
        assert run.id == 1

    def test_run_set_as_default_version(self, service: RunService) -> None:
        service.set_as_default_version(1)

    def test_run_get_default_version(self, service: RunService) -> None:
        run = service.get_default_version("Model", "Scenario")
        assert run.id == 1

    def test_run_unset_as_default_version(self, service: RunService) -> None:
        service.unset_as_default_version(1)

    def test_run_get_by_id(self, service: RunService) -> None:
        run = service.get_by_id(1)
        assert run.id == 1

    def test_run_lock(self, service: RunService) -> None:
        service.lock(1)

    def test_run_unlock(self, service: RunService) -> None:
        service.unlock(1)

    def test_run_clone(self, service: RunService) -> None:
        cloned_run = service.clone(1)
        assert cloned_run.id == 2

    def test_run_revert(self, versioning_service: RunService) -> None:
        versioning_service.revert(1, 1)

    def test_run_list(self, service: RunService) -> None:
        results = service.list()
        assert len(results) == 1

    def test_run_tabulate(self, service: RunService) -> None:
        results = service.tabulate()
        assert len(results) == 1

    def test_run_delete(self, service: RunService) -> None:
        with pytest.raises(Forbidden):
            service.delete_by_id(1)
