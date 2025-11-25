import datetime

import pandas as pd
import pandas.testing as pdt
import pytest

from ixmp4.data.run.repositories import NoDefaultRunVersion, RunNotFound
from ixmp4.data.run.service import RunService
from tests import backends
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

    def test_region_create_versioning(
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

        vdf = versioning_service.pandas_versions.tabulate()
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

        vdf = versioning_service.pandas_versions.tabulate()
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

        vdf = versioning_service.pandas_versions.tabulate(
            columns=expected_versions.columns
        )
        pdt.assert_frame_equal(expected_versions, vdf, check_like=True)


class TestRunGetRunNoDefaultVersion(RunServiceTest):
    def test_run_no_default_version(
        self, service: RunService, fake_time: datetime.datetime
    ):
        service.create("Model", "Scenario")
        with pytest.raises(NoDefaultRunVersion):
            service.get_default_version("Model", "Scenario")


class TestRunGetOrCreate(RunServiceTest):
    def test_run_get_or_create(self, service: RunService, fake_time: datetime.datetime):
        run1 = service.create("Model", "Scenario")
        run2 = service.get_or_create("Model", "Scenario")

        assert run1 != run2
        assert run2.version == 2

        service.set_as_default_version(run1.id)

        run3 = service.get_or_create("Model", "Scenario")

        # is_default has changed, so the exact equality check will fail
        assert run1.id == run3.id


class TestRunGetById(RunServiceTest):
    def test_run_get_by_id(self, service: RunService, fake_time: datetime.datetime):
        expected = service.create("Model", "Scenario")
        result = service.get_by_id(expected.id)
        assert expected == result


class TestRunNotFound(RunServiceTest):
    def test_run_not_found(self, service: RunService, fake_time: datetime.datetime):
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
        # TODO: not working yet
        # assert runs[0].updated_by == "@unknown"
        # assert runs[0].updated_at == fake_time.replace(tzinfo=None)

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
                    None,  # "@unknown",
                    pd.NaT,  # fake_time.replace(tzinfo=None),
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
