import datetime

import pandas as pd
import pandas.testing as pdt
import pytest

from ixmp4.data.scenario.exceptions import ScenarioNotFound, ScenarioNotUnique
from ixmp4.data.scenario.service import ScenarioService
from tests import backends
from tests.data.base import ServiceTest

transport = backends.get_transport_fixture(scope="class")


class ScenarioServiceTest(ServiceTest[ScenarioService]):
    service_class = ScenarioService


class TestScenarioCreate(ScenarioServiceTest):
    def test_scenario_create(
        self, service: ScenarioService, fake_time: datetime.datetime
    ) -> None:
        scenario = service.create("Scenario")
        assert scenario.name == "Scenario"
        assert scenario.created_at == fake_time.replace(tzinfo=None)
        assert scenario.created_by == "@unknown"

    def test_scenario_create_versioning(
        self, versioning_service: ScenarioService, fake_time: datetime.datetime
    ) -> None:
        expected_versions = pd.DataFrame(
            [
                [
                    1,
                    "Scenario",
                    fake_time.replace(tzinfo=None),
                    "@unknown",
                    1,
                    None,
                    0,
                ],
            ],
            columns=[
                "id",
                "name",
                "created_at",
                "created_by",
                "transaction_id",
                "end_transaction_id",
                "operation_type",
            ],
        )
        vdf = versioning_service.versions.tabulate()
        pdt.assert_frame_equal(expected_versions, vdf, check_like=True)


class TestScenarioDeleteById(ScenarioServiceTest):
    def test_scenario_delete_by_id(
        self, service: ScenarioService, fake_time: datetime.datetime
    ) -> None:
        scenario = service.create("Scenario")
        service.delete_by_id(scenario.id)
        assert service.tabulate().empty

    def test_scenario_delete_by_id_versioning(
        self, versioning_service: ScenarioService, fake_time: datetime.datetime
    ) -> None:
        expected_versions = pd.DataFrame(
            [
                [
                    1,
                    "Scenario",
                    fake_time.replace(tzinfo=None),
                    "@unknown",
                    1,
                    2,
                    0,
                ],
                [
                    1,
                    "Scenario",
                    fake_time.replace(tzinfo=None),
                    "@unknown",
                    2,
                    None,
                    2,
                ],
            ],
            columns=[
                "id",
                "name",
                "created_at",
                "created_by",
                "transaction_id",
                "end_transaction_id",
                "operation_type",
            ],
        )
        vdf = versioning_service.versions.tabulate()
        pdt.assert_frame_equal(
            expected_versions,
            vdf,
            check_like=True,
        )


class TestScenarioUnique(ScenarioServiceTest):
    def test_scenario_unique(self, service: ScenarioService) -> None:
        service.create("Scenario")

        with pytest.raises(ScenarioNotUnique):
            service.create("Scenario")


class TestScenarioGetByName(ScenarioServiceTest):
    def test_scenario_get_by_name(self, service: ScenarioService) -> None:
        scenario1 = service.create("Scenario")
        scenario2 = service.get_by_name("Scenario")
        assert scenario1 == scenario2


class TestScenarioGetById(ScenarioServiceTest):
    def test_scenario_get_by_id(self, service: ScenarioService) -> None:
        scenario1 = service.create("Scenario")
        scenario2 = service.get_by_id(1)
        assert scenario1 == scenario2


class TestScenarioNotFound(ScenarioServiceTest):
    def test_scenario_not_found(self, service: ScenarioService) -> None:
        with pytest.raises(ScenarioNotFound):
            service.get_by_name("Scenario")

        with pytest.raises(ScenarioNotFound):
            service.get_by_id(1)


class TestScenarioList(ScenarioServiceTest):
    def test_scenario_list(
        self, service: ScenarioService, fake_time: datetime.datetime
    ) -> None:
        service.create("Scenario 1")
        service.create("Scenario 2")

        scenarios = service.list()

        assert scenarios[0].id == 1
        assert scenarios[0].name == "Scenario 1"
        assert scenarios[0].created_by == "@unknown"
        assert scenarios[0].created_at == fake_time.replace(tzinfo=None)

        assert scenarios[1].id == 2
        assert scenarios[1].name == "Scenario 2"
        assert scenarios[1].created_by == "@unknown"
        assert scenarios[1].created_at == fake_time.replace(tzinfo=None)


class TestScenarioTabulate(ScenarioServiceTest):
    def test_scenario_tabulate(
        self, service: ScenarioService, fake_time: datetime.datetime
    ) -> None:
        service.create("Scenario 1")
        service.create("Scenario 2")

        expected_scenarios = pd.DataFrame(
            [
                [
                    1,
                    "Scenario 1",
                    fake_time.replace(tzinfo=None),
                    "@unknown",
                ],
                [
                    2,
                    "Scenario 2",
                    fake_time.replace(tzinfo=None),
                    "@unknown",
                ],
            ],
            columns=["id", "name", "created_at", "created_by"],
        )

        scenarios = service.tabulate()
        pdt.assert_frame_equal(scenarios, expected_scenarios, check_like=True)
