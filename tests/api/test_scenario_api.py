import httpx
import pytest

from ixmp4.data.scenario.dto import Scenario
from ixmp4.data.scenario.service import ScenarioService
from tests.api.base import (
    ApiServiceTest,
    api_transport,
    assert_frame_payload,
    assert_paginated_list,
)

transport = api_transport


class ScenarioApiTest(ApiServiceTest[ScenarioService]):
    service_class = ScenarioService


class TestScenarioCreate(ScenarioApiTest):
    def test_scenario_create(self, client: httpx.Client) -> None:
        created = self.request(
            client, "POST", "/scenarios", json={"name": "Scenario"}
        ).json()

        assert created["id"] == 1
        assert created["name"] == "Scenario"


class TestScenarioLookup(ScenarioApiTest):
    @pytest.fixture(scope="class")
    def scenario(self, direct_service: ScenarioService) -> Scenario:
        return direct_service.create("Scenario")

    def test_scenario_get_by_id(self, client: httpx.Client, scenario: Scenario) -> None:
        got = self.request(client, "GET", f"/scenarios/{scenario.id}").json()

        assert got["id"] == scenario.id
        assert got["name"] == scenario.name

    def test_scenario_get_by_name(
        self, client: httpx.Client, scenario: Scenario
    ) -> None:
        got = self.request(
            client, "POST", "/scenarios/get-by-name", json={"name": scenario.name}
        ).json()

        assert got["id"] == scenario.id
        assert got["name"] == scenario.name


class TestScenarioQuery(ScenarioApiTest):
    @pytest.fixture(scope="class")
    def scenarios(self, direct_service: ScenarioService) -> list[Scenario]:
        return [
            direct_service.create("Scenario 1"),
            direct_service.create("Scenario 2"),
        ]

    def test_scenario_list(
        self, client: httpx.Client, scenarios: list[Scenario]
    ) -> None:
        listed = self.request(client, "PATCH", "/scenarios/list", json={}).json()

        assert_paginated_list(listed, expected_count=2)
        assert [item["name"] for item in listed["results"]] == [
            scenario.name for scenario in scenarios
        ]

    def test_scenario_tabulate(
        self, client: httpx.Client, scenarios: list[Scenario]
    ) -> None:
        tabulated = self.request(client, "PATCH", "/scenarios/tabulate", json={}).json()

        assert tabulated["total"] == len(scenarios)
        assert_frame_payload(
            tabulated["results"],
            expected_columns={"id", "name", "created_at", "created_by"},
        )

    def test_scenario_query(
        self, client: httpx.Client, scenarios: list[Scenario]
    ) -> None:
        queried = self.request(client, "PATCH", "/scenarios", json={}).json()

        assert_paginated_list(queried, expected_count=2)
        assert [item["name"] for item in queried["results"]] == [
            scenario.name for scenario in scenarios
        ]

    def test_scenario_query_table(
        self, client: httpx.Client, scenarios: list[Scenario]
    ) -> None:
        query_table = self.request(
            client, "PATCH", "/scenarios", json={}, params={"table": "true"}
        ).json()

        assert query_table["total"] == len(scenarios)
        assert_frame_payload(
            query_table["results"],
            expected_columns={"id", "name", "created_at", "created_by"},
        )


class TestScenarioDocs(ScenarioApiTest):
    @pytest.fixture(scope="class")
    def scenario(self, direct_service: ScenarioService) -> Scenario:
        return direct_service.create("Scenario")

    @pytest.fixture(scope="class")
    def documented_scenario(
        self, direct_service: ScenarioService, scenario: Scenario
    ) -> Scenario:
        direct_service.set_docs(scenario.id, "Scenario docs")
        return scenario

    def test_scenario_set_docs(self, client: httpx.Client, scenario: Scenario) -> None:
        created = self.request(
            client,
            "POST",
            f"/scenarios/{scenario.id}/docs",
            json={"description": "Scenario docs"},
        ).json()

        assert created["dimension__id"] == scenario.id
        assert created["description"] == "Scenario docs"

    def test_scenario_get_docs(
        self, client: httpx.Client, documented_scenario: Scenario
    ) -> None:
        got = self.request(
            client, "GET", f"/scenarios/{documented_scenario.id}/docs"
        ).json()

        assert got["dimension__id"] == documented_scenario.id
        assert got["description"] == "Scenario docs"

    def test_scenario_list_docs(
        self, client: httpx.Client, documented_scenario: Scenario
    ) -> None:
        listed = self.request(client, "PATCH", "/scenarios/docs/list", json={}).json()

        assert_paginated_list(listed, expected_count=1)
        assert listed["results"][0]["dimension__id"] == documented_scenario.id

    def test_scenario_compat_docs(
        self, client: httpx.Client, documented_scenario: Scenario
    ) -> None:
        listed = self.request(
            client,
            "GET",
            "/docs/scenarios",
            params={"dimension_id": documented_scenario.id},
        ).json()

        assert_paginated_list(listed, expected_count=1)
        assert listed["results"][0]["dimension__id"] == documented_scenario.id


class TestScenarioDelete(ScenarioApiTest):
    @pytest.fixture(scope="class")
    def scenario(self, direct_service: ScenarioService) -> Scenario:
        return direct_service.create("Scenario")

    def test_scenario_delete(
        self,
        client: httpx.Client,
        direct_service: ScenarioService,
        scenario: Scenario,
    ) -> None:
        self.request(client, "DELETE", f"/scenarios/{scenario.id}")

        assert direct_service.tabulate().empty
