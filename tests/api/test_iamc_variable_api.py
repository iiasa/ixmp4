import httpx
import pandas as pd
import pytest

from ixmp4.data.iamc.timeseries.service import TimeSeriesService
from ixmp4.data.iamc.variable.dto import Variable
from ixmp4.data.iamc.variable.service import VariableService
from ixmp4.data.region.service import RegionService
from ixmp4.data.run.service import RunService
from ixmp4.data.unit.service import UnitService
from ixmp4.transport import DirectTransport
from tests.api.base import (
    ApiServiceTest,
    api_transport,
    assert_frame_payload,
    assert_paginated_list,
)

transport = api_transport


class IamcVariableApiTest(ApiServiceTest[VariableService]):
    service_class = VariableService


def create_variable_query_data(direct_transport: DirectTransport, name: str) -> None:
    runs = RunService(direct_transport)
    run = runs.create("Model", "Scenario")
    runs.set_as_default_version(run.id)
    RegionService(direct_transport).create("Region 1", "default")
    UnitService(direct_transport).create("Unit 1")
    TimeSeriesService(direct_transport).bulk_upsert(
        pd.DataFrame(
            [[run.id, "Region 1", name, "Unit 1"]],
            columns=["run__id", "region", "variable", "unit"],
        )
    )


class TestIamcVariableCreate(IamcVariableApiTest):
    def test_iamc_variable_create(self, client: httpx.Client) -> None:
        created = self.request(
            client, "POST", "/iamc/variables", json={"name": "Variable"}
        ).json()

        assert created["id"] == 1
        assert created["name"] == "Variable"


class TestIamcVariableLookup(IamcVariableApiTest):
    @pytest.fixture(scope="class")
    def variable(self, direct_service: VariableService) -> Variable:
        return direct_service.create("Variable")

    def test_iamc_variable_get_by_id(
        self, client: httpx.Client, variable: Variable
    ) -> None:
        got = self.request(client, "GET", f"/iamc/variables/{variable.id}").json()

        assert got["id"] == variable.id
        assert got["name"] == variable.name

    def test_iamc_variable_get_by_name(
        self, client: httpx.Client, variable: Variable
    ) -> None:
        got = self.request(
            client,
            "POST",
            "/iamc/variables/get-by-name",
            json={"name": variable.name},
        ).json()

        assert got["id"] == variable.id
        assert got["name"] == variable.name


class TestIamcVariableQuery(IamcVariableApiTest):
    @pytest.fixture(scope="class")
    def variable(
        self,
        direct_service: VariableService,
        direct_transport: DirectTransport,
    ) -> Variable:
        variable = direct_service.create("Variable")
        create_variable_query_data(direct_transport, variable.name)
        return variable

    def test_iamc_variable_list(self, client: httpx.Client, variable: Variable) -> None:
        listed = self.request(client, "PATCH", "/iamc/variables/list", json={}).json()

        assert_paginated_list(listed, expected_count=1)
        assert listed["results"][0]["id"] == variable.id

    def test_iamc_variable_tabulate(
        self, client: httpx.Client, variable: Variable
    ) -> None:
        tabulated = self.request(
            client, "PATCH", "/iamc/variables/tabulate", json={}
        ).json()

        assert tabulated["total"] == 1
        assert_frame_payload(
            tabulated["results"],
            expected_columns={"id", "name", "created_at", "created_by"},
        )

    def test_iamc_variable_query(
        self, client: httpx.Client, variable: Variable
    ) -> None:
        queried = self.request(client, "PATCH", "/iamc/variables", json={}).json()

        assert_paginated_list(queried, expected_count=1)
        assert queried["results"][0]["id"] == variable.id

    def test_iamc_variable_query_table(
        self, client: httpx.Client, variable: Variable
    ) -> None:
        query_table = self.request(
            client,
            "PATCH",
            "/iamc/variables",
            json={},
            params={"table": "true"},
        ).json()

        assert query_table["total"] == 1
        assert_frame_payload(
            query_table["results"],
            expected_columns={"id", "name", "created_at", "created_by"},
        )


class TestIamcVariableDocs(IamcVariableApiTest):
    @pytest.fixture(scope="class")
    def variable(self, direct_service: VariableService) -> Variable:
        return direct_service.create("Variable")

    @pytest.fixture(scope="class")
    def documented_variable(
        self, direct_service: VariableService, variable: Variable
    ) -> Variable:
        direct_service.set_docs(variable.id, "Variable docs")
        return variable

    def test_iamc_variable_set_docs(
        self, client: httpx.Client, variable: Variable
    ) -> None:
        created = self.request(
            client,
            "POST",
            f"/iamc/variables/{variable.id}/docs",
            json={"description": "Variable docs"},
        ).json()

        assert created["dimension__id"] == variable.id
        assert created["description"] == "Variable docs"

    def test_iamc_variable_get_docs(
        self, client: httpx.Client, documented_variable: Variable
    ) -> None:
        got = self.request(
            client, "GET", f"/iamc/variables/{documented_variable.id}/docs"
        ).json()

        assert got["dimension__id"] == documented_variable.id
        assert got["description"] == "Variable docs"

    def test_iamc_variable_list_docs(
        self, client: httpx.Client, documented_variable: Variable
    ) -> None:
        listed = self.request(
            client, "PATCH", "/iamc/variables/docs/list", json={}
        ).json()

        assert_paginated_list(listed, expected_count=1)
        assert listed["results"][0]["dimension__id"] == documented_variable.id

    def test_iamc_variable_compat_docs(
        self, client: httpx.Client, documented_variable: Variable
    ) -> None:
        listed = self.request(
            client,
            "GET",
            "/docs/iamc/variables",
            params={"dimension_id": documented_variable.id},
        ).json()

        assert_paginated_list(listed, expected_count=1)
        assert listed["results"][0]["dimension__id"] == documented_variable.id


class TestIamcVariableDelete(IamcVariableApiTest):
    @pytest.fixture(scope="class")
    def variable(self, direct_service: VariableService) -> Variable:
        return direct_service.create("Variable")

    def test_iamc_variable_delete(
        self,
        client: httpx.Client,
        direct_service: VariableService,
        variable: Variable,
    ) -> None:
        self.request(client, "DELETE", f"/iamc/variables/{variable.id}")

        assert direct_service.tabulate().empty


class TestIamcVariableTabulateVersionsApi(IamcVariableApiTest):
    def test_iamc_variable_tabulate_versions(self, client: httpx.Client) -> None:
        self.request(
            client, "POST", "/iamc/variables", json={"name": "VersionedVariable"}
        )
        tabulated = self.request(
            client,
            "PATCH",
            "/iamc/variables/versions/tabulate",
            json={},
        ).json()
        assert_frame_payload(
            tabulated["results"],
            expected_columns={
                "id",
                "name",
                "transaction_id",
                "end_transaction_id",
                "operation_type",
            },
        )
