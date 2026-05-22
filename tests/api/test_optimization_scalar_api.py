import httpx
import pytest

from ixmp4.data.optimization.scalar.service import ScalarService
from ixmp4.data.run.dto import Run
from ixmp4.data.run.service import RunService
from ixmp4.data.unit.dto import Unit
from ixmp4.data.unit.service import UnitService
from ixmp4.transport import DirectTransport
from tests.api.base import ApiServiceTest, api_transport, assert_frame_payload

transport = api_transport


class ScalarApiTest(ApiServiceTest[ScalarService]):
    service_class = ScalarService


class TestScalarCreateAndUpdate(ScalarApiTest):
    @pytest.fixture(scope="class")
    def run(self, direct_transport: DirectTransport) -> Run:
        return RunService(direct_transport).create("Model", "Scenario")

    @pytest.fixture(scope="class")
    def unit(self, direct_transport: DirectTransport) -> Unit:
        return UnitService(direct_transport).create("Unit")

    def test_scalar_create_update_and_tabulate(
        self, client: httpx.Client, run: Run, unit: Unit
    ) -> None:
        created = self.request(
            client,
            "POST",
            "/optimization/scalars",
            json={
                "run_id": run.id,
                "name": "Scalar",
                "value": 13,
                "unit_name": unit.name,
            },
        ).json()

        assert created["id"] == 1
        assert created["name"] == "Scalar"

        updated = self.request(
            client,
            "POST",
            f"/optimization/scalars/{created['id']}",
            json={"value": 42},
        ).json()
        assert updated["value"] == 42.0

        tabulated = self.request(
            client, "PATCH", "/optimization/scalars/tabulate", json={}
        ).json()
        assert tabulated["total"] == 1
        assert_frame_payload(
            tabulated["results"],
            expected_columns={"id", "run__id", "name", "value", "unit__id"},
        )


class TestScalarTabulateVersionsApi(ScalarApiTest):
    @pytest.fixture(scope="class")
    def run(self, direct_transport: DirectTransport) -> Run:
        return RunService(direct_transport).create("Model", "Scenario")

    @pytest.fixture(scope="class")
    def unit(self, direct_transport: DirectTransport) -> Unit:
        return UnitService(direct_transport).create("Unit")

    def test_scalar_tabulate_versions(
        self, client: httpx.Client, run: Run, unit: Unit
    ) -> None:
        self.request(
            client,
            "POST",
            "/optimization/scalars",
            json={
                "run_id": run.id,
                "name": "VersionedScalar",
                "value": 1.0,
                "unit_name": unit.name,
            },
        )
        tabulated = self.request(
            client,
            "PATCH",
            "/optimization/scalars/versions/tabulate",
            json={},
        ).json()
        assert_frame_payload(
            tabulated["results"],
            expected_columns={
                "id",
                "run__id",
                "name",
                "value",
                "unit__id",
                "transaction_id",
                "end_transaction_id",
                "operation_type",
            },
        )
