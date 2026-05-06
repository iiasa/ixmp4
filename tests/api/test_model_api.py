import httpx
import pytest

from ixmp4.data.model.dto import Model
from ixmp4.data.model.service import ModelService
from tests.api.base import (
    ApiServiceTest,
    api_transport,
    assert_frame_payload,
    assert_paginated_list,
)

transport = api_transport


class ModelApiTest(ApiServiceTest[ModelService]):
    service_class = ModelService


class TestModelCreate(ModelApiTest):
    def test_model_create(self, client: httpx.Client) -> None:
        created = self.request(client, "POST", "/models", json={"name": "Model"}).json()

        assert created["id"] == 1
        assert created["name"] == "Model"


class TestModelLookup(ModelApiTest):
    @pytest.fixture(scope="class")
    def model(self, direct_service: ModelService) -> Model:
        return direct_service.create("Model")

    def test_model_get_by_id(self, client: httpx.Client, model: Model) -> None:
        got = self.request(client, "GET", f"/models/{model.id}").json()

        assert got["id"] == model.id
        assert got["name"] == model.name

    def test_model_get_by_name(self, client: httpx.Client, model: Model) -> None:
        got = self.request(
            client, "POST", "/models/get-by-name", json={"name": model.name}
        ).json()

        assert got["id"] == model.id
        assert got["name"] == model.name


class TestModelQuery(ModelApiTest):
    @pytest.fixture(scope="class")
    def models(self, direct_service: ModelService) -> list[Model]:
        return [
            direct_service.create("Model 1"),
            direct_service.create("Model 2"),
        ]

    def test_model_list(self, client: httpx.Client, models: list[Model]) -> None:
        listed = self.request(client, "PATCH", "/models/list", json={}).json()

        assert_paginated_list(listed, expected_count=2)
        assert [item["name"] for item in listed["results"]] == [m.name for m in models]

    def test_model_tabulate(self, client: httpx.Client, models: list[Model]) -> None:
        tabulated = self.request(client, "PATCH", "/models/tabulate", json={}).json()

        assert tabulated["total"] == len(models)
        assert_frame_payload(
            tabulated["results"],
            expected_columns={"id", "name", "created_at", "created_by"},
        )

    def test_model_query(self, client: httpx.Client, models: list[Model]) -> None:
        queried = self.request(client, "PATCH", "/models", json={}).json()

        assert_paginated_list(queried, expected_count=2)
        assert [item["name"] for item in queried["results"]] == [m.name for m in models]

    def test_model_query_table(self, client: httpx.Client, models: list[Model]) -> None:
        query_table = self.request(
            client, "PATCH", "/models", json={}, params={"table": "true"}
        ).json()

        assert query_table["total"] == len(models)
        assert_frame_payload(
            query_table["results"],
            expected_columns={"id", "name", "created_at", "created_by"},
        )


class TestModelDocs(ModelApiTest):
    @pytest.fixture(scope="class")
    def model(self, direct_service: ModelService) -> Model:
        return direct_service.create("Model")

    @pytest.fixture(scope="class")
    def documented_model(self, direct_service: ModelService, model: Model) -> Model:
        direct_service.set_docs(model.id, "Model docs")
        return model

    def test_model_set_docs(self, client: httpx.Client, model: Model) -> None:
        created = self.request(
            client,
            "POST",
            f"/models/{model.id}/docs",
            json={"description": "Model docs"},
        ).json()

        assert created["dimension__id"] == model.id
        assert created["description"] == "Model docs"

    def test_model_get_docs(
        self, client: httpx.Client, documented_model: Model
    ) -> None:
        got = self.request(client, "GET", f"/models/{documented_model.id}/docs").json()

        assert got["dimension__id"] == documented_model.id
        assert got["description"] == "Model docs"

    def test_model_list_docs(
        self, client: httpx.Client, documented_model: Model
    ) -> None:
        listed = self.request(client, "PATCH", "/models/docs/list", json={}).json()

        assert_paginated_list(listed, expected_count=1)
        assert listed["results"][0]["dimension__id"] == documented_model.id

    def test_model_compat_docs(
        self, client: httpx.Client, documented_model: Model
    ) -> None:
        listed = self.request(
            client,
            "GET",
            "/docs/models",
            params={"dimension_id": documented_model.id},
        ).json()

        assert_paginated_list(listed, expected_count=1)
        assert listed["results"][0]["dimension__id"] == documented_model.id


class TestModelDelete(ModelApiTest):
    @pytest.fixture(scope="class")
    def model(self, direct_service: ModelService) -> Model:
        return direct_service.create("Model")

    def test_model_delete(
        self,
        client: httpx.Client,
        direct_service: ModelService,
        model: Model,
    ) -> None:
        self.request(client, "DELETE", f"/models/{model.id}")

        assert direct_service.tabulate().empty
