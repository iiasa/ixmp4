import datetime

import pandas as pd
import pandas.testing as pdt
import pytest

from ixmp4.base_exceptions import Forbidden
from ixmp4.data.model.exceptions import ModelNotFound, ModelNotUnique
from ixmp4.data.model.service import ModelService
from tests import auth, backends
from tests.data.base import ServiceTest

transport = backends.get_transport_fixture(scope="class")


class ModelServiceTest(ServiceTest[ModelService]):
    service_class = ModelService


class TestModelCreate(ModelServiceTest):
    def test_model_create(
        self, service: ModelService, fake_time: datetime.datetime
    ) -> None:
        model = service.create("Model")
        assert model.name == "Model"
        assert model.created_at == fake_time.replace(tzinfo=None)
        assert model.created_by == "@unknown"

    def test_model_create_versioning(
        self, versioning_service: ModelService, fake_time: datetime.datetime
    ) -> None:
        expected_versions = pd.DataFrame(
            [
                [
                    1,
                    "Model",
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


class TestModelDeleteById(ModelServiceTest):
    def test_model_delete_by_id(
        self, service: ModelService, fake_time: datetime.datetime
    ) -> None:
        model = service.create("Model")
        service.delete_by_id(model.id)
        assert service.tabulate().empty

    def test_model_delete_by_id_versioning(
        self, versioning_service: ModelService, fake_time: datetime.datetime
    ) -> None:
        expected_versions = pd.DataFrame(
            [
                [
                    1,
                    "Model",
                    fake_time.replace(tzinfo=None),
                    "@unknown",
                    1,
                    2,
                    0,
                ],
                [
                    1,
                    "Model",
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


class TestModelUnique(ModelServiceTest):
    def test_model_unique(self, service: ModelService) -> None:
        service.create("Model")

        with pytest.raises(ModelNotUnique):
            service.create("Model")


class TestModelGetByName(ModelServiceTest):
    def test_model_get_by_name(self, service: ModelService) -> None:
        model1 = service.create("Model")
        model2 = service.get_by_name("Model")
        assert model1 == model2


class TestModelGetById(ModelServiceTest):
    def test_model_get_by_id(self, service: ModelService) -> None:
        model1 = service.create("Model")
        model2 = service.get_by_id(1)
        assert model1 == model2


class TestModelNotFound(ModelServiceTest):
    def test_model_not_found(self, service: ModelService) -> None:
        with pytest.raises(ModelNotFound):
            service.get_by_name("Model")

        with pytest.raises(ModelNotFound):
            service.get_by_id(1)


class TestModelList(ModelServiceTest):
    def test_model_list(
        self, service: ModelService, fake_time: datetime.datetime
    ) -> None:
        service.create("Model 1")
        service.create("Model 2")

        models = service.list()

        assert models[0].id == 1
        assert models[0].name == "Model 1"
        assert models[0].created_by == "@unknown"
        assert models[0].created_at == fake_time.replace(tzinfo=None)

        assert models[1].id == 2
        assert models[1].name == "Model 2"
        assert models[1].created_by == "@unknown"
        assert models[1].created_at == fake_time.replace(tzinfo=None)


class TestModelTabulate(ModelServiceTest):
    def test_model_tabulate(
        self, service: ModelService, fake_time: datetime.datetime
    ) -> None:
        service.create("Model 1")
        service.create("Model 2")

        expected_models = pd.DataFrame(
            [
                [
                    1,
                    "Model 1",
                    fake_time.replace(tzinfo=None),
                    "@unknown",
                ],
                [
                    2,
                    "Model 2",
                    fake_time.replace(tzinfo=None),
                    "@unknown",
                ],
            ],
            columns=["id", "name", "created_at", "created_by"],
        )

        models = service.tabulate()
        pdt.assert_frame_equal(models, expected_models, check_like=True)


class TestModelAuthSarahPrivate(
    auth.SarahTest, auth.PrivatePlatformTest, ModelServiceTest
):
    def test_model_create(self, service: ModelService) -> None:
        model = service.create("Model")
        assert model.id == 1
        assert model.created_by == "superuser_sarah"

    def test_model_get_by_name(self, service: ModelService) -> None:
        model = service.get_by_name("Model")
        assert model.id == 1

    def test_model_get_by_id(self, service: ModelService) -> None:
        model = service.get_by_id(1)
        assert model.name == "Model"

    def test_model_list(self, service: ModelService) -> None:
        results = service.list()
        assert len(results) == 1

    def test_model_tabulate(self, service: ModelService) -> None:
        results = service.tabulate()
        assert len(results) == 1

    def test_model_delete(self, service: ModelService) -> None:
        service.delete_by_id(1)


class TestModelAuthAlicePrivate(
    auth.AliceTest, auth.PrivatePlatformTest, ModelServiceTest
):
    def test_model_create(self, service: ModelService) -> None:
        with pytest.raises(Forbidden):
            model = service.create("Model")
            assert model.id == 1

    def test_model_get_by_name(self, service: ModelService) -> None:
        with pytest.raises(Forbidden):
            service.get_by_name("Model")

    def test_model_get_by_id(self, service: ModelService) -> None:
        with pytest.raises(Forbidden):
            service.get_by_id(1)

    def test_model_list(self, service: ModelService) -> None:
        with pytest.raises(Forbidden):
            service.list()

    def test_model_tabulate(self, service: ModelService) -> None:
        with pytest.raises(Forbidden):
            service.tabulate()

    def test_model_delete(self, service: ModelService) -> None:
        with pytest.raises(Forbidden):
            service.delete_by_id(1)


class TestModelAuthBobPrivate(auth.BobTest, auth.PrivatePlatformTest, ModelServiceTest):
    def test_model_create(self, service: ModelService) -> None:
        model = service.create("Model")
        assert model.id == 1
        assert model.created_by == "staffuser_bob"

    def test_model_get_by_name(self, service: ModelService) -> None:
        model = service.get_by_name("Model")
        assert model.id == 1

    def test_model_get_by_id(self, service: ModelService) -> None:
        model = service.get_by_id(1)
        assert model.name == "Model"

    def test_model_list(self, service: ModelService) -> None:
        results = service.list()
        assert len(results) == 1

    def test_model_tabulate(self, service: ModelService) -> None:
        results = service.tabulate()
        assert len(results) == 1

    def test_model_delete(self, service: ModelService) -> None:
        service.delete_by_id(1)


class TestModelAuthCarinaPrivate(
    auth.CarinaTest, auth.PrivatePlatformTest, ModelServiceTest
):
    def test_model_create(self, service: ModelService) -> None:
        with pytest.raises(Forbidden):
            model = service.create("Model")
            assert model.id == 1

    def test_model_get_by_name(self, service: ModelService) -> None:
        with pytest.raises(ModelNotFound):
            service.get_by_name("Model")

    def test_model_get_by_id(self, service: ModelService) -> None:
        with pytest.raises(ModelNotFound):
            service.get_by_id(1)

    def test_model_list(self, service: ModelService) -> None:
        results = service.list()
        assert len(results) == 0

    def test_model_tabulate(self, service: ModelService) -> None:
        results = service.tabulate()
        assert len(results) == 0

    def test_model_delete(self, service: ModelService) -> None:
        with pytest.raises(Forbidden):
            service.delete_by_id(1)


class TestModelAuthNonePrivate(
    auth.NoneTest, auth.PrivatePlatformTest, ModelServiceTest
):
    def test_model_create(self, service: ModelService) -> None:
        with pytest.raises(Forbidden):
            model = service.create("Model")
            assert model.id == 1

    def test_model_get_by_name(self, service: ModelService) -> None:
        with pytest.raises(Forbidden):
            service.get_by_name("Model")

    def test_model_get_by_id(self, service: ModelService) -> None:
        with pytest.raises(Forbidden):
            service.get_by_id(1)

    def test_model_list(self, service: ModelService) -> None:
        with pytest.raises(Forbidden):
            service.list()

    def test_model_tabulate(self, service: ModelService) -> None:
        with pytest.raises(Forbidden):
            service.tabulate()

    def test_model_delete(self, service: ModelService) -> None:
        with pytest.raises(Forbidden):
            service.delete_by_id(1)


class TestModelAuthDavePublic(auth.DaveTest, auth.PublicPlatformTest, ModelServiceTest):
    def test_model_create(self, service: ModelService) -> None:
        with pytest.raises(Forbidden):
            model = service.create("Model")
            assert model.id == 1

    def test_model_get_by_name(self, service: ModelService) -> None:
        with pytest.raises(ModelNotFound):
            service.get_by_name("Model")

    def test_model_get_by_id(self, service: ModelService) -> None:
        with pytest.raises(ModelNotFound):
            service.get_by_id(1)

    def test_model_list(self, service: ModelService) -> None:
        results = service.list()
        assert len(results) == 0

    def test_model_tabulate(self, service: ModelService) -> None:
        results = service.tabulate()
        assert len(results) == 0

    def test_model_delete(self, service: ModelService) -> None:
        with pytest.raises(Forbidden):
            service.delete_by_id(1)


class TestModelAuthNonePublic(auth.NoneTest, auth.PublicPlatformTest, ModelServiceTest):
    def test_model_create(self, service: ModelService) -> None:
        with pytest.raises(Forbidden):
            model = service.create("Model")
            assert model.id == 1

    def test_model_get_by_name(self, service: ModelService) -> None:
        with pytest.raises(ModelNotFound):
            service.get_by_name("Model")

    def test_model_get_by_id(self, service: ModelService) -> None:
        with pytest.raises(ModelNotFound):
            service.get_by_id(1)

    def test_model_list(self, service: ModelService) -> None:
        results = service.list()
        assert len(results) == 0

    def test_model_tabulate(self, service: ModelService) -> None:
        results = service.tabulate()
        assert len(results) == 0

    def test_model_delete(self, service: ModelService) -> None:
        with pytest.raises(Forbidden):
            service.delete_by_id(1)


# TODO: More detail in auth tests
